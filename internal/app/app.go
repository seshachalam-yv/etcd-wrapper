// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"syscall"
	"time"

	"github.com/gardener/etcd-wrapper/internal/types"

	"github.com/gardener/etcd-wrapper/internal/bootstrap"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

// Application is a top level struct which serves as an entry point for this application.
type Application struct {
	ctx      context.Context
	cancelFn context.CancelFunc
	// Config is the application config
	Config           types.Config
	etcdInitializer  bootstrap.EtcdInitializer
	cfg              *embed.Config
	etcdClient       *clientv3.Client
	etcd             *embed.Etcd
	waitReadyTimeout time.Duration
	logger           *zap.Logger
	etcdReady        bool // should have only one actor that updates it, queryAndUpdateEtcdReadiness()
	server           *http.Server
	// mu guards fields that can be modified by HTTP handlers concurrently.
	mu sync.Mutex
	// embeddedEtcdRequested is set to true when the steward posts a config to /embedded-etcd.
	embeddedEtcdRequested bool
	// manualReadyOverride allows the steward to override the readiness probe via /readyz/set.
	manualReadyOverride bool
	// stewardMode indicates that the wrapper is being driven by etcd-steward (POST /embedded-etcd
	// was received) rather than by the legacy backup-restore sidecar flow.
	stewardMode bool
}

// NewApplication initializes and returns an application struct
func NewApplication(ctx context.Context, cancelFn context.CancelFunc, config types.Config, waitReadyTimeout time.Duration, logger *zap.Logger) (*Application, error) {
	logger.Info("Initializing application", zap.Any("config", config))
	etcdInitializer, err := bootstrap.NewEtcdInitializer(&config.BackupRestore, logger)
	if err != nil {
		return nil, err
	}
	return &Application{
		ctx:              ctx,
		cancelFn:         cancelFn,
		Config:           config,
		etcdInitializer:  etcdInitializer,
		waitReadyTimeout: waitReadyTimeout,
		logger:           logger,
	}, nil
}

// Setup sets up etcd by triggering initialization of the etcd DB.
// It supports two mutually exclusive flows:
//
//   - Legacy flow (backup-restore sidecar): the wrapper polls the sidecar's
//     /initialization/status endpoint, triggers /initialization/start, then
//     retrieves the etcd config via GET /config.
//   - Steward flow (etcd-steward): the steward posts the etcd config to
//     POST /embedded-etcd. The wrapper does not interact with the sidecar at all.
//
// Both paths race: whichever delivers a valid *embed.Config first wins.
// The HTTP server is started early so that the steward can reach the
// /embedded-etcd endpoint while the legacy initializer is still running.
func (a *Application) Setup() error {
	// Start the HTTP server early so the steward can reach /embedded-etcd
	// before (or instead of) the legacy initializer completing.
	a.RegisterHandler()
	go a.startHTTPServer()

	cfgChan := make(chan *embed.Config, 1)

	// Path A: Legacy flow — poll the backup-restore sidecar.
	go func() {
		cfg, err := a.etcdInitializer.Run(a.ctx)
		if err != nil {
			a.logger.Info("legacy initializer did not produce a config", zap.Error(err))
			return
		}
		select {
		case cfgChan <- cfg:
			a.logger.Info("etcd config obtained via legacy backup-restore flow")
		default:
		}
	}()

	// Path B: Steward flow — wait for POST /embedded-etcd to set the config.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			a.mu.Lock()
			requested := a.embeddedEtcdRequested
			cfg := a.cfg
			a.mu.Unlock()

			if requested && cfg != nil {
				select {
				case cfgChan <- cfg:
					a.logger.Info("etcd config obtained via steward flow (POST /embedded-etcd)")
				default:
				}
				return
			}

			select {
			case <-a.ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// Wait for either path to deliver a config, or the context to be cancelled.
	select {
	case cfg := <-cfgChan:
		a.mu.Lock()
		isSteward := a.embeddedEtcdRequested
		a.mu.Unlock()

		a.stewardMode = isSteward
		a.cfg = cfg
		syscall.Umask(0077)
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}

// Start sets up readiness probe and starts an embedded etcd.
func (a *Application) Start() error {
	var err error

	// Change file permissions for files previously created without umask 0077
	// TODO (shreyas-s-rao): remove this temporary code in etcd-wrapper v0.8.0
	if err = bootstrap.ChangeFilePermissions(a.cfg.Dir, 0600); err != nil {
		return fmt.Errorf("failed to change file permissions: %w", err)
	}

	// Create etcd client for readiness probe
	cli, err := a.createEtcdClient()
	if err != nil {
		return err
	}
	a.etcdClient = cli
	defer a.Close()

	// In the legacy flow the wrapper polls etcd to determine readiness.
	// In the steward flow the steward controls readiness via POST /readyz/set,
	// so the autonomous polling goroutine is not started.
	if !a.stewardMode {
		go a.queryAndUpdateEtcdReadiness()
	} else {
		a.logger.Info("steward mode active: readiness is controlled via POST /readyz/set")
	}

	// The HTTP server is already running (started in Setup). Ensure it is
	// stopped when Start returns.
	defer func() {
		if err := a.stopHTTPServer(); err != nil {
			a.logger.Error("unable to stop HTTP server: %v",
				zap.Error(err),
			)
		}
	}()

	// Create embedded etcd and start.
	if err = a.startEtcd(); err != nil {
		return err
	}
	// Delete exit code file after etcd starts successfully
	if err = bootstrap.CleanupExitCode(types.DefaultExitCodeFilePath); err != nil {
		a.logger.Warn("failed to clean-up last captured exit code", zap.Error(err))
	}

	// Start leadership watcher to push leader info into etcd for steward.
	go a.watchLeadership(a.ctx)

	// block till application context is cancelled, or there is a notification on etcd.Server.StopNotify channel
	// or there is an error notification on etcd.Err channel
	select {
	case <-a.ctx.Done():
		a.logger.Error("application context has been cancelled", zap.Error(a.ctx.Err()))
	case <-a.etcd.Server.StopNotify():
		a.logger.Error("etcd server has been aborted, received notification on StopNotify channel")
	case err = <-a.etcd.Err():
		a.logger.Error("error received on etcd Err channel", zap.Error(err))
	}

	return nil
}

// Close closes resources(e.g. etcd client) and cancels the context if not already done so.
func (a *Application) Close() {
	if err := a.etcdClient.Close(); err != nil {
		a.logger.Error("failed to close etcd client", zap.Error(err))
	}
	if a.etcd != nil {
		a.etcd.Close()
	}
	a.cancelContext()
}

func (a *Application) cancelContext() {
	// only if the context has not yet been cancelled, call the context.CancelFunc
	if a.ctx.Err() == nil {
		a.cancelFn()
	}
}

func (a *Application) startEtcd() error {
	// TODO StartEtcd returns an Etcd object. In future we should use that to listen on leadership change notifications (when we move to a version of etcd which exposes the channel).
	etcd, err := embed.StartEtcd(a.cfg)
	if err != nil {
		return err
	}

	// wait till the etcd server notifies that it is ready, or if an abrupt stop has happened which is notified
	// via etcd.Server.Notify or there is a timeout waiting for the etcd server to start.
	select {
	case <-etcd.Server.ReadyNotify():
		a.logger.Info("etcd server is now ready to serve client requests")
	case <-etcd.Server.StopNotify():
		a.logger.Error("etcd server has been aborted, received notification on StopNotify channel")
	case <-time.After(a.waitReadyTimeout):
		a.logger.Error("timeout waiting for ReadyNotify signal, aborting start of etcd")
	}
	a.etcd = etcd
	return nil
}
