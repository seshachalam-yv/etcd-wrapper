// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gardener/etcd-wrapper/internal/bootstrap"
	"github.com/gardener/etcd-wrapper/internal/types"
	"github.com/gardener/etcd-wrapper/internal/util"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

const (
	etcdWrapperReadHeaderTimeout = 5 * time.Second
	etcdConnectionTimeout        = 5 * time.Second
	etcdGetTimeout               = 5 * time.Second
	etcdQueryInterval            = 2 * time.Second
)

// queryAndUpdateEtcdReadiness periodically queries the etcd DB to check its readiness and updates the status
// of the query into the etcdStatus struct. It stops querying when the application context is cancelled.
func (a *Application) queryAndUpdateEtcdReadiness() {
	// Create a ticker to periodically query etcd readiness
	ticker := time.NewTicker(etcdQueryInterval)
	defer ticker.Stop()

	for {
		// Query etcd readiness and update the status
		a.etcdReady = a.isEtcdReady()
		select {
		// Stop querying and return when the context is cancelled
		case <-a.ctx.Done():
			a.logger.Error("stopped periodic DB query: context cancelled", zap.Error(a.ctx.Err()))
			return
		// Wait for the next tick before querying again
		case <-ticker.C:
		}
	}
}

// isEtcdReady checks if ETCD is ready by making a `GET` call (with a timeout).
// if there is an error then it returns false else it returns true.
func (a *Application) isEtcdReady() bool {
	etcdConnCtx, cancelFunc := context.WithTimeout(a.ctx, etcdGetTimeout)
	defer cancelFunc()
	_, err := a.etcdClient.Get(etcdConnCtx, "foo")
	if err != nil {
		a.logger.Error("failed to retrieve from etcd db", zap.Error(err))
	}
	return err == nil
}

// readinessHandler reads the etcd status from the etcdStatus struct and writes that onto the http responsewriter.
// It also respects a manual override that can be set via the /readyz/set endpoint.
func (a *Application) readinessHandler(w http.ResponseWriter, _ *http.Request) {
	a.mu.Lock()
	override := a.manualReadyOverride
	a.mu.Unlock()

	if a.etcdReady || override {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

// createEtcdClient creates an ETCD client
func (a *Application) createEtcdClient() (*clientv3.Client, error) {
	// fetch tls configuration
	tlsConfig, err := util.CreateTLSConfig(a.isTLSEnabled, a.Config.EtcdClientTLS.ServerName, a.cfg.ClientTLSInfo.TrustedCAFile, &util.KeyPair{
		CertPath: a.Config.EtcdClientTLS.CertPath,
		KeyPath:  a.Config.EtcdClientTLS.KeyPath,
	})
	if err != nil {
		return nil, err
	}

	// Create etcd client
	cli, err := clientv3.New(clientv3.Config{
		Context:     a.ctx,
		Endpoints:   []string{util.ConstructBaseAddress(a.isTLSEnabled(), fmt.Sprintf("%s:%d", a.Config.EtcdClientTLS.ServerName, a.Config.EtcdClientPort))},
		DialTimeout: etcdConnectionTimeout,
		LogConfig:   bootstrap.SetupLoggerConfig(types.DefaultLogLevel),
		TLS:         tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	return cli, nil
}

// isTLSEnabled checks if TLS has been enabled in the etcd configuration.
func (a *Application) isTLSEnabled() bool {
	return len(strings.TrimSpace(a.cfg.ClientTLSInfo.CertFile)) != 0 &&
		len(strings.TrimSpace(a.cfg.ClientTLSInfo.KeyFile)) != 0 &&
		len(strings.TrimSpace(a.cfg.ClientTLSInfo.TrustedCAFile)) != 0
}

// startEmbeddedEtcdHandler handles POST /embedded-etcd requests from the steward.
// It accepts a YAML etcd config in the request body, parses it, and signals that
// the embedded etcd should be started with the provided configuration.
// Receiving this request switches the wrapper into steward mode.
func (a *Application) startEmbeddedEtcdHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Write config to temp file so embed.ConfigFromFile can parse it.
	tmpFile := filepath.Join(os.TempDir(), "etcd-steward-config.yaml")
	if err := os.WriteFile(tmpFile, body, 0600); err != nil {
		http.Error(w, "failed to write config", http.StatusInternalServerError)
		return
	}

	cfg, err := embed.ConfigFromFile(tmpFile)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid etcd config: %v", err), http.StatusBadRequest)
		return
	}

	// When the steward is joining an existing cluster the peer TLS handshake
	// may fail SAN verification because the certificates were issued for the
	// original cluster members. Skip the client SAN check on the peer
	// transport to allow the new member to connect.
	cfg.PeerTLSInfo.SkipClientSANVerify = true

	a.mu.Lock()
	a.cfg = cfg
	a.embeddedEtcdRequested = true
	a.stewardMode = true
	a.mu.Unlock()

	a.logger.Info("received POST /embedded-etcd from steward, switching to steward mode")

	w.WriteHeader(http.StatusAccepted)
	_, _ = w.Write([]byte("embedded etcd start requested"))
}

// setReadinessHandler handles POST /readyz/set requests from the steward.
// The body must be either "ready" or "unready" to control the readiness probe override.
func (a *Application) setReadinessHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, _ := io.ReadAll(req.Body)
	switch strings.TrimSpace(string(body)) {
	case "ready":
		a.mu.Lock()
		a.manualReadyOverride = true
		a.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case "unready":
		a.mu.Lock()
		a.manualReadyOverride = false
		a.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	default:
		http.Error(w, "body must be 'ready' or 'unready'", http.StatusBadRequest)
	}
}

func (a *Application) stopEtcdHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method != "POST" {
		return
	}
	a.logger.Info("received stop request, stopping etcd-wrapper...")
	a.cancelContext()
	w.WriteHeader(http.StatusOK)
}

func (a *Application) startHTTPServer() {
	a.logger.Info(
		"Starting HTTP server at addr",
		zap.Int64("Port No: ", int64(a.Config.EtcdWrapperPort)),
	)
	// RegisterHandler must have been called before startHTTPServer.
	// When the server is started early (during Setup, before the etcd config
	// is available) TLS cannot be determined yet, so fall back to plain HTTP.
	if a.cfg != nil && a.isTLSEnabled() {
		a.logger.Info("TLS enabled. Starting HTTPS server.")
		err := a.server.ListenAndServeTLS(a.cfg.ClientTLSInfo.CertFile, a.cfg.ClientTLSInfo.KeyFile)
		if err != nil && err != http.ErrServerClosed {
			a.logger.Fatal("Failed to start http server: %v", zap.Error(err))
		}
		a.logger.Info("HTTPS server closed gracefully.")
		return
	}

	err := a.server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		a.logger.Fatal("Failed to start http server: %v", zap.Error(err))
	}
	a.logger.Info("HTTP server closed gracefully.")
}

func (a *Application) stopHTTPServer() error {
	return a.server.Close()
}

// RegisterHandler registers the handler for different requests
func (a *Application) RegisterHandler() {
	mux := http.NewServeMux()

	mux.HandleFunc("/readyz", a.readinessHandler)
	mux.HandleFunc("/readyz/set", a.setReadinessHandler)
	mux.HandleFunc("/stop", a.stopEtcdHandler)
	mux.HandleFunc("/embedded-etcd", a.startEmbeddedEtcdHandler)

	a.server = &http.Server{
		Addr:              fmt.Sprintf(":%d", a.Config.EtcdWrapperPort),
		Handler:           mux,
		ReadHeaderTimeout: etcdWrapperReadHeaderTimeout,
	}
}
