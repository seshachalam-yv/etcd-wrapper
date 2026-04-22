// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gardener/etcd-wrapper/internal/types"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"

	. "github.com/onsi/gomega"
)

func TestStartEmbeddedEtcdHandler_MethodNotAllowed(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	request, err := http.NewRequest("GET", "/embedded-etcd", nil)
	g.Expect(err).To(BeNil())
	response := httptest.NewRecorder()
	handler := http.HandlerFunc(app.startEmbeddedEtcdHandler)
	handler.ServeHTTP(response, request)
	g.Expect(response.Code).To(Equal(http.StatusMethodNotAllowed))
}

func TestStartEmbeddedEtcdHandler_SetsStewardMode(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	// Write a minimal valid etcd config file so embed.ConfigFromFile succeeds.
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "etcd-data")
	configBody := "name: test-etcd\ndata-dir: " + dataDir + "\n"

	request, err := http.NewRequest("POST", "/embedded-etcd", strings.NewReader(configBody))
	g.Expect(err).To(BeNil())
	response := httptest.NewRecorder()
	handler := http.HandlerFunc(app.startEmbeddedEtcdHandler)
	handler.ServeHTTP(response, request)

	g.Expect(response.Code).To(Equal(http.StatusAccepted))

	app.mu.Lock()
	defer app.mu.Unlock()
	g.Expect(app.stewardMode).To(BeTrue())
	g.Expect(app.embeddedEtcdRequested).To(BeTrue())
	g.Expect(app.cfg).ToNot(BeNil())
	g.Expect(app.cfg.PeerTLSInfo.SkipClientSANVerify).To(BeTrue())

	// Clean up the temp config file written by the handler.
	_ = os.Remove(filepath.Join(os.TempDir(), "etcd-steward-config.yaml"))
}

func TestStartEmbeddedEtcdHandler_InvalidConfig(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	request, err := http.NewRequest("POST", "/embedded-etcd", strings.NewReader("invalid: [yaml"))
	g.Expect(err).To(BeNil())
	response := httptest.NewRecorder()
	handler := http.HandlerFunc(app.startEmbeddedEtcdHandler)
	handler.ServeHTTP(response, request)

	g.Expect(response.Code).To(Equal(http.StatusBadRequest))

	app.mu.Lock()
	defer app.mu.Unlock()
	g.Expect(app.stewardMode).To(BeFalse())
	g.Expect(app.embeddedEtcdRequested).To(BeFalse())
}

func TestSetReadinessHandler_Ready(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	// Initially etcdReady is false and no override, so /readyz should be 503.
	readyzReq, err := http.NewRequest("GET", "/readyz", nil)
	g.Expect(err).To(BeNil())
	readyzResp := httptest.NewRecorder()
	http.HandlerFunc(app.readinessHandler).ServeHTTP(readyzResp, readyzReq)
	g.Expect(readyzResp.Code).To(Equal(http.StatusServiceUnavailable))

	// POST "ready" to /readyz/set
	setReq, err := http.NewRequest("POST", "/readyz/set", strings.NewReader("ready"))
	g.Expect(err).To(BeNil())
	setResp := httptest.NewRecorder()
	http.HandlerFunc(app.setReadinessHandler).ServeHTTP(setResp, setReq)
	g.Expect(setResp.Code).To(Equal(http.StatusOK))

	// Now /readyz should return 200 due to override.
	readyzReq2, err := http.NewRequest("GET", "/readyz", nil)
	g.Expect(err).To(BeNil())
	readyzResp2 := httptest.NewRecorder()
	http.HandlerFunc(app.readinessHandler).ServeHTTP(readyzResp2, readyzReq2)
	g.Expect(readyzResp2.Code).To(Equal(http.StatusOK))
}

func TestSetReadinessHandler_Unready(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	// First set ready override to true.
	app.mu.Lock()
	app.manualReadyOverride = true
	app.mu.Unlock()

	// Verify /readyz returns 200.
	readyzReq, err := http.NewRequest("GET", "/readyz", nil)
	g.Expect(err).To(BeNil())
	readyzResp := httptest.NewRecorder()
	http.HandlerFunc(app.readinessHandler).ServeHTTP(readyzResp, readyzReq)
	g.Expect(readyzResp.Code).To(Equal(http.StatusOK))

	// POST "unready" to /readyz/set
	setReq, err := http.NewRequest("POST", "/readyz/set", strings.NewReader("unready"))
	g.Expect(err).To(BeNil())
	setResp := httptest.NewRecorder()
	http.HandlerFunc(app.setReadinessHandler).ServeHTTP(setResp, setReq)
	g.Expect(setResp.Code).To(Equal(http.StatusOK))

	// Now /readyz should return 503 (etcdReady is false and override is false).
	readyzReq2, err := http.NewRequest("GET", "/readyz", nil)
	g.Expect(err).To(BeNil())
	readyzResp2 := httptest.NewRecorder()
	http.HandlerFunc(app.readinessHandler).ServeHTTP(readyzResp2, readyzReq2)
	g.Expect(readyzResp2.Code).To(Equal(http.StatusServiceUnavailable))
}

func TestSetReadinessHandler_InvalidBody(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	setReq, err := http.NewRequest("POST", "/readyz/set", strings.NewReader("invalid-value"))
	g.Expect(err).To(BeNil())
	setResp := httptest.NewRecorder()
	http.HandlerFunc(app.setReadinessHandler).ServeHTTP(setResp, setReq)
	g.Expect(setResp.Code).To(Equal(http.StatusBadRequest))
}

func TestSetReadinessHandler_MethodNotAllowed(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app := createStewardTestApp(ctx, cancel)

	setReq, err := http.NewRequest("GET", "/readyz/set", nil)
	g.Expect(err).To(BeNil())
	setResp := httptest.NewRecorder()
	http.HandlerFunc(app.setReadinessHandler).ServeHTTP(setResp, setReq)
	g.Expect(setResp.Code).To(Equal(http.StatusMethodNotAllowed))
}

func TestSetup_StewardFlowWins(t *testing.T) {
	g := NewWithT(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create an app that will never succeed via the legacy path because the
	// backup-restore sidecar does not exist in this test. The steward path
	// should win the race instead.
	app := createStewardTestApp(ctx, cancel)
	// Give the app a no-op initializer that blocks until ctx is cancelled
	// (simulating backup-restore never being available).
	app.etcdInitializer = &blockingInitializer{ctx: ctx}

	// In a separate goroutine, simulate the steward posting config after a
	// short delay.
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "etcd-data")
	configBody := "name: test-etcd\ndata-dir: " + dataDir + "\n"

	go func() {
		time.Sleep(200 * time.Millisecond)
		req, _ := http.NewRequest("POST", "/embedded-etcd", strings.NewReader(configBody))
		resp := httptest.NewRecorder()
		app.startEmbeddedEtcdHandler(resp, req)
	}()

	err := app.Setup()
	g.Expect(err).To(BeNil())
	g.Expect(app.stewardMode).To(BeTrue())
	g.Expect(app.cfg).ToNot(BeNil())

	// Cleanup: stop HTTP server started by Setup.
	if app.server != nil {
		_ = app.server.Close()
	}
	_ = os.Remove(filepath.Join(os.TempDir(), "etcd-steward-config.yaml"))
}

// blockingInitializer is a test double that blocks until the context is cancelled.
type blockingInitializer struct {
	ctx context.Context
}

func (b *blockingInitializer) Run(ctx context.Context) (*embed.Config, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// createStewardTestApp creates a minimal Application for testing steward-specific handlers.
// It does not require TLS or a real etcd client since these handlers don't interact with etcd.
func createStewardTestApp(ctx context.Context, cancelFn context.CancelFunc) *Application {
	return &Application{
		ctx:      ctx,
		cancelFn: cancelFn,
		Config: types.Config{
			EtcdWrapperPort: 0,
		},
		cfg:              &embed.Config{},
		waitReadyTimeout: time.Minute,
		logger:           zap.NewExample(),
	}
}
