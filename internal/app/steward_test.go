// SPDX-FileCopyrightText: 2024 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"context"
	"net/http"
	"net/http/httptest"
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
