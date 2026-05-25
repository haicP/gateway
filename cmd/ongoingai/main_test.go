package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ongoingai/gateway/internal/config"
	"github.com/ongoingai/gateway/internal/correlation"
)

func TestShouldCaptureTrace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
		want bool
	}{
		{path: "/api/health", want: false},
		{path: "/api/traces", want: false},
		{path: "/dashboard", want: false},
		{path: "/api", want: false},
		{path: "/apiish", want: true},
		{path: "/llm/v1/chat/completions", want: true},
		{path: "/llm/v1/messages", want: true},
	}

	for _, tt := range tests {
		if got := shouldCaptureTrace(tt.path); got != tt.want {
			t.Fatalf("shouldCaptureTrace(%q)=%t, want %t", tt.path, got, tt.want)
		}
	}
}

func TestConfiguredProviderSummaries(t *testing.T) {
	t.Parallel()

	cfg := config.Default()

	got := configuredProviderSummaries(cfg)
	want := []string{
		"llm:/llm->https://api.openai.com",
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("configuredProviderSummaries()=%v, want %v", got, want)
	}
}

func TestConfiguredProviderSummariesSkipsIncompleteProviders(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Providers["llm"] = config.ProviderConfig{}

	got := configuredProviderSummaries(cfg)
	if len(got) != 0 {
		t.Fatalf("configuredProviderSummaries()=%v, want empty result", got)
	}
}

func TestRunServeRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	configPath := filepath.Join(t.TempDir(), "invalid.yaml")
	configBody := `server:
  host: 127.0.0.1
  port: 70000
storage:
  driver: sqlite
  path: ./data/ongoingai.db
providers:
  llm:
    upstream: https://api.openai.com
    prefix: /llm
`
	if err := os.WriteFile(configPath, []byte(configBody), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if code := runServe([]string{"--config", configPath}); code != 1 {
		t.Fatalf("runServe exit code=%d, want 1", code)
	}
}

func TestNewGatewayServerUsesSafeTimeouts(t *testing.T) {
	t.Parallel()

	server := newGatewayServer(config.Default(), nil, http.NotFoundHandler())
	if server.ReadHeaderTimeout != serverReadHeaderTimeout {
		t.Fatalf("ReadHeaderTimeout=%s, want %s", server.ReadHeaderTimeout, serverReadHeaderTimeout)
	}
	if server.ReadTimeout != 0 {
		t.Fatalf("ReadTimeout=%s, want disabled for websocket/streaming requests", server.ReadTimeout)
	}
	if server.IdleTimeout != serverIdleTimeout {
		t.Fatalf("IdleTimeout=%s, want %s", server.IdleTimeout, serverIdleTimeout)
	}
}

func TestNewGatewayServerSuppressesCorrelationHeaderForProviderRoutes(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Providers = config.ProvidersConfig{
		"openai": {Upstream: "https://api.openai.com", Prefix: "/openai"},
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Test", "preserved")
		_, _ = w.Write([]byte("upstream-body"))
	})
	server := newGatewayServer(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), handler)

	proxyRec := httptest.NewRecorder()
	server.Handler.ServeHTTP(proxyRec, httptest.NewRequest(http.MethodGet, "/openai/v1/models", nil))
	if got := proxyRec.Header().Get(correlation.HeaderName); got != "" {
		t.Fatalf("proxy response correlation header=%q, want empty", got)
	}
	if got := proxyRec.Header().Get("X-Upstream-Test"); got != "preserved" {
		t.Fatalf("upstream header=%q, want preserved", got)
	}
	if got := proxyRec.Body.String(); got != "upstream-body" {
		t.Fatalf("response body=%q, want upstream-body", got)
	}

	apiRec := httptest.NewRecorder()
	server.Handler.ServeHTTP(apiRec, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if got := apiRec.Header().Get(correlation.HeaderName); got == "" {
		t.Fatal("api response correlation header is empty, want gateway header")
	}
}

func TestRequestCorrelationIDPrefersContextThenHeader(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodGet, "/openai/v1/chat/completions", nil)
	req.Header.Set(correlation.HeaderName, "corr-from-header")
	req = req.WithContext(correlation.WithContext(req.Context(), "corr-from-context"))

	if got := requestCorrelationID(req); got != "corr-from-context" {
		t.Fatalf("requestCorrelationID()=%q, want corr-from-context", got)
	}

	reqNoContext := httptest.NewRequest(http.MethodGet, "/openai/v1/chat/completions", nil)
	reqNoContext.Header.Set(correlation.HeaderName, "corr-header-only")
	if got := requestCorrelationID(reqNoContext); got != "corr-header-only" {
		t.Fatalf("requestCorrelationID()=%q, want corr-header-only", got)
	}
}
