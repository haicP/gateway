package proxy

import (
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ongoingai/gateway/internal/pathutil"
)

type recordingRoundTripper struct {
	base  http.RoundTripper
	calls atomic.Int64
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	cloned.Header.Set("X-Test-Transport", "set")
	return r.base.RoundTrip(cloned)
}

func (r *recordingRoundTripper) Calls() int64 {
	return r.calls.Load()
}

func TestRouterMatchPathBoundaries(t *testing.T) {
	t.Parallel()

	router := NewRouter(DefaultRoutes())

	tests := []struct {
		path      string
		wantMatch bool
	}{
		{path: "/llm", wantMatch: true},
		{path: "/llm/v1/chat/completions", wantMatch: true},
		{path: "/llm/v1/messages", wantMatch: true},
		{path: "/llmish", wantMatch: false},
		{path: "/v1/chat/completions", wantMatch: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()
			_, ok := router.Match(tt.path)
			if ok != tt.wantMatch {
				t.Fatalf("path %q match=%t, want %t", tt.path, ok, tt.wantMatch)
			}
		})
	}
}

func TestHandlerProxiesAndStripsPrefix(t *testing.T) {
	t.Parallel()

	var gotPath string
	var gotQuery string
	var gotMethod string
	var gotBody string
	var gotHost string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotMethod = r.Method
		gotBody = string(body)
		gotHost = r.Host
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	fallbackCalled := false
	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fallbackCalled = true
		w.WriteHeader(http.StatusNoContent)
	})

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	handler, err := NewHandler([]Route{
		{Prefix: "/openai", Upstream: upstream.URL},
	}, logger, fallback)
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/openai/v1/chat/completions?stream=true", strings.NewReader(`{"model":"gpt-4o-mini"}`))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusCreated)
	}
	if fallbackCalled {
		t.Fatal("fallback handler should not have been called")
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("upstream path %q, want %q", gotPath, "/v1/chat/completions")
	}
	if gotQuery != "stream=true" {
		t.Fatalf("upstream query %q, want %q", gotQuery, "stream=true")
	}
	if gotMethod != http.MethodPost {
		t.Fatalf("upstream method %q, want %q", gotMethod, http.MethodPost)
	}
	if gotBody != `{"model":"gpt-4o-mini"}` {
		t.Fatalf("upstream body %q, want %q", gotBody, `{"model":"gpt-4o-mini"}`)
	}
	if gotHost != strings.TrimPrefix(upstream.URL, "http://") {
		t.Fatalf("upstream host %q, want %q", gotHost, strings.TrimPrefix(upstream.URL, "http://"))
	}
}

func TestHandlerStripsConfiguredPrefix(t *testing.T) {
	t.Parallel()

	var gotPath string
	var gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	handler, err := NewHandler([]Route{
		{Prefix: "/myllm", Upstream: upstream.URL},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/myllm/v1/chat/completions?stream=true", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusOK)
	}
	if gotPath != "/v1/chat/completions" {
		t.Fatalf("upstream path %q, want %q", gotPath, "/v1/chat/completions")
	}
	if gotQuery != "stream=true" {
		t.Fatalf("upstream query %q, want %q", gotQuery, "stream=true")
	}
}

func TestHandlerProxiesLLMCompatibilityPaths(t *testing.T) {
	t.Parallel()

	var gotPath string
	var gotQuery string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	handler, err := NewHandler([]Route{
		{Prefix: "/llm", Upstream: upstream.URL},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	tests := []struct {
		name      string
		method    string
		path      string
		wantPath  string
		wantQuery string
	}{
		{name: "claude messages", method: http.MethodPost, path: "/llm/v1/messages", wantPath: "/v1/messages"},
		{name: "openai responses subpath", method: http.MethodPost, path: "/llm/v1/responses/compact", wantPath: "/v1/responses/compact"},
		{name: "openai chat completions", method: http.MethodPost, path: "/llm/v1/chat/completions", wantPath: "/v1/chat/completions"},
		{name: "openai image edits", method: http.MethodPost, path: "/llm/v1/images/edits", wantPath: "/v1/images/edits"},
		{name: "gemini stream", method: http.MethodPost, path: "/llm/v1beta/models/gemini-pro:streamGenerateContent?alt=sse", wantPath: "/v1beta/models/gemini-pro:streamGenerateContent", wantQuery: "alt=sse"},
		{name: "antigravity messages", method: http.MethodPost, path: "/llm/antigravity/v1/messages", wantPath: "/antigravity/v1/messages"},
		{name: "codex responses", method: http.MethodGet, path: "/llm/backend-api/codex/responses", wantPath: "/backend-api/codex/responses"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath = ""
			gotQuery = ""
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d, want %d", rec.Code, http.StatusOK)
			}
			if gotPath != tt.wantPath {
				t.Fatalf("upstream path=%q, want %q", gotPath, tt.wantPath)
			}
			if gotQuery != tt.wantQuery {
				t.Fatalf("upstream query=%q, want %q", gotQuery, tt.wantQuery)
			}
		})
	}
}

func TestHandlerFallsBackWhenNoRouteMatches(t *testing.T) {
	t.Parallel()

	fallback := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	handler, err := NewHandler(DefaultRoutes(), slog.Default(), fallback)
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestNewHandlerRejectsInvalidUpstream(t *testing.T) {
	t.Parallel()

	_, err := NewHandler([]Route{
		{Prefix: "/openai", Upstream: "://missing-scheme"},
	}, slog.Default(), http.NotFoundHandler())
	if err == nil {
		t.Fatal("expected error for invalid upstream URL")
	}
}

func TestHandlerReturnsBadGatewayWhenUpstreamUnavailable(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	handler, err := NewHandler([]Route{
		{Prefix: "/openai", Upstream: "http://" + addr},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/openai/v1/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusBadGateway)
	}
	if !strings.Contains(rec.Body.String(), "upstream request failed") {
		t.Fatalf("body=%q, want upstream request failed", rec.Body.String())
	}
}

func TestHandlerReturnsBadGatewayWhenUpstreamConnectionDrops(t *testing.T) {
	t.Parallel()

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatalf("response writer does not support hijacking")
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("hijack connection: %v", err)
		}
		_ = conn.Close()
	}))
	defer upstream.Close()

	handler, err := NewHandler([]Route{
		{Prefix: "/openai", Upstream: upstream.URL},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/openai/v1/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusBadGateway)
	}
	if !strings.Contains(rec.Body.String(), "upstream request failed") {
		t.Fatalf("body=%q, want upstream request failed", rec.Body.String())
	}
}

func TestHandlerUsesConfiguredTransport(t *testing.T) {
	t.Parallel()

	var gotTransportHeader string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTransportHeader = r.Header.Get("X-Test-Transport")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	transport := &recordingRoundTripper{base: http.DefaultTransport}
	handler, err := NewHandlerWithOptions([]Route{
		{Prefix: "/openai", Upstream: upstream.URL},
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler(), HandlerOptions{
		Transport: transport,
	})
	if err != nil {
		t.Fatalf("NewHandlerWithOptions error: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/openai/v1/models", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status code %d, want %d", rec.Code, http.StatusOK)
	}
	if gotTransportHeader != "set" {
		t.Fatalf("upstream X-Test-Transport header=%q, want %q", gotTransportHeader, "set")
	}
	if transport.Calls() == 0 {
		t.Fatal("expected custom transport RoundTrip to be called")
	}
}

func TestStripPathPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		prefix string
		want   string
	}{
		{name: "exact prefix returns slash", path: "/openai", prefix: "/openai", want: "/"},
		{name: "strips nested path", path: "/openai/v1/chat/completions", prefix: "/openai", want: "/v1/chat/completions"},
		{name: "does not strip similar prefix", path: "/openaiish/v1", prefix: "/openai", want: "/openaiish/v1"},
		{name: "normalizes prefix slash", path: "/openai/v1", prefix: "openai", want: "/v1"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := pathutil.StripPathPrefix(tt.path, tt.prefix); got != tt.want {
				t.Fatalf("StripPathPrefix(%q, %q)=%q, want %q", tt.path, tt.prefix, got, tt.want)
			}
		})
	}
}
