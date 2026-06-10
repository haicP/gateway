package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ongoingai/gateway/internal/trace"
)

type prefixTraceStore struct {
	mu          sync.Mutex
	traceByID   map[string]*trace.Trace
	queryResult *trace.TraceResult
	lastFilter  trace.TraceFilter
}

func (s *prefixTraceStore) WriteTrace(context.Context, *trace.Trace) error {
	return nil
}

func (s *prefixTraceStore) WriteBatch(context.Context, []*trace.Trace) error {
	return nil
}

func (s *prefixTraceStore) GetTrace(_ context.Context, id string) (*trace.Trace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.traceByID == nil || s.traceByID[id] == nil {
		return nil, trace.ErrNotFound
	}
	return s.traceByID[id], nil
}

func (s *prefixTraceStore) QueryTraces(_ context.Context, filter trace.TraceFilter) (*trace.TraceResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastFilter = filter
	if s.queryResult != nil {
		return s.queryResult, nil
	}
	return &trace.TraceResult{}, nil
}

func (s *prefixTraceStore) CountTraces(context.Context) (int64, error) {
	return 0, nil
}

func TestTracesHandlerFiltersAndReturnsProviderPrefix(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 26, 1, 2, 3, 0, time.UTC)
	store := &prefixTraceStore{
		queryResult: &trace.TraceResult{
			Items: []*trace.Trace{
				{
					ID:               "trace-prefix",
					Timestamp:        now,
					Provider:         "openai",
					Model:            "gpt-5.5",
					RequestMethod:    http.MethodPost,
					RequestPath:      "/llmgateway/v1/responses",
					LLMRequestPrompt: "hidden prompt",
					ResponseStatus:   http.StatusOK,
					CreatedAt:        now,
				},
			},
			TotalCount: 1,
		},
	}
	handler := NewRouter(RouterOptions{
		AppVersion:       "dev",
		Store:            store,
		ProviderPrefixes: []string{"/llmgateway"},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/traces?prefix=/llmgateway&limit=10", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200: %s", rec.Code, rec.Body.String())
	}
	if store.lastFilter.ProviderPrefix != "/llmgateway" {
		t.Fatalf("provider prefix filter=%q, want /llmgateway", store.lastFilter.ProviderPrefix)
	}

	var body struct {
		Items []struct {
			Provider       string `json:"provider"`
			ProviderPrefix string `json:"provider_prefix"`
			Endpoint       string `json:"endpoint"`
		} `json:"items"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(body.Items) != 1 {
		t.Fatalf("items=%d, want 1", len(body.Items))
	}
	if body.Items[0].Provider != "openai" || body.Items[0].ProviderPrefix != "/llmgateway" {
		t.Fatalf("provider fields=%+v, want openai//llmgateway", body.Items[0])
	}
	if body.Items[0].Endpoint != "/v1/responses" {
		t.Fatalf("endpoint=%q, want /v1/responses", body.Items[0].Endpoint)
	}
	var rawBody map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &rawBody); err != nil {
		t.Fatalf("decode raw response: %v", err)
	}
	rawItems := rawBody["items"].([]any)
	if _, ok := rawItems[0].(map[string]any)["llm_request_prompt"]; ok {
		t.Fatalf("list item unexpectedly included llm_request_prompt: %v", rawItems[0])
	}
}

func TestTracesHandlerRejectsUnknownProviderPrefix(t *testing.T) {
	t.Parallel()

	handler := NewRouter(RouterOptions{
		AppVersion:       "dev",
		Store:            &prefixTraceStore{},
		ProviderPrefixes: []string{"/llmgateway"},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/traces?prefix=/missing", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d, want 400", rec.Code)
	}
}

func TestTraceDetailReturnsProviderPrefix(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 5, 26, 1, 2, 3, 0, time.UTC)
	store := &prefixTraceStore{
		traceByID: map[string]*trace.Trace{
			"trace-prefix": {
				ID:               "trace-prefix",
				Timestamp:        now,
				Provider:         "openai",
				Model:            "gpt-5.5",
				RequestMethod:    http.MethodPost,
				RequestPath:      "/llmgateway/v1/responses",
				LLMRequestPrompt: "latest prompt",
				ResponseStatus:   http.StatusOK,
				CreatedAt:        now,
			},
		},
	}
	handler := NewRouter(RouterOptions{
		AppVersion:       "dev",
		Store:            store,
		ProviderPrefixes: []string{"/llmgateway"},
	})

	req := httptest.NewRequest(http.MethodGet, "/api/traces/trace-prefix", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200: %s", rec.Code, rec.Body.String())
	}

	var body struct {
		Provider       string `json:"provider"`
		ProviderPrefix string `json:"provider_prefix"`
		Endpoint       string `json:"endpoint"`
		RequestPrompt  string `json:"llm_request_prompt"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Provider != "openai" || body.ProviderPrefix != "/llmgateway" {
		t.Fatalf("provider fields=%+v, want openai//llmgateway", body)
	}
	if body.Endpoint != "/v1/responses" {
		t.Fatalf("endpoint=%q, want /v1/responses", body.Endpoint)
	}
	if body.RequestPrompt != "latest prompt" {
		t.Fatalf("llm_request_prompt=%q, want latest prompt", body.RequestPrompt)
	}
}

func TestDashboardInjectsProviderPrefixes(t *testing.T) {
	t.Parallel()

	handler := NewRouter(RouterOptions{
		AppVersion:       "dev",
		DashboardEnabled: true,
		ProviderPrefixes: []string{"/llmgateway", "/openai"},
	})

	req := httptest.NewRequest(http.MethodGet, "/dashboard", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "__PROVIDER_PREFIXES__") {
		t.Fatal("dashboard provider prefix placeholder was not replaced")
	}
	if !strings.Contains(body, `const providerPrefixes = ["/llmgateway","/openai"];`) {
		t.Fatalf("dashboard body missing provider prefix config")
	}
	if !strings.Contains(body, `请求文本`) || !strings.Contains(body, `id="detailPrompt"`) {
		t.Fatal("dashboard body missing request prompt detail block")
	}
	if !strings.Contains(body, `function sha256HexFallback(data)`) || !strings.Contains(body, `globalThis.crypto?.subtle?.digest`) {
		t.Fatal("dashboard body missing local SHA-256 fallback for non-secure browser contexts")
	}
	if strings.Contains(body, `params.set('api_key'`) {
		t.Fatal("dashboard must not send plaintext api keys in trace query params")
	}
}
