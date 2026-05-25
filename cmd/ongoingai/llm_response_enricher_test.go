package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ongoingai/gateway/internal/trace"
)

func TestLLMResponseContentEnricherUpdatesEligibleTrace(t *testing.T) {
	t.Parallel()

	store := newEnricherSQLiteStore(t)
	if err := store.WriteTrace(context.Background(), &trace.Trace{
		ID:             "trace-enrich",
		Provider:       "openai",
		Model:          "gpt-4o-mini",
		RequestMethod:  "POST",
		RequestPath:    "/openai/v1/chat/completions",
		ResponseStatus: 200,
		ResponseBody:   `{"choices":[{"message":{"content":"hello"}}]}`,
		Metadata:       `{"streaming":false}`,
	}); err != nil {
		t.Fatalf("WriteTrace() error: %v", err)
	}

	enricher := newLLMResponseContentEnricher(store, nil, 4, time.Second)
	enricher.Start(context.Background())
	defer func() {
		_ = enricher.Shutdown(context.Background())
	}()
	enricher.EnqueueTraceIDs([]string{"trace-enrich"})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := store.GetTrace(context.Background(), "trace-enrich")
		if err != nil {
			t.Fatalf("GetTrace() error: %v", err)
		}
		if strings.Contains(got.LLMResponseContent, `"hello"`) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := store.GetTrace(context.Background(), "trace-enrich")
	if err != nil {
		t.Fatalf("GetTrace() error: %v", err)
	}
	t.Fatalf("llm_response_content=%q, want enriched content", got.LLMResponseContent)
}

func TestLLMResponseContentEnricherUpdatesWebSocketTraceAfterWriterSuccess(t *testing.T) {
	t.Parallel()

	store := newEnricherSQLiteStore(t)
	writer := trace.NewWriter(store, 8)
	enricher := newLLMResponseContentEnricher(store, nil, 4, time.Second)
	enricher.Start(context.Background())
	defer func() {
		_ = enricher.Shutdown(context.Background())
	}()
	attachLLMResponseContentEnricher(writer, enricher)
	writer.Start(context.Background())

	traceID := "trace-ws-enrich"
	queued := writer.Enqueue(&trace.Trace{
		ID:             traceID,
		Provider:       "openai",
		Model:          "gpt-5.5",
		RequestMethod:  "GET",
		RequestPath:    "/llmgateway/v1/responses",
		ResponseStatus: 101,
		ResponseBody: `[
			{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.output_text.delta","item_id":"msg_1","delta":"ignore"}},
			{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"city\""}},
			{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":":\"Paris\"}"}}
		]`,
		Metadata: `{"transport":"websocket","websocket_turn_terminal_type":"response.completed"}`,
	})
	if !queued {
		t.Fatal("Enqueue() returned false")
	}
	if err := writer.Shutdown(context.Background()); err != nil {
		t.Fatalf("writer Shutdown() error: %v", err)
	}

	waitForLLMResponseContent(t, store, traceID, `"arguments":"{\"city\":\"Paris\"}"`)
}

func TestLLMResponseContentEnricherSkipsLargeBodyRedactionBypass(t *testing.T) {
	t.Parallel()

	store := newEnricherSQLiteStore(t)
	if err := store.WriteTrace(context.Background(), &trace.Trace{
		ID:             "trace-skip",
		Provider:       "openai",
		Model:          "gpt-4o-mini",
		RequestMethod:  "POST",
		RequestPath:    "/openai/v1/chat/completions",
		ResponseStatus: 200,
		ResponseBody:   `{"choices":[{"message":{"content":"secret"}}]}`,
		Metadata:       `{"streaming":false,"body_pii_status":"skipped_large_body","redaction_storage_skipped":true}`,
	}); err != nil {
		t.Fatalf("WriteTrace() error: %v", err)
	}

	enricher := newLLMResponseContentEnricher(store, nil, 4, time.Second)
	enricher.Start(context.Background())
	enricher.EnqueueTraceIDs([]string{"trace-skip"})
	if err := enricher.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() error: %v", err)
	}

	got, err := store.GetTrace(context.Background(), "trace-skip")
	if err != nil {
		t.Fatalf("GetTrace() error: %v", err)
	}
	if got.LLMResponseContent != "" {
		t.Fatalf("llm_response_content=%q, want skipped", got.LLMResponseContent)
	}
}

func waitForLLMResponseContent(t *testing.T, store trace.TraceStore, traceID, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := store.GetTrace(context.Background(), traceID)
		if err != nil {
			t.Fatalf("GetTrace() error: %v", err)
		}
		if strings.Contains(got.LLMResponseContent, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	got, err := store.GetTrace(context.Background(), traceID)
	if err != nil {
		t.Fatalf("GetTrace() error: %v", err)
	}
	t.Fatalf("llm_response_content=%q, want containing %s", got.LLMResponseContent, want)
}

func newEnricherSQLiteStore(t *testing.T) *trace.SQLiteStore {
	t.Helper()
	store, err := trace.NewSQLiteStore(filepath.Join(t.TempDir(), "enricher.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}
