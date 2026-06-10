package trace

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestSQLiteStoreCoreTraceLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	store, err := NewSQLiteStore(filepath.Join(t.TempDir(), "ongoingai.db"))
	if err != nil {
		t.Fatalf("NewSQLiteStore() error: %v", err)
	}
	defer store.Close()

	base := time.Date(2026, 5, 25, 10, 0, 0, 0, time.UTC)
	traces := []*Trace{
		{
			ID:              "trace-old",
			Timestamp:       base.Add(-48 * time.Hour),
			CreatedAt:       base.Add(-48 * time.Hour),
			OrgID:           "default",
			WorkspaceID:     "default",
			Provider:        "llm",
			Model:           "gpt-old",
			RequestMethod:   "POST",
			RequestPath:     "/llm/v1/chat/completions",
			RequestHeaders:  `{"Authorization":["[REDACTED]"]}`,
			RequestBody:     `{"model":"gpt-old"}`,
			ResponseStatus:  200,
			ResponseHeaders: `{"Content-Type":["application/json"]}`,
			ResponseBody:    `{"ok":true}`,
			TotalTokens:     3,
			LatencyMS:       20,
		},
		{
			ID:               "trace-new",
			Timestamp:        base,
			CreatedAt:        base,
			OrgID:            "default",
			WorkspaceID:      "default",
			Provider:         "llm",
			Model:            "gpt-new",
			RequestMethod:    "POST",
			RequestPath:      "/llm/v1/responses",
			RequestHeaders:   `{"X-API-Key":["[REDACTED]"]}`,
			RequestBody:      `{"model":"gpt-new"}`,
			LLMRequestPrompt: "stored prompt",
			ResponseStatus:   201,
			ResponseHeaders:  `{"Content-Type":["application/json"]}`,
			ResponseBody:     `{"usage":{"total_tokens":9}}`,
			TotalTokens:      9,
			LatencyMS:        40,
			Metadata:         `{"streaming":false}`,
		},
	}
	if err := store.WriteBatch(ctx, traces); err != nil {
		t.Fatalf("WriteBatch() error: %v", err)
	}

	count, err := store.CountTraces(ctx)
	if err != nil {
		t.Fatalf("CountTraces() error: %v", err)
	}
	if count != 2 {
		t.Fatalf("CountTraces()=%d, want 2", count)
	}

	got, err := store.GetTrace(ctx, "trace-new")
	if err != nil {
		t.Fatalf("GetTrace() error: %v", err)
	}
	if got.RequestBody != `{"model":"gpt-new"}` || got.ResponseBody == "" {
		t.Fatalf("GetTrace() body=%q/%q, want full request and response bodies", got.RequestBody, got.ResponseBody)
	}
	if got.LLMRequestPrompt != "stored prompt" {
		t.Fatalf("GetTrace() llm_request_prompt=%q, want stored prompt", got.LLMRequestPrompt)
	}
	if err := store.UpdateLLMRequestPrompt(ctx, "trace-new", "updated prompt"); err != nil {
		t.Fatalf("UpdateLLMRequestPrompt() error: %v", err)
	}
	got, err = store.GetTrace(ctx, "trace-new")
	if err != nil {
		t.Fatalf("GetTrace() after prompt update error: %v", err)
	}
	if got.LLMRequestPrompt != "updated prompt" {
		t.Fatalf("updated llm_request_prompt=%q, want updated prompt", got.LLMRequestPrompt)
	}

	page, err := store.QueryTraces(ctx, TraceFilter{Provider: "llm", Limit: 1})
	if err != nil {
		t.Fatalf("QueryTraces() error: %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].ID != "trace-new" {
		t.Fatalf("QueryTraces() items=%v, want newest trace first", traceIDs(page.Items))
	}
	if page.Items[0].LLMRequestPrompt != "" {
		t.Fatalf("summary llm_request_prompt=%q, want empty", page.Items[0].LLMRequestPrompt)
	}
	if page.NextCursor == "" {
		t.Fatal("QueryTraces() next cursor is empty, want pagination cursor")
	}

	exported, err := store.ExportTraces(ctx, TraceExportFilter{Limit: 1})
	if err != nil {
		t.Fatalf("ExportTraces() error: %v", err)
	}
	if len(exported.Items) != 1 || exported.Items[0].ID != "trace-old" {
		t.Fatalf("ExportTraces() items=%v, want oldest trace first for backup", traceIDs(exported.Items))
	}
	if exported.NextCursor == "" {
		t.Fatal("ExportTraces() next cursor is empty, want backup pagination cursor")
	}

	deleted, err := store.DeleteTracesBefore(ctx, base.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("DeleteTracesBefore() error: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("DeleteTracesBefore()=%d, want 1", deleted)
	}
	count, err = store.CountTraces(ctx)
	if err != nil {
		t.Fatalf("CountTraces() after delete error: %v", err)
	}
	if count != 1 {
		t.Fatalf("CountTraces() after delete=%d, want 1", count)
	}
}

func traceIDs(items []*Trace) []string {
	ids := make([]string, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		ids = append(ids, item.ID)
	}
	return ids
}
