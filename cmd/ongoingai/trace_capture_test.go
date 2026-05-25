package main

import (
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ongoingai/gateway/internal/config"
	"github.com/ongoingai/gateway/internal/providers"
	"github.com/ongoingai/gateway/internal/proxy"
)

func TestBuildTraceRecordParsesButDoesNotStoreBodiesWhenDisabled(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/messages",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"X-Api-Key": {"sk-ant-123456"}},
		RequestBody:    []byte(`{"model":"claude-haiku-4-5-20251001"}`),
		ResponseBody:   []byte(`{"usage":{"input_tokens":1000,"output_tokens":500,"total_tokens":1500}}`),
		DurationMS:     123,
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.Provider != "llm" {
		t.Fatalf("provider=%q, want llm", record.Provider)
	}
	if record.Model != "claude-haiku-4-5-20251001" {
		t.Fatalf("model=%q", record.Model)
	}
	if record.InputTokens != 1000 || record.OutputTokens != 500 || record.TotalTokens != 1500 {
		t.Fatalf("usage=%d/%d/%d", record.InputTokens, record.OutputTokens, record.TotalTokens)
	}
	if math.Abs(record.EstimatedCostUSD) > 1e-9 {
		t.Fatalf("estimated_cost_usd=%f", record.EstimatedCostUSD)
	}
	if record.RequestBody != "" || record.ResponseBody != "" {
		t.Fatalf("expected bodies not stored when capture disabled")
	}
	if record.TimeToFirstTokenMS != 0 {
		t.Fatalf("ttft_ms=%d, want 0 for non-stream trace", record.TimeToFirstTokenMS)
	}
	if record.TimeToFirstTokenUS != 0 {
		t.Fatalf("ttft_us=%d, want 0 for non-stream trace", record.TimeToFirstTokenUS)
	}
	if record.APIKeyHash == "" {
		t.Fatal("expected API key hash to be set")
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata["api_key_last4"] != "3456" {
		t.Fatalf("api_key_last4=%v, want 3456", metadata["api_key_last4"])
	}
}

func TestBuildTraceRecordCapturesWebSocketTurnMetadataAndParsesEnvelope(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:                    http.MethodGet,
		Path:                      "/llm/v1/responses",
		StatusCode:                http.StatusSwitchingProtocols,
		RequestHeaders:            http.Header{"Authorization": {"Bearer sk-provider-secret"}},
		ResponseHeaders:           http.Header{"Upgrade": {"websocket"}},
		RequestBody:               []byte(`[{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.create","model":"gpt-5.5","input":[]},"bytes":61}]`),
		ResponseBody:              []byte(`[{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.completed","response":{"usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}}},"bytes":120}]`),
		RequestBodyBytes:          61,
		ResponseBodyBytes:         120,
		Streaming:                 true,
		StreamChunks:              2,
		Transport:                 "websocket",
		WebSocketConnectionID:     "ws-test",
		WebSocketTurnIndex:        2,
		WebSocketTurnStartType:    "response.create",
		WebSocketTurnTerminalType: "response.completed",
		WebSocketRequestMessages:  1,
		WebSocketResponseMessages: 1,
		TimeToFirstTokenUS:        1500,
		DurationMS:                25,
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.Model != "gpt-5.5" {
		t.Fatalf("model=%q, want gpt-5.5", record.Model)
	}
	if record.InputTokens != 11 || record.OutputTokens != 7 || record.TotalTokens != 18 {
		t.Fatalf("usage=%d/%d/%d", record.InputTokens, record.OutputTokens, record.TotalTokens)
	}
	if !strings.Contains(record.RequestBody, "response.create") {
		t.Fatalf("request body=%q", record.RequestBody)
	}
	if !strings.Contains(record.ResponseBody, "response.completed") {
		t.Fatalf("response body=%q", record.ResponseBody)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata["transport"] != "websocket" {
		t.Fatalf("transport metadata=%v", metadata["transport"])
	}
	if metadata["websocket_connection_id"] != "ws-test" {
		t.Fatalf("websocket_connection_id=%v", metadata["websocket_connection_id"])
	}
	if metadata["websocket_turn_start_type"] != "response.create" || metadata["websocket_turn_terminal_type"] != "response.completed" {
		t.Fatalf("websocket metadata=%v", metadata)
	}
	if metadata["websocket_turn_index"] != float64(2) {
		t.Fatalf("websocket_turn_index=%v", metadata["websocket_turn_index"])
	}
	if record.TimeToFirstTokenUS != 1500 || record.TimeToFirstTokenMS != 2 {
		t.Fatalf("ttft us/ms=%d/%d", record.TimeToFirstTokenUS, record.TimeToFirstTokenMS)
	}
}

func TestBuildTraceRecordUsesRequestStartTimeForTimestamp(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	startedAt := time.Date(2026, 2, 12, 1, 0, 0, 123, time.UTC)
	exchange := &proxy.CapturedExchange{
		StartedAt: startedAt,
		Method:    http.MethodPost,
		Path:      "/llm/v1/messages",
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if !record.Timestamp.Equal(startedAt) {
		t.Fatalf("timestamp=%s, want request start %s", record.Timestamp.Format(time.RFC3339Nano), startedAt.Format(time.RFC3339Nano))
	}
	if record.CreatedAt.IsZero() {
		t.Fatal("created_at should still be set")
	}
	if record.CreatedAt.Equal(record.Timestamp) {
		t.Fatal("created_at should reflect trace creation time, not request start")
	}
}

func TestBuildTraceRecordParsesStreamingUsageWithCaptureDisabled(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:             http.MethodPost,
		Path:               "/llm/v1/messages",
		StatusCode:         http.StatusOK,
		Streaming:          true,
		StreamChunks:       2,
		TimeToFirstTokenUS: 42123,
		ResponseBody: []byte("data: {\"model\":\"claude-opus-4-6-20260220\"}\n\n" +
			"data: {\"usage\":{\"input_tokens\":2000,\"output_tokens\":1000,\"total_tokens\":3000}}\n\n" +
			"data: [DONE]\n\n"),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.Model != "claude-opus-4-6-20260220" {
		t.Fatalf("model=%q", record.Model)
	}
	if record.InputTokens != 2000 || record.OutputTokens != 1000 || record.TotalTokens != 3000 {
		t.Fatalf("usage=%d/%d/%d", record.InputTokens, record.OutputTokens, record.TotalTokens)
	}
	if math.Abs(record.EstimatedCostUSD) > 1e-9 {
		t.Fatalf("estimated_cost_usd=%f", record.EstimatedCostUSD)
	}
	if record.TimeToFirstTokenUS != 42123 {
		t.Fatalf("ttft_us=%d, want 42123", record.TimeToFirstTokenUS)
	}
	if record.TimeToFirstTokenMS != 43 {
		t.Fatalf("ttft_ms=%d, want 43", record.TimeToFirstTokenMS)
	}
	if record.ResponseBody != "" {
		t.Fatalf("expected response body not stored when capture disabled")
	}
}

func TestBuildTraceRecordParsesStreamingAnthropicEnvelopeUsageAndModel(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:       http.MethodPost,
		Path:         "/llm/v1/messages",
		StatusCode:   http.StatusOK,
		Streaming:    true,
		StreamChunks: 3,
		ResponseBody: []byte(
			"data: {oops}\n\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-sonnet-4-latest\",\"usage\":{\"input_tokens\":9}}}\n\n" +
				"data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":6}}\n\n" +
				"data: [DONE]\n\n",
		),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.Model != "claude-sonnet-4-latest" {
		t.Fatalf("model=%q, want claude-sonnet-4-latest", record.Model)
	}
	if record.InputTokens != 9 || record.OutputTokens != 6 || record.TotalTokens != 15 {
		t.Fatalf("usage=%d/%d/%d, want 9/6/15", record.InputTokens, record.OutputTokens, record.TotalTokens)
	}
	if record.ResponseBody != "" {
		t.Fatalf("expected response body not stored when capture disabled")
	}
}

func TestBuildTraceRecordBackfillsTTFTUSFromMS(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:             http.MethodPost,
		Path:               "/llm/v1/chat/completions",
		StatusCode:         http.StatusOK,
		Streaming:          true,
		TimeToFirstTokenMS: 42,
		ResponseBody: []byte("data: {\"model\":\"gpt-4o-mini\"}\n\n" +
			"data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n" +
			"data: [DONE]\n\n"),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.TimeToFirstTokenMS != 42 {
		t.Fatalf("ttft_ms=%d, want 42", record.TimeToFirstTokenMS)
	}
	if record.TimeToFirstTokenUS != 42000 {
		t.Fatalf("ttft_us=%d, want 42000", record.TimeToFirstTokenUS)
	}
}

func TestExtractUsageFromSSEMergesPartialUsageAcrossMixedPayloadShapes(t *testing.T) {
	t.Parallel()

	body := []byte(
		"event: message\ndata: {\"usage\":{\"prompt_tokens\":11}}\n\n" +
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":13}}}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n" +
			"data: [DONE]\n\n",
	)

	input, output, total := extractUsageFromSSE(body)
	if input != 13 || output != 5 || total != 18 {
		t.Fatalf("usage=%d/%d/%d, want 13/5/18", input, output, total)
	}
}

func TestExtractUsageFromSSEIgnoresMalformedPayloads(t *testing.T) {
	t.Parallel()

	body := []byte(
		"data: {\"usage\":\n\n" +
			"data: totally-not-json\n\n" +
			"data: {\"usage\":{\"input_tokens\":2,\"output_tokens\":1}}\n\n",
	)

	input, output, total := extractUsageFromSSE(body)
	if input != 2 || output != 1 || total != 3 {
		t.Fatalf("usage=%d/%d/%d, want 2/1/3", input, output, total)
	}
}

func TestExtractModelFromSSESupportsAnthropicMessageEnvelope(t *testing.T) {
	t.Parallel()

	body := []byte(
		"data: {bad}\n\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"model\":\"claude-opus-4-6-20260220\"}}\n\n",
	)

	model := extractModelFromSSE(body)
	if model != "claude-opus-4-6-20260220" {
		t.Fatalf("model=%q, want claude-opus-4-6-20260220", model)
	}
}

func TestBuildTraceRecordUsesDefaultIdentityInLightweightMode(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:             http.MethodPost,
		Path:               "/llm/v1/chat/completions",
		StatusCode:         http.StatusOK,
		GatewayOrgID:       "org-a",
		GatewayWorkspaceID: "workspace-a",
		GatewayKeyID:       "team-a-dev-1",
		GatewayTeam:        "team-a",
		GatewayRole:        "developer",
		ResponseBody:       []byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.OrgID != "default" {
		t.Fatalf("org_id=%q, want default", record.OrgID)
	}
	if record.WorkspaceID != "default" {
		t.Fatalf("workspace_id=%q, want default", record.WorkspaceID)
	}
	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	for _, key := range []string{"gateway_key_id", "team", "role", "org_id", "workspace_id"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q]=%v, want omitted in lightweight mode", key, metadata[key])
		}
	}
}

func TestBuildTraceRecordIncludesCorrelationIDMetadata(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		CorrelationID:  "corr-trace-1",
		ResponseBody:   []byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata["correlation_id"] != "corr-trace-1" {
		t.Fatalf("correlation_id=%v, want corr-trace-1", metadata["correlation_id"])
	}
}

func TestBuildTraceRecordIncludesLineageMetadataAndCheckpoint(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	headers := make(http.Header)
	headers.Set("X-OngoingAI-Trace-Group-ID", "group-1")
	headers.Set("X-OngoingAI-Thread-ID", "thread-1")
	headers.Set("X-OngoingAI-Run-ID", "run-1")
	headers.Set("X-OngoingAI-Parent-Checkpoint-ID", "checkpoint-0")
	headers.Set("X-OngoingAI-Checkpoint-Seq", "2")

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: headers,
		ResponseBody:   []byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.TraceGroupID != "group-1" {
		t.Fatalf("trace_group_id=%q, want group-1", record.TraceGroupID)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if metadata["lineage_group_id"] != "group-1" {
		t.Fatalf("lineage_group_id=%v, want group-1", metadata["lineage_group_id"])
	}
	if metadata["lineage_thread_id"] != "thread-1" {
		t.Fatalf("lineage_thread_id=%v, want thread-1", metadata["lineage_thread_id"])
	}
	if metadata["lineage_run_id"] != "run-1" {
		t.Fatalf("lineage_run_id=%v, want run-1", metadata["lineage_run_id"])
	}
	if metadata["lineage_parent_checkpoint_id"] != "checkpoint-0" {
		t.Fatalf("lineage_parent_checkpoint_id=%v, want checkpoint-0", metadata["lineage_parent_checkpoint_id"])
	}
	if metadata["lineage_checkpoint_id"] != record.ID {
		t.Fatalf("lineage_checkpoint_id=%v, want %q", metadata["lineage_checkpoint_id"], record.ID)
	}
	if metadata["lineage_immutable"] != true {
		t.Fatalf("lineage_immutable=%v, want true", metadata["lineage_immutable"])
	}
	if metadata["lineage_checkpoint_seq"] != float64(2) {
		t.Fatalf("lineage_checkpoint_seq=%v, want 2", metadata["lineage_checkpoint_seq"])
	}
}

func TestBuildTraceRecordRedactsSensitiveRequestHeaders(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	headers := make(http.Header)
	headers.Set("Authorization", "Bearer sk-openai-secret")
	headers.Set("X-API-Key", "sk-anthropic-secret")
	headers.Set("X-OngoingAI-Gateway-Key", "gwk-secret")
	headers.Set("Content-Type", "application/json")

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: headers,
		ResponseBody:   []byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)

	var got map[string][]string
	if err := json.Unmarshal([]byte(record.RequestHeaders), &got); err != nil {
		t.Fatalf("unmarshal request headers: %v", err)
	}

	if value := headerValueIgnoreCase(got, "Authorization"); value != "[REDACTED]" {
		t.Fatalf("authorization header=%q, want [REDACTED]", value)
	}
	if value := headerValueIgnoreCase(got, "X-API-Key"); value != "[REDACTED]" {
		t.Fatalf("x-api-key header=%q, want [REDACTED]", value)
	}
	if value := headerValueIgnoreCase(got, "X-OngoingAI-Gateway-Key"); value != "[REDACTED]" {
		t.Fatalf("gateway key header=%q, want [REDACTED]", value)
	}
	if value := headerValueIgnoreCase(got, "Content-Type"); value != "application/json" {
		t.Fatalf("content-type header=%q, want application/json", value)
	}
}

func TestBuildTraceRecordRedactsSensitiveResponseHeaders(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = false

	responseHeaders := make(http.Header)
	responseHeaders.Set("Set-Cookie", "session=abc123")
	responseHeaders.Set("X-API-Key", "resp-secret")
	responseHeaders.Set("Content-Type", "application/json")

	exchange := &proxy.CapturedExchange{
		Method:          http.MethodPost,
		Path:            "/llm/v1/chat/completions",
		StatusCode:      http.StatusOK,
		RequestHeaders:  http.Header{"Content-Type": {"application/json"}},
		ResponseHeaders: responseHeaders,
		ResponseBody:    []byte(`{"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)

	var got map[string][]string
	if err := json.Unmarshal([]byte(record.ResponseHeaders), &got); err != nil {
		t.Fatalf("unmarshal response headers: %v", err)
	}

	if value := headerValueIgnoreCase(got, "Set-Cookie"); value != "[REDACTED]" {
		t.Fatalf("set-cookie header=%q, want [REDACTED]", value)
	}
	if value := headerValueIgnoreCase(got, "X-API-Key"); value != "[REDACTED]" {
		t.Fatalf("x-api-key header=%q, want [REDACTED]", value)
	}
	if value := headerValueIgnoreCase(got, "Content-Type"); value != "application/json" {
		t.Fatalf("content-type header=%q, want application/json", value)
	}
}

func TestBuildTraceRecordStoresCapturedBodiesWithoutPIIRules(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"email":"alice@example.com","profile":{"phone":"415-555-1212"},"api_key":"sk_test_1234567890"}`),
		ResponseBody:   []byte(`{"ssn":"123-45-6789","token":"ghp_abcd1234efgh5678"}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)

	if !strings.Contains(record.RequestBody, "alice@example.com") || !strings.Contains(record.RequestBody, "415-555-1212") {
		t.Fatalf("request body=%q, want captured raw body", record.RequestBody)
	}
	if !strings.Contains(record.ResponseBody, "123-45-6789") || !strings.Contains(record.ResponseBody, "ghp_abcd1234efgh5678") {
		t.Fatalf("response body=%q, want captured raw body", record.ResponseBody)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	for _, key := range []string{"redaction_mode", "redaction_applied", "redaction_counts"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q]=%v, want omitted in lightweight mode", key, metadata[key])
		}
	}
}

func TestBuildTraceRecordKeepsCapturedBodiesUnchanged(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	reqBody := `{"email":"alice@example.com"}`
	respBody := `{"phone":"415-555-1212"}`
	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(reqBody),
		ResponseBody:   []byte(respBody),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.RequestBody != reqBody {
		t.Fatalf("request body=%q, want unchanged %q", record.RequestBody, reqBody)
	}
	if record.ResponseBody != respBody {
		t.Fatalf("response body=%q, want unchanged %q", record.ResponseBody, respBody)
	}
}

func TestBuildTraceRecordUsesDefaultTenantScope(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"email":"alice@example.com"}`),
		ResponseBody:   []byte(`{"ok":true}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.OrgID != "default" || record.WorkspaceID != "default" {
		t.Fatalf("tenant scope=%s/%s, want default/default", record.OrgID, record.WorkspaceID)
	}
	if record.RequestBody != `{"email":"alice@example.com"}` {
		t.Fatalf("request body=%q, want raw body in lightweight mode", record.RequestBody)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	for _, key := range []string{"redaction_mode", "redaction_policy_id"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q]=%v, want omitted in lightweight mode", key, metadata[key])
		}
	}
}

func TestBuildTraceRecordAlwaysStoresRawBodiesWhenCaptureEnabled(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"email":"alice@example.com"}`),
		ResponseBody:   []byte(`{"ok":true}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.RequestBody != `{"email":"alice@example.com"}` {
		t.Fatalf("request body=%q, want unchanged under scoped off mode", record.RequestBody)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	for _, key := range []string{"redaction_mode", "redaction_policy_id"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q]=%v, want omitted in lightweight mode", key, metadata[key])
		}
	}
}

func TestBuildTraceRecordCapturesRequestAndResponseBodiesTogether(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"application/json"}},
		RequestBody:    []byte(`{"email":"alice@example.com"}`),
		ResponseBody:   []byte(`{"email":"bob@example.com"}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.RequestBody != `{"email":"alice@example.com"}` {
		t.Fatalf("request body=%q, want unchanged", record.RequestBody)
	}
	if record.ResponseBody != `{"email":"bob@example.com"}` {
		t.Fatalf("response body=%q, want unchanged", record.ResponseBody)
	}
}

func TestBuildTraceRecordOmitsRedactionMetadataForTruncatedBody(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:               http.MethodPost,
		Path:                 "/llm/v1/chat/completions",
		StatusCode:           http.StatusOK,
		RequestHeaders:       http.Header{"Content-Type": {"application/json"}},
		RequestBody:          []byte(`{"email":"alice@example.com"}`),
		RequestBodyTruncated: true,
		ResponseBody:         []byte(`{"ok":true}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	if _, ok := metadata["redaction_truncated"]; ok {
		t.Fatalf("redaction_truncated=%v, want omitted in lightweight mode", metadata["redaction_truncated"])
	}
}

func TestBuildTraceRecordStoresInvalidUTF8BodyBytes(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Tracing.CaptureBodies = true

	exchange := &proxy.CapturedExchange{
		Method:         http.MethodPost,
		Path:           "/llm/v1/chat/completions",
		StatusCode:     http.StatusOK,
		RequestHeaders: http.Header{"Content-Type": {"text/plain"}},
		RequestBody:    []byte{0xff, 0xfe, 0xfd},
		ResponseBody:   []byte(`{"ok":true}`),
	}

	record := buildTraceRecord(cfg, providers.DefaultRegistry(), exchange)
	if record.RequestBody == "" || record.ResponseBody != `{"ok":true}` {
		t.Fatalf("expected raw bodies stored, got request=%q response=%q", record.RequestBody, record.ResponseBody)
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(record.Metadata), &metadata); err != nil {
		t.Fatalf("unmarshal metadata: %v", err)
	}
	for _, key := range []string{"redaction_storage_drop", "redaction_failure_semantics"} {
		if _, ok := metadata[key]; ok {
			t.Fatalf("metadata[%q]=%v, want omitted in lightweight mode", key, metadata[key])
		}
	}
}

func headerValueIgnoreCase(headers map[string][]string, name string) string {
	for key, values := range headers {
		if !strings.EqualFold(key, name) {
			continue
		}
		if len(values) == 0 {
			return ""
		}
		return values[0]
	}
	return ""
}
