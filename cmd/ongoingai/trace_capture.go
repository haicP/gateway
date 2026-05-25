package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ongoingai/gateway/internal/config"
	"github.com/ongoingai/gateway/internal/pathutil"
	"github.com/ongoingai/gateway/internal/providers"
	"github.com/ongoingai/gateway/internal/proxy"
	"github.com/ongoingai/gateway/internal/trace"
)

const (
	lineageTraceGroupHeader       = "X-OngoingAI-Trace-Group-ID"
	lineageThreadHeader           = "X-OngoingAI-Thread-ID"
	lineageRunHeader              = "X-OngoingAI-Run-ID"
	lineageParentCheckpointHeader = "X-OngoingAI-Parent-Checkpoint-ID"
	lineageCheckpointSeqHeader    = "X-OngoingAI-Checkpoint-Seq"
	metadataParseMaxSize          = 1 << 20
)

type lineageContext struct {
	groupID            string
	threadID           string
	runID              string
	parentCheckpointID string
	checkpointSeq      int
	checkpointSeqSet   bool
}

func buildTraceRecord(cfg config.Config, registry *providers.Registry, exchange *proxy.CapturedExchange) *trace.Trace {
	now := time.Now().UTC()
	traceTimestamp := now
	if exchange.StartedAt.IsZero() {
		traceTimestamp = now
	} else {
		traceTimestamp = exchange.StartedAt.UTC()
	}
	traceID := newTraceID()

	provider := detectProvider(cfg, exchange.Path)
	model := extractModel(exchange.RequestBody, exchange.ResponseBody, exchange.Streaming)
	inputTokens, outputTokens, totalTokens := extractUsage(exchange.ResponseBody, exchange.Streaming)
	lineage := extractLineageContext(exchange.RequestHeaders)
	requestHeaders := redactSensitiveHeaders(exchange.RequestHeaders)
	responseHeaders := redactSensitiveHeaders(exchange.ResponseHeaders)
	apiKey := extractAPIKey(exchange.RequestHeaders)
	apiKeyHash := ""
	last4 := ""
	if apiKey != "" {
		apiKeyHash = hashSHA256(apiKey)
		last4 = tail(apiKey, 4)
	}

	requestBody := ""
	responseBody := ""
	if cfg.Tracing.CaptureBodies {
		if exchange.RequestBodyPath == "" && exchange.ResponseBodyPath == "" {
			requestBody = string(exchange.RequestBody)
			responseBody = string(exchange.ResponseBody)
		}
	}

	estimatedCostUSD := 0.0
	if registry != nil {
		if providerAdapter, ok := registry.Get(provider); ok {
			estimatedCostUSD = providerAdapter.EstimateCost(model, inputTokens, outputTokens)
		}
	}

	metadata := map[string]any{
		"streaming":             exchange.Streaming,
		"stream_chunks":         exchange.StreamChunks,
		"lineage_checkpoint_id": traceID,
		"lineage_immutable":     true,
	}
	if exchange.Transport != "" {
		metadata["transport"] = exchange.Transport
	}
	if exchange.WebSocketConnectionID != "" {
		metadata["websocket_connection_id"] = exchange.WebSocketConnectionID
	}
	if exchange.WebSocketTurnIndex > 0 {
		metadata["websocket_turn_index"] = exchange.WebSocketTurnIndex
	}
	if exchange.WebSocketTurnStartType != "" {
		metadata["websocket_turn_start_type"] = exchange.WebSocketTurnStartType
	}
	if exchange.WebSocketTurnTerminalType != "" {
		metadata["websocket_turn_terminal_type"] = exchange.WebSocketTurnTerminalType
	}
	if exchange.WebSocketRequestMessages > 0 {
		metadata["websocket_request_messages"] = exchange.WebSocketRequestMessages
	}
	if exchange.WebSocketResponseMessages > 0 {
		metadata["websocket_response_messages"] = exchange.WebSocketResponseMessages
	}
	if exchange.WebSocketTurnIncomplete {
		metadata["websocket_turn_incomplete"] = true
	}
	if exchange.WebSocketCloseCode > 0 {
		metadata["websocket_close_code"] = exchange.WebSocketCloseCode
	}
	if lineage.groupID != "" {
		metadata["lineage_group_id"] = lineage.groupID
	}
	if lineage.threadID != "" {
		metadata["lineage_thread_id"] = lineage.threadID
	}
	if lineage.runID != "" {
		metadata["lineage_run_id"] = lineage.runID
	}
	if lineage.parentCheckpointID != "" {
		metadata["lineage_parent_checkpoint_id"] = lineage.parentCheckpointID
	}
	if lineage.checkpointSeqSet {
		metadata["lineage_checkpoint_seq"] = lineage.checkpointSeq
	}
	if last4 != "" {
		metadata["api_key_last4"] = last4
	}
	if correlationID := strings.TrimSpace(exchange.CorrelationID); correlationID != "" {
		metadata["correlation_id"] = correlationID
	}

	metadataJSON, _ := json.Marshal(metadata)
	timeToFirstTokenUS, timeToFirstTokenMS := normalizeTTFT(exchange.TimeToFirstTokenUS, exchange.TimeToFirstTokenMS)

	return &trace.Trace{
		ID:                    traceID,
		TraceGroupID:          lineage.groupID,
		Timestamp:             traceTimestamp,
		OrgID:                 "default",
		WorkspaceID:           "default",
		Provider:              provider,
		Model:                 nonEmpty(model, "unknown"),
		RequestMethod:         nonEmpty(exchange.Method, "UNKNOWN"),
		RequestPath:           nonEmpty(exchange.Path, "/"),
		RequestHeaders:        headersToJSON(requestHeaders),
		RequestBody:           requestBody,
		RequestBodyPath:       exchange.RequestBodyPath,
		RequestBodyBytes:      exchange.RequestBodyBytes,
		RequestBodySHA256:     exchange.RequestBodySHA256,
		RequestBodyTruncated:  exchange.RequestBodyTruncated,
		ResponseStatus:        exchange.StatusCode,
		ResponseHeaders:       headersToJSON(responseHeaders),
		ResponseBody:          responseBody,
		ResponseBodyPath:      exchange.ResponseBodyPath,
		ResponseBodyBytes:     exchange.ResponseBodyBytes,
		ResponseBodySHA256:    exchange.ResponseBodySHA256,
		ResponseBodyTruncated: exchange.ResponseBodyTruncated,
		InputTokens:           inputTokens,
		OutputTokens:          outputTokens,
		TotalTokens:           totalTokens,
		LatencyMS:             exchange.DurationMS,
		TimeToFirstTokenMS:    timeToFirstTokenMS,
		TimeToFirstTokenUS:    timeToFirstTokenUS,
		APIKeyHash:            apiKeyHash,
		EstimatedCostUSD:      estimatedCostUSD,
		Metadata:              string(metadataJSON),
		CreatedAt:             now,
	}
}

func extractLineageContext(headers http.Header) lineageContext {
	seq, seqSet := parseLineageCheckpointSeq(headers.Get(lineageCheckpointSeqHeader))
	return lineageContext{
		groupID:            strings.TrimSpace(headers.Get(lineageTraceGroupHeader)),
		threadID:           strings.TrimSpace(headers.Get(lineageThreadHeader)),
		runID:              strings.TrimSpace(headers.Get(lineageRunHeader)),
		parentCheckpointID: strings.TrimSpace(headers.Get(lineageParentCheckpointHeader)),
		checkpointSeq:      seq,
		checkpointSeqSet:   seqSet,
	}
}

func parseLineageCheckpointSeq(raw string) (int, bool) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, false
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 0 {
		return 0, false
	}
	return parsed, true
}

func normalizeTTFT(us, ms int64) (int64, int64) {
	if us > 0 {
		if ms <= 0 {
			ms = (us + 999) / 1000
		}
		return us, ms
	}

	if ms > 0 {
		return ms * 1000, ms
	}

	return 0, 0
}

func detectProvider(cfg config.Config, path string) string {
	for _, name := range sortedProviderNames(cfg.Providers) {
		if pathutil.HasPathPrefix(path, cfg.Providers[name].Prefix) {
			return name
		}
	}
	return "unknown"
}

func extractModel(requestBody, responseBody []byte, streaming bool) string {
	if model := extractModelFromJSON(requestBody); model != "" {
		return model
	}
	if streaming {
		if json.Valid(responseBody) {
			return extractModelFromJSON(responseBody)
		}
		return extractModelFromSSE(responseBody)
	}
	return extractModelFromJSON(responseBody)
}

func extractModelFromJSON(body []byte) string {
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return ""
	}
	return extractModelFromValue(payload)
}

func extractModelFromSSE(body []byte) string {
	for _, payload := range ssePayloads(body) {
		if model := extractModelFromJSON(payload); model != "" {
			return model
		}
	}
	return ""
}

func extractUsage(body []byte, streaming bool) (int, int, int) {
	if streaming {
		if json.Valid(body) {
			return extractUsageFromJSON(body)
		}
		return extractUsageFromSSE(body)
	}
	return extractUsageFromJSON(body)
}

func extractUsageFromSSE(body []byte) (int, int, int) {
	input, output := 0, 0
	total := 0
	hasExplicitTotal := false
	for _, payload := range ssePayloads(body) {
		nextInput, nextOutput, nextTotal := extractUsageFromJSON(payload)
		if nextInput > 0 {
			input = nextInput
		}
		if nextOutput > 0 {
			output = nextOutput
		}
		if nextTotal > 0 {
			total = nextTotal
			hasExplicitTotal = true
		}
	}
	if !hasExplicitTotal {
		total = input + output
	} else if total < input+output {
		// Streaming providers can emit partial usage updates where total is
		// stale relative to later input/output updates.
		total = input + output
	}
	return input, output, total
}

func extractUsageFromJSON(body []byte) (int, int, int) {
	var payload any
	if err := json.Unmarshal(body, &payload); err != nil {
		return 0, 0, 0
	}

	return extractUsageFromValue(payload)
}

func extractUsageFromValue(payload any) (int, int, int) {
	switch typed := payload.(type) {
	case []any:
		input, output, total := 0, 0, 0
		hasExplicitTotal := false
		for _, item := range typed {
			nextInput, nextOutput, nextTotal := extractUsageFromValue(item)
			if nextInput > 0 {
				input = nextInput
			}
			if nextOutput > 0 {
				output = nextOutput
			}
			if nextTotal > 0 {
				total = nextTotal
				hasExplicitTotal = true
			}
		}
		if !hasExplicitTotal {
			total = input + output
		} else if total < input+output {
			total = input + output
		}
		return input, output, total
	case map[string]any:
		if nested, ok := typed["payload"]; ok {
			if input, output, total := extractUsageFromValue(nested); input > 0 || output > 0 || total > 0 {
				return input, output, total
			}
		}
		if nested, ok := typed["response"]; ok {
			if input, output, total := extractUsageFromValue(nested); input > 0 || output > 0 || total > 0 {
				return input, output, total
			}
		}
		return extractUsageFromPayload(typed)
	default:
		return 0, 0, 0
	}
}

func extractUsageFromPayload(payload map[string]any) (int, int, int) {
	usageObj := extractUsageObject(payload)
	if usageObj == nil {
		return 0, 0, 0
	}

	input := firstInt(usageObj, "prompt_tokens", "input_tokens")
	output := firstInt(usageObj, "completion_tokens", "output_tokens")
	total := firstInt(usageObj, "total_tokens")
	if total == 0 {
		total = input + output
	}

	return input, output, total
}

func extractModelFromPayload(payload map[string]any) string {
	return extractModelFromValue(payload)
}

func extractModelFromValue(payload any) string {
	switch typed := payload.(type) {
	case []any:
		for _, item := range typed {
			if model := extractModelFromValue(item); model != "" {
				return model
			}
		}
		return ""
	case map[string]any:
		if nested, ok := typed["payload"]; ok {
			if model := extractModelFromValue(nested); model != "" {
				return model
			}
		}
		if nested, ok := typed["response"]; ok {
			if model := extractModelFromValue(nested); model != "" {
				return model
			}
		}
		return extractModelFromPayloadMap(typed)
	default:
		return ""
	}
}

func extractModelFromPayloadMap(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if model, ok := payload["model"].(string); ok {
		model = strings.TrimSpace(model)
		if model != "" {
			return model
		}
	}

	// Anthropic message_start events put model under message.model.
	if message, ok := payload["message"].(map[string]any); ok {
		if model, ok := message["model"].(string); ok {
			model = strings.TrimSpace(model)
			if model != "" {
				return model
			}
		}
	}
	return ""
}

func extractUsageObject(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}
	if usageObj, ok := payload["usage"].(map[string]any); ok {
		return usageObj
	}
	// Anthropic message_start events can place usage under message.usage.
	if message, ok := payload["message"].(map[string]any); ok {
		if usageObj, ok := message["usage"].(map[string]any); ok {
			return usageObj
		}
	}
	if response, ok := payload["response"].(map[string]any); ok {
		if usageObj, ok := response["usage"].(map[string]any); ok {
			return usageObj
		}
	}
	return nil
}

func ssePayloads(body []byte) [][]byte {
	lines := strings.Split(string(body), "\n")
	payloads := make([][]byte, 0, len(lines))
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" || payload == "[DONE]" {
			continue
		}
		payloads = append(payloads, []byte(payload))
	}
	return payloads
}

func firstInt(values map[string]any, keys ...string) int {
	for _, key := range keys {
		v, ok := values[key]
		if !ok {
			continue
		}
		switch typed := v.(type) {
		case float64:
			return int(typed)
		case int:
			return typed
		}
	}
	return 0
}

func headersToJSON(headers http.Header) string {
	if headers == nil {
		return "{}"
	}
	data, err := json.Marshal(headers)
	if err != nil {
		return "{}"
	}
	return string(data)
}

func redactSensitiveHeaders(headers http.Header) http.Header {
	redacted := headers.Clone()
	for _, name := range []string{"authorization", "cookie", "set-cookie", "x-api-key", "x-ongoingai-gateway-key"} {
		if _, ok := redacted[http.CanonicalHeaderKey(name)]; ok {
			redacted.Set(name, "[REDACTED]")
		}
	}
	return redacted
}

func extractAPIKey(headers http.Header) string {
	auth := strings.TrimSpace(headers.Get("Authorization"))
	if auth != "" {
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			return strings.TrimSpace(auth[7:])
		}
		return auth
	}
	return strings.TrimSpace(headers.Get("X-API-Key"))
}

func hashSHA256(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func tail(value string, size int) string {
	if size <= 0 || len(value) <= size {
		return value
	}
	return value[len(value)-size:]
}

func newTraceID() string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return fmt.Sprintf("trace-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(bytes[:])
}

func nonEmpty(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
