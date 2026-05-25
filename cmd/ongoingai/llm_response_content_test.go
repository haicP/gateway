package main

import (
	"encoding/json"
	"testing"
)

func TestExtractLLMResponseContentOpenAIChatStreaming(t *testing.T) {
	t.Parallel()

	body := []byte(
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"Hel\"}}]}\n\n" +
			"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n" +
			"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\"\"}}]}}]}\n\n" +
			"data: {\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\":\\\"sf\\\"}\"}}]}}]}\n\n" +
			"data: [DONE]\n\n",
	)

	doc := mustExtractLLMResponseContent(t, body, true)
	if got := findLLMPart(t, doc, "text")["text"]; got != "Hello" {
		t.Fatalf("text=%v, want Hello", got)
	}
	tool := findLLMPart(t, doc, "tool_call")
	if tool["id"] != "call_1" || tool["name"] != "lookup" || tool["arguments"] != `{"q":"sf"}` {
		t.Fatalf("tool part=%v", tool)
	}
}

func TestExtractLLMResponseContentOpenAIResponsesStreaming(t *testing.T) {
	t.Parallel()

	body := []byte(
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"content_index\":0,\"delta\":\"Hi \"}\n\n" +
			"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"content_index\":0,\"delta\":\"there\"}\n\n" +
			"event: response.reasoning_summary_text.delta\ndata: {\"type\":\"response.reasoning_summary_text.delta\",\"item_id\":\"rs_1\",\"delta\":\"thinking\"}\n\n" +
			"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"delta\":\"{\\\"city\\\"\"}\n\n" +
			"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"item_id\":\"fc_1\",\"delta\":\":\\\"Paris\\\"}\"}\n\n",
	)

	doc := mustExtractLLMResponseContent(t, body, true)
	if got := findLLMPart(t, doc, "text")["text"]; got != "Hi there" {
		t.Fatalf("text=%v, want Hi there", got)
	}
	if got := findLLMPart(t, doc, "thinking")["text"]; got != "thinking" {
		t.Fatalf("thinking=%v, want thinking", got)
	}
	if got := findLLMPart(t, doc, "tool_call")["arguments"]; got != `{"city":"Paris"}` {
		t.Fatalf("arguments=%v, want city JSON", got)
	}
}

func TestExtractLLMResponseContentAnthropicStreaming(t *testing.T) {
	t.Parallel()

	body := []byte(
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Bon\"}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"jour\"}}\n\n" +
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"weather\",\"input\":{}}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\\\"Paris\\\"}\"}}\n\n" +
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"considering\"}}\n\n",
	)

	doc := mustExtractLLMResponseContent(t, body, true)
	if got := findLLMPart(t, doc, "text")["text"]; got != "Bonjour" {
		t.Fatalf("text=%v, want Bonjour", got)
	}
	tool := findLLMPart(t, doc, "tool_call")
	if tool["id"] != "toolu_1" || tool["name"] != "weather" || tool["arguments"] != `{"city":"Paris"}` {
		t.Fatalf("tool part=%v", tool)
	}
	if got := findLLMPart(t, doc, "thinking")["text"]; got != "considering" {
		t.Fatalf("thinking=%v, want considering", got)
	}
}

func TestExtractLLMResponseContentNonStreamingResponsesAndAnthropic(t *testing.T) {
	t.Parallel()

	responseBody := []byte(`{
		"output": [
			{"type":"reasoning","summary":[{"text":"checked policy"}]},
			{"type":"message","content":[{"type":"output_text","text":"Done"}]},
			{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"id\":\"42\"}"}
		]
	}`)
	responseDoc := mustExtractLLMResponseContent(t, responseBody, false)
	if got := findLLMPart(t, responseDoc, "text")["text"]; got != "Done" {
		t.Fatalf("response text=%v, want Done", got)
	}
	if got := findLLMPart(t, responseDoc, "thinking")["text"]; got != "checked policy" {
		t.Fatalf("response thinking=%v, want checked policy", got)
	}
	if got := findLLMPart(t, responseDoc, "tool_call")["arguments"]; got != `{"id":"42"}` {
		t.Fatalf("response arguments=%v, want id JSON", got)
	}

	anthropicBody := []byte(`{
		"content": [
			{"type":"text","text":"Salut"},
			{"type":"thinking","thinking":"internal summary"},
			{"type":"tool_use","id":"toolu_2","name":"lookup","input":{"id":"abc"}}
		]
	}`)
	anthropicDoc := mustExtractLLMResponseContent(t, anthropicBody, false)
	if got := findLLMPart(t, anthropicDoc, "text")["text"]; got != "Salut" {
		t.Fatalf("anthropic text=%v, want Salut", got)
	}
	if got := findLLMPart(t, anthropicDoc, "thinking")["text"]; got != "internal summary" {
		t.Fatalf("anthropic thinking=%v, want internal summary", got)
	}
	tool := findLLMPart(t, anthropicDoc, "tool_call")
	if tool["id"] != "toolu_2" || tool["name"] != "lookup" {
		t.Fatalf("anthropic tool part=%v", tool)
	}
	if input, ok := tool["input"].(map[string]any); !ok || input["id"] != "abc" {
		t.Fatalf("anthropic tool input=%v", tool["input"])
	}
}

func TestExtractLLMResponseContentWebSocketCapturedMessages(t *testing.T) {
	t.Parallel()

	body := []byte(`[
		{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.output_text.delta","item_id":"msg_1","content_index":0,"delta":"ignore"}},
		{"direction":"upstream_to_client","opcode":"binary","payload_base64":"AAAA"},
		{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.output_text.delta","item_id":"msg_1","content_index":0,"delta":"Hi "}},
		{"direction":"upstream_to_client","opcode":"text","payload":"{\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"content_index\":0,\"delta\":\"there\"}"},
		{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"city\""}},
		{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":":\"Paris\"}"}},
		{"direction":"upstream_to_client","opcode":"text","payload":"{oops}"}
	]`)

	doc := mustExtractLLMResponseContent(t, body, false)
	if got := findLLMPart(t, doc, "text")["text"]; got != "Hi there" {
		t.Fatalf("text=%v, want Hi there", got)
	}
	if got := findLLMPart(t, doc, "tool_call")["arguments"]; got != `{"city":"Paris"}` {
		t.Fatalf("arguments=%v, want city JSON", got)
	}
}

func TestExtractLLMResponseContentWebSocketDoneAndCompletedFinalResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		eventType string
		want      string
	}{
		{name: "done", eventType: "response.done", want: "done text"},
		{name: "completed", eventType: "response.completed", want: "completed text"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := []byte(`[
				{"direction":"upstream_to_client","opcode":"text","payload":{
					"type":"` + tt.eventType + `",
					"response":{"output":[{"type":"message","content":[{"type":"output_text","text":"` + tt.want + `"}]}]}
				}}
			]`)

			doc := mustExtractLLMResponseContent(t, body, false)
			if got := findLLMPart(t, doc, "text")["text"]; got != tt.want {
				t.Fatalf("text=%v, want %s", got, tt.want)
			}
		})
	}
}

func TestExtractLLMResponseContentIgnoresNonSemanticWebSocketEnvelope(t *testing.T) {
	t.Parallel()

	body := []byte(`[
		{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.output_text.delta","delta":"request"}},
		{"direction":"upstream_to_client","opcode":"binary","payload_base64":"AAAA"},
		{"direction":"upstream_to_client","opcode":"text","payload":"{oops}"}
	]`)

	if raw, ok := extractLLMResponseContentJSON(body, false); ok {
		t.Fatalf("content=%s, want no extracted content", raw)
	}
}

func TestExtractLLMResponseContentIgnoresMalformedSSE(t *testing.T) {
	t.Parallel()

	body := []byte(
		"data: {oops}\n\n" +
			"data: not-json\n\n" +
			"data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n",
	)

	doc := mustExtractLLMResponseContent(t, body, true)
	if got := findLLMPart(t, doc, "text")["text"]; got != "ok" {
		t.Fatalf("text=%v, want ok", got)
	}
}

func mustExtractLLMResponseContent(t *testing.T, body []byte, streaming bool) map[string]any {
	t.Helper()
	raw, ok := extractLLMResponseContentJSON(body, streaming)
	if !ok {
		t.Fatal("expected llm response content")
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(raw), &doc); err != nil {
		t.Fatalf("unmarshal llm response content: %v", err)
	}
	if doc["schema_version"] != llmResponseContentSchemaVersion {
		t.Fatalf("schema_version=%v", doc["schema_version"])
	}
	return doc
}

func findLLMPart(t *testing.T, doc map[string]any, typ string) map[string]any {
	t.Helper()
	parts, ok := doc["parts"].([]any)
	if !ok {
		t.Fatalf("parts has type %T", doc["parts"])
	}
	for _, raw := range parts {
		part, ok := raw.(map[string]any)
		if ok && part["type"] == typ {
			return part
		}
	}
	t.Fatalf("missing part type %q in %v", typ, parts)
	return nil
}
