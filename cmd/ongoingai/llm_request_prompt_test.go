package main

import "testing"

func TestExtractLLMRequestPromptOpenAIMessagesUsesLastUser(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"model":"gpt-4o-mini",
		"messages":[
			{"role":"system","content":"ignore"},
			{"role":"user","content":"first"},
			{"role":"assistant","content":"answer"},
			{"role":"user","content":"last"}
		]
	}`)

	got, ok := extractLLMRequestPrompt(body)
	if !ok || got != "last" {
		t.Fatalf("prompt=%q ok=%v, want last user prompt", got, ok)
	}
}

func TestExtractLLMRequestPromptContentArray(t *testing.T) {
	t.Parallel()

	body := []byte(`{
		"messages":[
			{"role":"user","content":[
				{"type":"text","text":"part one"},
				{"type":"image","source":{"type":"base64"}},
				{"type":"text","text":"part two"}
			]}
		]
	}`)

	got, ok := extractLLMRequestPrompt(body)
	if !ok || got != "part one\npart two" {
		t.Fatalf("prompt=%q ok=%v, want joined text parts", got, ok)
	}
}

func TestExtractLLMRequestPromptResponsesInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "string",
			body: `{"input":"direct prompt"}`,
			want: "direct prompt",
		},
		{
			name: "array",
			body: `{"input":[
				{"role":"user","content":"first"},
				{"role":"assistant","content":"answer"},
				{"role":"user","content":[{"type":"input_text","text":"last input"}]}
			]}`,
			want: "last input",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := extractLLMRequestPrompt([]byte(tt.body))
			if !ok || got != tt.want {
				t.Fatalf("prompt=%q ok=%v, want %q", got, ok, tt.want)
			}
		})
	}
}

func TestExtractLLMRequestPromptWebSocketCapturedResponsesInput(t *testing.T) {
	t.Parallel()

	body := []byte(`[
		{"direction":"upstream_to_client","opcode":"text","payload":{"type":"response.output_text.delta","delta":"ignore"}},
		{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.create","input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"first"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"answer"}]},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"last"}]}
		]}}
	]`)

	got, ok := extractLLMRequestPrompt(body)
	if !ok || got != "last" {
		t.Fatalf("prompt=%q ok=%v, want last websocket user prompt", got, ok)
	}
}

func TestExtractLLMRequestPromptWebSocketCapturedRealtimeConversationItem(t *testing.T) {
	t.Parallel()

	body := []byte(`[
		{"direction":"client_to_upstream","opcode":"text","payload":{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello realtime"}]}}},
		{"direction":"client_to_upstream","opcode":"text","payload":{"type":"response.create","response":{"modalities":["text"]}}}
	]`)

	got, ok := extractLLMRequestPrompt(body)
	if !ok || got != "hello realtime" {
		t.Fatalf("prompt=%q ok=%v, want realtime user prompt", got, ok)
	}
}

func TestExtractLLMRequestPromptWebSocketCapturedCodexTurnInput(t *testing.T) {
	t.Parallel()

	body := []byte(`[
		{"direction":"client_to_upstream","opcode":"text","payload":{"method":"turn/start","params":{"input":[{"type":"text","text":"first"}]}}},
		{"direction":"client_to_upstream","opcode":"text","payload":{"method":"turn/steer","params":{"input":[{"type":"text","text":"last steer"}]}}}
	]`)

	got, ok := extractLLMRequestPrompt(body)
	if !ok || got != "last steer" {
		t.Fatalf("prompt=%q ok=%v, want codex turn input", got, ok)
	}
}

func TestExtractLLMRequestPromptRejectsMissingOrMalformedText(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{oops}`,
		`{"messages":[{"role":"assistant","content":"answer"}]}`,
		`{"messages":[{"role":"user","content":[{"type":"image","source":{}}]}]}`,
	}
	for _, body := range tests {
		if got, ok := extractLLMRequestPrompt([]byte(body)); ok || got != "" {
			t.Fatalf("prompt=%q ok=%v, want empty for %s", got, ok, body)
		}
	}
}
