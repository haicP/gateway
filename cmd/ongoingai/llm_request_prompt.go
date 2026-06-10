package main

import (
	"encoding/json"
	"strings"
)

func extractLLMRequestPrompt(body []byte) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err == nil {
		return extractLLMRequestPromptPayload(payload)
	}

	var messages []map[string]any
	if err := json.Unmarshal(body, &messages); err == nil {
		return extractLLMRequestPromptFromWebSocketCapturedMessages(messages)
	}

	return "", false
}

func extractLLMRequestPromptPayload(payload map[string]any) (string, bool) {
	if prompt, ok := extractLastUserPromptFromMessages(asSlice(payload["messages"])); ok {
		return prompt, true
	}

	switch input := payload["input"].(type) {
	case string:
		return nonEmptyPrompt(input)
	case []any:
		return extractLastUserPromptFromMessages(input)
	default:
		return "", false
	}
}

func extractLLMRequestPromptFromWebSocketCapturedMessages(messages []map[string]any) (string, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		message := messages[i]
		if strings.TrimSpace(stringValue(message["direction"])) != "client_to_upstream" {
			continue
		}
		if strings.TrimSpace(stringValue(message["opcode"])) != "text" {
			continue
		}
		payload, ok := webSocketCapturedPayloadMap(message["payload"])
		if !ok {
			continue
		}
		if prompt, ok := extractLLMRequestPromptPayload(payload); ok {
			return prompt, true
		}
		if prompt, ok := extractLLMRequestPromptFromWebSocketPayload(payload); ok {
			return prompt, true
		}
	}
	return "", false
}

func extractLLMRequestPromptFromWebSocketPayload(payload map[string]any) (string, bool) {
	if payload == nil {
		return "", false
	}
	if item := asMap(payload["item"]); strings.TrimSpace(stringValue(item["role"])) == "user" {
		if prompt, ok := extractPromptText(item); ok {
			return prompt, true
		}
	}
	if response := asMap(payload["response"]); response != nil {
		if prompt, ok := extractLLMRequestPromptPayload(response); ok {
			return prompt, true
		}
	}

	method := strings.TrimSpace(stringValue(payload["method"]))
	if method != "turn/start" && method != "turn/steer" {
		return "", false
	}
	params := asMap(payload["params"])
	if params == nil {
		return "", false
	}
	return extractTextValue(params["input"])
}

func extractLastUserPromptFromMessages(messages []any) (string, bool) {
	for i := len(messages) - 1; i >= 0; i-- {
		message := asMap(messages[i])
		if message == nil || strings.TrimSpace(stringValue(message["role"])) != "user" {
			continue
		}
		if prompt, ok := extractPromptText(message); ok {
			return prompt, true
		}
	}
	return "", false
}

func extractPromptText(message map[string]any) (string, bool) {
	if message == nil {
		return "", false
	}
	if prompt, ok := extractTextValue(message["content"]); ok {
		return prompt, true
	}
	return extractTextValue(message["text"])
}

func extractTextValue(value any) (string, bool) {
	switch typed := value.(type) {
	case string:
		return nonEmptyPrompt(typed)
	case []any:
		parts := make([]string, 0, len(typed))
		for _, rawPart := range typed {
			switch part := rawPart.(type) {
			case string:
				if text := strings.TrimSpace(part); text != "" {
					parts = append(parts, text)
				}
			case map[string]any:
				partType := strings.TrimSpace(stringValue(part["type"]))
				if partType != "" && partType != "text" && partType != "input_text" {
					continue
				}
				if text := strings.TrimSpace(stringValue(part["text"])); text != "" {
					parts = append(parts, text)
				}
			}
		}
		return nonEmptyPrompt(strings.Join(parts, "\n"))
	default:
		return "", false
	}
}

func nonEmptyPrompt(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	return value, true
}
