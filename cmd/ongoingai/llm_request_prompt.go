package main

import (
	"encoding/json"
	"strings"
)

func extractLLMRequestPrompt(body []byte) (string, bool) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return "", false
	}

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
