package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

const llmResponseContentSchemaVersion = "llm_response_content.v1"

type llmResponseContentDocument struct {
	SchemaVersion string           `json:"schema_version"`
	Parts         []map[string]any `json:"parts"`
}

type llmResponseContentBuilder struct {
	parts []*llmResponseContentPart
	byKey map[string]*llmResponseContentPart
}

type llmResponseContentPart struct {
	typ       string
	text      string
	arguments string
	input     any
	id        string
	name      string
	source    string
}

type sseEvent struct {
	event string
	data  string
}

func extractLLMResponseContentJSON(body []byte, streaming bool) (string, bool) {
	builder := newLLMResponseContentBuilder()
	if streaming || looksLikeSSE(body) {
		for _, event := range llmResponseSSEEvents(body) {
			var payload map[string]any
			if err := json.Unmarshal([]byte(event.data), &payload); err != nil {
				continue
			}
			builder.addStreamingPayload(event.event, payload)
		}
	}

	if !builder.hasParts() {
		builder.addWebSocketCapturedMessages(body)
	}

	if !builder.hasParts() {
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err == nil {
			builder.addJSONPayload(payload)
		}
	}

	return builder.marshal()
}

func newLLMResponseContentBuilder() *llmResponseContentBuilder {
	return &llmResponseContentBuilder{
		byKey: make(map[string]*llmResponseContentPart),
	}
}

func (b *llmResponseContentBuilder) addWebSocketCapturedMessages(body []byte) {
	var messages []map[string]any
	if err := json.Unmarshal(body, &messages); err != nil {
		return
	}
	for _, message := range messages {
		if strings.TrimSpace(stringValue(message["direction"])) != "upstream_to_client" {
			continue
		}
		if strings.TrimSpace(stringValue(message["opcode"])) != "text" {
			continue
		}
		payload, ok := webSocketCapturedPayloadMap(message["payload"])
		if !ok {
			continue
		}
		eventType := stringValue(payload["type"])
		if eventType == "" {
			eventType = stringValue(payload["method"])
		}
		b.addStreamingPayload(eventType, payload)
	}
}

func webSocketCapturedPayloadMap(value any) (map[string]any, bool) {
	if payload := asMap(value); payload != nil {
		return payload, true
	}
	text := stringValue(value)
	if strings.TrimSpace(text) == "" {
		return nil, false
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func (b *llmResponseContentBuilder) addJSONPayload(payload map[string]any) {
	if payload == nil {
		return
	}
	if response := asMap(payload["response"]); response != nil {
		b.addOpenAIResponsesObject(response)
	}
	b.addOpenAIResponsesObject(payload)
	b.addOpenAIChatPayload(payload)
	b.addAnthropicPayload(payload)
}

func (b *llmResponseContentBuilder) addStreamingPayload(eventType string, payload map[string]any) {
	if payload == nil {
		return
	}
	if eventType == "" {
		eventType, _ = payload["type"].(string)
	}
	eventType = strings.TrimSpace(eventType)

	b.addOpenAIResponsesEvent(eventType, payload)
	b.addOpenAIChatPayload(payload)
	b.addAnthropicEvent(payload)
}

func (b *llmResponseContentBuilder) addOpenAIChatPayload(payload map[string]any) {
	choices := asSlice(payload["choices"])
	for choiceIndex, rawChoice := range choices {
		choice := asMap(rawChoice)
		if choice == nil {
			continue
		}
		index := intValue(choice["index"], choiceIndex)
		if message := asMap(choice["message"]); message != nil {
			b.addOpenAIChatMessage(message, index, "openai_chat")
		}
		if delta := asMap(choice["delta"]); delta != nil {
			b.addOpenAIChatMessage(delta, index, "openai_chat")
		}
	}
}

func (b *llmResponseContentBuilder) addOpenAIChatMessage(message map[string]any, choiceIndex int, source string) {
	if message == nil {
		return
	}
	keyPrefix := fmt.Sprintf("%s:choice:%d", source, choiceIndex)
	b.addContentValue(keyPrefix+":text", "text", source, message["content"])
	b.appendString(keyPrefix+":refusal", "refusal", source, stringValue(message["refusal"]))
	b.appendString(keyPrefix+":thinking", "thinking", source, stringValue(message["reasoning_content"]))
	b.appendString(keyPrefix+":thinking", "thinking", source, stringValue(message["thinking"]))
	b.appendString(keyPrefix+":reasoning", "reasoning", source, stringValue(message["reasoning"]))

	if audio := asMap(message["audio"]); audio != nil {
		b.appendString(keyPrefix+":audio_transcript", "audio_transcript", source, stringValue(audio["transcript"]))
	}
	for i, rawToolCall := range asSlice(message["tool_calls"]) {
		toolCall := asMap(rawToolCall)
		if toolCall == nil {
			continue
		}
		index := intValue(toolCall["index"], i)
		key := fmt.Sprintf("%s:tool:%d", keyPrefix, index)
		part := b.part(key, "tool_call", source)
		part.setID(stringValue(toolCall["id"]))
		part.setName(stringValue(toolCall["name"]))
		if fn := asMap(toolCall["function"]); fn != nil {
			part.setName(stringValue(fn["name"]))
			part.appendArguments(stringValue(fn["arguments"]))
		}
		part.appendArguments(stringValue(toolCall["arguments"]))
	}
	if fn := asMap(message["function_call"]); fn != nil {
		part := b.part(keyPrefix+":function_call", "tool_call", source)
		part.setName(stringValue(fn["name"]))
		part.appendArguments(stringValue(fn["arguments"]))
	}
}

func (b *llmResponseContentBuilder) addOpenAIResponsesObject(payload map[string]any) {
	for i, rawItem := range asSlice(payload["output"]) {
		item := asMap(rawItem)
		if item != nil {
			b.addOpenAIResponsesOutputItem(item, i)
		}
	}
	b.appendString("openai_response:output_text", "text", "openai_response", stringValue(payload["output_text"]))
}

func (b *llmResponseContentBuilder) addOpenAIResponsesEvent(eventType string, payload map[string]any) {
	if eventType == "" {
		return
	}
	key := responseEventKey(eventType, payload)
	delta := stringValue(payload["delta"])
	text := stringValue(payload["text"])
	arguments := stringValue(payload["arguments"])
	input := stringValue(payload["input"])

	switch {
	case eventType == "response.completed" || eventType == "response.done":
		if !b.hasParts() {
			if response := asMap(payload["response"]); response != nil {
				b.addOpenAIResponsesObject(response)
			}
		}
	case eventType == "response.output_item.done":
		if item := asMap(payload["item"]); item != nil {
			b.addOpenAIResponsesOutputItem(item, intValue(payload["output_index"], 0))
		}
	case eventType == "response.content_part.done":
		if part := asMap(payload["part"]); part != nil {
			itemID := firstNonEmptyString(
				stringValue(payload["item_id"]),
				stringValue(payload["output_item_id"]),
				fmt.Sprintf("output:%d", intValue(payload["output_index"], 0)),
			)
			b.addOpenAIResponseContentPart(responseContentPartKey(itemID, intValue(payload["content_index"], 0), part), part, "openai_response")
		}
	case strings.Contains(eventType, "output_text"):
		if delta != "" {
			b.appendString(key, "text", "openai_response", delta)
		}
		b.setLongerString(key, "text", "openai_response", text)
	case strings.Contains(eventType, "refusal"):
		if delta != "" {
			b.appendString(key, "refusal", "openai_response", delta)
		}
		b.setLongerString(key, "refusal", "openai_response", stringValue(payload["refusal"]))
	case strings.Contains(eventType, "reasoning") || strings.Contains(eventType, "thinking"):
		if delta != "" {
			b.appendString(key, "thinking", "openai_response", delta)
		}
		b.setLongerString(key, "thinking", "openai_response", text)
	case strings.Contains(eventType, "function_call_arguments") || strings.Contains(eventType, "tool_call_arguments"):
		part := b.part(key, "tool_call", "openai_response")
		part.appendArguments(delta)
		part.setArgumentsIfLonger(arguments)
	case strings.Contains(eventType, "mcp_call_arguments"):
		part := b.part(key, "tool_call", "openai_response")
		part.setName("mcp_call")
		part.appendArguments(delta)
		part.setArgumentsIfLonger(arguments)
	case strings.Contains(eventType, "custom_tool_call_input"):
		part := b.part(key, "tool_call", "openai_response")
		part.setName("custom_tool_call")
		part.appendArguments(delta)
		part.setArgumentsIfLonger(input)
	case strings.Contains(eventType, "transcript"):
		if delta != "" {
			b.appendString(key, "audio_transcript", "openai_response", delta)
		}
		b.setLongerString(key, "audio_transcript", "openai_response", text)
	}
}

func responseEventKey(eventType string, payload map[string]any) string {
	itemID := firstNonEmptyString(
		stringValue(payload["item_id"]),
		stringValue(payload["output_item_id"]),
		stringValue(payload["call_id"]),
	)
	if itemID == "" {
		itemID = fmt.Sprintf("output:%d", intValue(payload["output_index"], 0))
	}
	contentIndex := intValue(payload["content_index"], 0)
	return fmt.Sprintf("openai_response:%s:%d:%s", itemID, contentIndex, responseEventCategory(eventType))
}

func responseEventCategory(eventType string) string {
	switch {
	case strings.Contains(eventType, "output_text"):
		return "output_text"
	case strings.Contains(eventType, "refusal"):
		return "refusal"
	case strings.Contains(eventType, "reasoning") || strings.Contains(eventType, "thinking"):
		return "thinking"
	case strings.Contains(eventType, "function_call_arguments") || strings.Contains(eventType, "tool_call_arguments"):
		return "tool_call_arguments"
	case strings.Contains(eventType, "mcp_call_arguments"):
		return "mcp_call_arguments"
	case strings.Contains(eventType, "custom_tool_call_input"):
		return "custom_tool_call_input"
	case strings.Contains(eventType, "transcript"):
		return "audio_transcript"
	default:
		return eventType
	}
}

func (b *llmResponseContentBuilder) addOpenAIResponsesOutputItem(item map[string]any, itemIndex int) {
	if item == nil {
		return
	}
	itemType := strings.TrimSpace(stringValue(item["type"]))
	itemID := firstNonEmptyString(stringValue(item["id"]), stringValue(item["call_id"]), fmt.Sprintf("item:%d", itemIndex))
	key := "openai_response:" + itemID

	if content := asSlice(item["content"]); content != nil {
		for i, rawPart := range content {
			part := asMap(rawPart)
			if part != nil {
				b.addOpenAIResponseContentPart(responseContentPartKey(itemID, i, part), part, "openai_response")
			}
		}
	}
	if itemType == "reasoning" || strings.Contains(itemType, "reasoning") {
		b.addOpenAIResponseReasoningItem(key, item)
		return
	}
	if itemType == "function_call" || strings.Contains(itemType, "tool_call") || strings.Contains(itemType, "mcp_call") {
		part := b.part(key+":tool", "tool_call", "openai_response")
		part.setID(itemID)
		part.setName(firstNonEmptyString(stringValue(item["name"]), itemType))
		part.appendArguments(stringValue(item["arguments"]))
		part.setInput(item["input"])
		return
	}
	if itemType == "message" {
		return
	}
	b.appendString(key+":text", "text", "openai_response", stringValue(item["text"]))
}

func (b *llmResponseContentBuilder) addOpenAIResponseReasoningItem(key string, item map[string]any) {
	for i, rawSummary := range asSlice(item["summary"]) {
		summary := asMap(rawSummary)
		if summary == nil {
			continue
		}
		b.appendString(fmt.Sprintf("%s:summary:%d", key, i), "thinking", "openai_response", stringValue(summary["text"]))
	}
	b.appendString(key+":text", "thinking", "openai_response", stringValue(item["text"]))
	b.appendString(key+":summary", "thinking", "openai_response", stringValue(item["summary_text"]))
}

func (b *llmResponseContentBuilder) addOpenAIResponseContentParts(keyPrefix string, parts []any, source string) {
	for i, rawPart := range parts {
		part := asMap(rawPart)
		if part == nil {
			continue
		}
		b.addOpenAIResponseContentPart(fmt.Sprintf("%s:%d", keyPrefix, i), part, source)
	}
}

func (b *llmResponseContentBuilder) addOpenAIResponseContentPart(key string, part map[string]any, source string) {
	partType := strings.TrimSpace(stringValue(part["type"]))
	switch {
	case partType == "output_text" || partType == "text":
		b.appendString(key, "text", source, stringValue(part["text"]))
	case partType == "refusal":
		b.appendString(key, "refusal", source, firstNonEmptyString(stringValue(part["refusal"]), stringValue(part["text"])))
	case strings.Contains(partType, "audio"):
		b.appendString(key, "audio_transcript", source, firstNonEmptyString(stringValue(part["transcript"]), stringValue(part["text"])))
	case strings.Contains(partType, "reasoning") || strings.Contains(partType, "thinking"):
		b.appendString(key, "thinking", source, firstNonEmptyString(stringValue(part["text"]), stringValue(part["thinking"])))
	}
}

func responseContentPartKey(itemID string, index int, part map[string]any) string {
	category := "content"
	partType := strings.TrimSpace(stringValue(part["type"]))
	switch {
	case partType == "output_text" || partType == "text":
		category = "output_text"
	case partType == "refusal":
		category = "refusal"
	case strings.Contains(partType, "audio"):
		category = "audio_transcript"
	case strings.Contains(partType, "reasoning") || strings.Contains(partType, "thinking"):
		category = "thinking"
	}
	return fmt.Sprintf("openai_response:%s:%d:%s", itemID, index, category)
}

func (b *llmResponseContentBuilder) addAnthropicPayload(payload map[string]any) {
	for i, rawBlock := range asSlice(payload["content"]) {
		block := asMap(rawBlock)
		if block != nil {
			b.addAnthropicContentBlock(block, i, "anthropic")
		}
	}
}

func (b *llmResponseContentBuilder) addAnthropicEvent(payload map[string]any) {
	eventType := stringValue(payload["type"])
	index := intValue(payload["index"], 0)
	switch eventType {
	case "content_block_start":
		if block := asMap(payload["content_block"]); block != nil {
			b.addAnthropicContentBlock(block, index, "anthropic")
		}
	case "content_block_delta":
		delta := asMap(payload["delta"])
		if delta == nil {
			return
		}
		switch stringValue(delta["type"]) {
		case "text_delta":
			b.appendString(fmt.Sprintf("anthropic:%d:text", index), "text", "anthropic", stringValue(delta["text"]))
		case "input_json_delta":
			b.part(fmt.Sprintf("anthropic:%d:tool", index), "tool_call", "anthropic").appendArguments(stringValue(delta["partial_json"]))
		case "thinking_delta":
			b.appendString(fmt.Sprintf("anthropic:%d:thinking", index), "thinking", "anthropic", stringValue(delta["thinking"]))
		}
	}
}

func (b *llmResponseContentBuilder) addAnthropicContentBlock(block map[string]any, index int, source string) {
	blockType := strings.TrimSpace(stringValue(block["type"]))
	switch blockType {
	case "text":
		b.appendString(fmt.Sprintf("anthropic:%d:text", index), "text", source, stringValue(block["text"]))
	case "thinking", "redacted_thinking":
		b.appendString(fmt.Sprintf("anthropic:%d:thinking", index), "thinking", source, firstNonEmptyString(stringValue(block["thinking"]), stringValue(block["text"])))
	case "tool_use", "server_tool_use":
		part := b.part(fmt.Sprintf("anthropic:%d:tool", index), "tool_call", source)
		part.setID(stringValue(block["id"]))
		part.setName(firstNonEmptyString(stringValue(block["name"]), blockType))
		part.setInput(block["input"])
	}
}

func (b *llmResponseContentBuilder) addContentValue(key, typ, source string, value any) {
	switch typed := value.(type) {
	case string:
		b.appendString(key, typ, source, typed)
	case []any:
		for i, raw := range typed {
			part := asMap(raw)
			if part == nil {
				continue
			}
			b.addOpenAIResponseContentPart(fmt.Sprintf("%s:%d", key, i), part, source)
		}
	}
}

func (b *llmResponseContentBuilder) part(key, typ, source string) *llmResponseContentPart {
	key = strings.TrimSpace(key)
	if key == "" {
		key = fmt.Sprintf("%s:%d", typ, len(b.parts))
	}
	if existing := b.byKey[key]; existing != nil {
		if existing.typ == "" {
			existing.typ = typ
		}
		return existing
	}
	part := &llmResponseContentPart{typ: typ, source: source}
	b.parts = append(b.parts, part)
	b.byKey[key] = part
	return part
}

func (b *llmResponseContentBuilder) appendString(key, typ, source, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	b.part(key, typ, source).appendText(value)
}

func (b *llmResponseContentBuilder) setLongerString(key, typ, source, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	b.part(key, typ, source).setTextIfLonger(value)
}

func (b *llmResponseContentBuilder) hasParts() bool {
	for _, part := range b.parts {
		if part.hasSemanticContent() {
			return true
		}
	}
	return false
}

func (b *llmResponseContentBuilder) marshal() (string, bool) {
	parts := make([]map[string]any, 0, len(b.parts))
	for _, part := range b.parts {
		if encoded := part.toMap(); encoded != nil {
			parts = append(parts, encoded)
		}
	}
	if len(parts) == 0 {
		return "", false
	}
	data, err := json.Marshal(llmResponseContentDocument{
		SchemaVersion: llmResponseContentSchemaVersion,
		Parts:         parts,
	})
	if err != nil {
		return "", false
	}
	return string(data), true
}

func (p *llmResponseContentPart) appendText(value string) {
	if p == nil {
		return
	}
	switch p.typ {
	case "tool_call":
		p.appendArguments(value)
	default:
		p.text += value
	}
}

func (p *llmResponseContentPart) setTextIfLonger(value string) {
	if p == nil || len(value) <= len(p.text) {
		return
	}
	p.text = value
}

func (p *llmResponseContentPart) appendArguments(value string) {
	if p == nil || value == "" {
		return
	}
	p.arguments += value
}

func (p *llmResponseContentPart) setArgumentsIfLonger(value string) {
	if p == nil || len(value) <= len(p.arguments) {
		return
	}
	p.arguments = value
}

func (p *llmResponseContentPart) setID(value string) {
	if p != nil && strings.TrimSpace(value) != "" {
		p.id = strings.TrimSpace(value)
	}
}

func (p *llmResponseContentPart) setName(value string) {
	if p != nil && strings.TrimSpace(value) != "" {
		p.name = strings.TrimSpace(value)
	}
}

func (p *llmResponseContentPart) setInput(value any) {
	if p == nil || value == nil {
		return
	}
	p.input = value
}

func (p *llmResponseContentPart) hasSemanticContent() bool {
	if p == nil {
		return false
	}
	return strings.TrimSpace(p.text) != "" ||
		strings.TrimSpace(p.arguments) != "" ||
		strings.TrimSpace(p.name) != "" ||
		p.input != nil
}

func (p *llmResponseContentPart) toMap() map[string]any {
	if !p.hasSemanticContent() {
		return nil
	}
	out := map[string]any{
		"type": p.typ,
	}
	if p.source != "" {
		out["source"] = p.source
	}
	if p.id != "" {
		out["id"] = p.id
	}
	if p.name != "" {
		out["name"] = p.name
	}
	if p.text != "" {
		out["text"] = p.text
	}
	if p.arguments != "" {
		out["arguments"] = p.arguments
	}
	if p.input != nil {
		out["input"] = p.input
	}
	return out
}

func llmResponseSSEEvents(body []byte) []sseEvent {
	blocks := strings.Split(strings.ReplaceAll(string(body), "\r\n", "\n"), "\n\n")
	events := make([]sseEvent, 0, len(blocks))
	for _, block := range blocks {
		lines := strings.Split(block, "\n")
		event := sseEvent{}
		dataLines := make([]string, 0, len(lines))
		for _, line := range lines {
			line = strings.TrimRight(line, "\r")
			switch {
			case strings.HasPrefix(line, "event:"):
				event.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			case strings.HasPrefix(line, "data:"):
				data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if data != "" {
					dataLines = append(dataLines, data)
				}
			}
		}
		if len(dataLines) == 0 {
			continue
		}
		event.data = strings.Join(dataLines, "\n")
		if strings.TrimSpace(event.data) == "[DONE]" {
			continue
		}
		events = append(events, event)
	}
	return events
}

func looksLikeSSE(body []byte) bool {
	value := strings.TrimSpace(string(body))
	return strings.HasPrefix(value, "data:") || strings.Contains(value, "\ndata:")
}

func asMap(value any) map[string]any {
	typed, _ := value.(map[string]any)
	return typed
}

func asSlice(value any) []any {
	typed, _ := value.([]any)
	return typed
}

func stringValue(value any) string {
	typed, _ := value.(string)
	return typed
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	default:
		return fallback
	}
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
