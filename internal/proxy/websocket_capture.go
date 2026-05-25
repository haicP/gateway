package proxy

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type webSocketCaptureConfig struct {
	maxBodySize     int
	sink            BodyCaptureSink
	base            CapturedExchange
	responseHeaders func() http.Header
}

type webSocketTurnRecorder struct {
	mu           sync.Mutex
	config       webSocketCaptureConfig
	connectionID string
	startedAt    time.Time
	turnIndex    int
	current      *webSocketTurn
	clientParser websocketFrameParser
	serverParser websocketFrameParser
	closeCode    int
	closed       bool
	eventsMu     sync.Mutex
	events       chan websocketCaptureEvent
	eventsDone   chan struct{}
	eventsClosed bool
}

type webSocketTurn struct {
	index            int
	startType        string
	terminalType     string
	startedAt        time.Time
	firstResponseUS  int64
	requestMessages  websocketMessageBuffer
	responseMessages websocketMessageBuffer
	incomplete       bool
}

type websocketMessageBuffer struct {
	maxPayloadBytes int
	messages        []websocketCapturedMessage
	payloadBytes    int64
	capturedBytes   int
	truncated       bool
	count           int
}

type websocketCapturedMessage struct {
	Direction     string `json:"direction"`
	Opcode        string `json:"opcode"`
	Payload       any    `json:"payload,omitempty"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
	Bytes         int    `json:"bytes"`
	Truncated     bool   `json:"truncated,omitempty"`
}

type websocketCaptureEvent struct {
	direction string
	data      []byte
}

type websocketObservedConn struct {
	net.Conn
	recorder *webSocketTurnRecorder
}

type websocketFrameParser struct {
	buffer      []byte
	failed      bool
	fragOpcode  byte
	fragPayload []byte
}

type websocketFrame struct {
	opcode  byte
	fin     bool
	payload []byte
}

const (
	websocketOpcodeContinuation = 0x0
	websocketOpcodeText         = 0x1
	websocketOpcodeBinary       = 0x2
	websocketOpcodeClose        = 0x8

	websocketCaptureQueueSize = 256
)

// IsWebSocketUpgrade reports whether a request is attempting a WebSocket
// upgrade. It intentionally does not validate the full handshake.
func IsWebSocketUpgrade(r *http.Request) bool {
	if r == nil {
		return false
	}
	return headerContainsToken(r.Header, "Connection", "upgrade") &&
		strings.EqualFold(strings.TrimSpace(r.Header.Get("Upgrade")), "websocket")
}

func headerContainsToken(headers http.Header, key, token string) bool {
	for _, value := range headers.Values(key) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func (w *captureResponseWriter) EnableWebSocketCapture(config webSocketCaptureConfig) {
	if w == nil || config.sink == nil {
		return
	}
	if config.maxBodySize < 0 {
		config.maxBodySize = 0
	}
	w.wsRecorder = newWebSocketTurnRecorder(config)
}

func newWebSocketTurnRecorder(config webSocketCaptureConfig) *webSocketTurnRecorder {
	recorder := &webSocketTurnRecorder{
		config:       config,
		connectionID: newWebSocketConnectionID(),
		startedAt:    config.base.StartedAt,
		events:       make(chan websocketCaptureEvent, websocketCaptureQueueSize),
		eventsDone:   make(chan struct{}),
	}
	go recorder.runCaptureEvents()
	return recorder
}

func (w *captureResponseWriter) WebSocketCaptured() bool {
	return w != nil && w.wsCaptured
}

func (w *captureResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	conn, brw, err := hijacker.Hijack()
	if err != nil {
		return nil, brw, err
	}
	if w.wsRecorder == nil {
		return conn, brw, nil
	}
	w.wsCaptured = true
	w.statusCode = http.StatusSwitchingProtocols
	return &websocketObservedConn{Conn: conn, recorder: w.wsRecorder}, brw, nil
}

func (c *websocketObservedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.recorder != nil {
		c.recorder.EnqueueClientBytes(p[:n])
	}
	return n, err
}

func (c *websocketObservedConn) Write(p []byte) (int, error) {
	if len(p) > 0 && c.recorder != nil {
		c.recorder.EnqueueServerBytes(p)
	}
	n, err := c.Conn.Write(p)
	return n, err
}

func (c *websocketObservedConn) Close() error {
	if c.recorder != nil {
		c.recorder.Close()
	}
	return c.Conn.Close()
}

func (r *webSocketTurnRecorder) EnqueueClientBytes(data []byte) {
	r.enqueueBytes("client_to_upstream", data)
}

func (r *webSocketTurnRecorder) EnqueueServerBytes(data []byte) {
	r.enqueueBytes("upstream_to_client", data)
}

func (r *webSocketTurnRecorder) enqueueBytes(direction string, data []byte) {
	if r == nil || len(data) == 0 {
		return
	}
	copied := append([]byte(nil), data...)
	if r.events == nil {
		r.observeBytes(direction, copied)
		return
	}

	r.eventsMu.Lock()
	defer r.eventsMu.Unlock()
	if r.eventsClosed {
		return
	}
	r.events <- websocketCaptureEvent{direction: direction, data: copied}
}

func (r *webSocketTurnRecorder) runCaptureEvents() {
	defer close(r.eventsDone)
	for event := range r.events {
		r.observeBytes(event.direction, event.data)
	}
}

func (r *webSocketTurnRecorder) ObserveClientBytes(data []byte) {
	if r == nil || len(data) == 0 {
		return
	}
	r.observeBytes("client_to_upstream", data)
}

func (r *webSocketTurnRecorder) ObserveServerBytes(data []byte) {
	if r == nil || len(data) == 0 {
		return
	}
	r.observeBytes("upstream_to_client", data)
}

func (r *webSocketTurnRecorder) observeBytes(direction string, data []byte) {
	var frames []websocketFrame
	switch direction {
	case "client_to_upstream":
		frames = r.clientParser.Add(data)
	case "upstream_to_client":
		frames = r.serverParser.Add(data)
	default:
		return
	}
	for _, frame := range frames {
		r.observeFrame(direction, frame)
	}
}

func (r *webSocketTurnRecorder) Close() {
	if r == nil {
		return
	}
	if r.events != nil {
		r.eventsMu.Lock()
		if !r.eventsClosed {
			r.eventsClosed = true
			close(r.events)
		}
		done := r.eventsDone
		r.eventsMu.Unlock()
		if done != nil {
			<-done
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	r.closed = true
	if r.current != nil {
		r.current.incomplete = true
		r.emitCurrentLocked("connection_closed")
	}
}

func (r *webSocketTurnRecorder) observeFrame(direction string, frame websocketFrame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}

	switch frame.opcode {
	case websocketOpcodeClose:
		r.closeCode = websocketCloseCode(frame.payload)
		if r.current != nil {
			r.current.incomplete = true
			r.emitCurrentLocked("websocket_close")
		}
	case websocketOpcodeText:
		r.observeTextLocked(direction, frame.payload)
	case websocketOpcodeBinary:
		r.observeBinaryLocked(direction, frame.payload)
	}
}

func (r *webSocketTurnRecorder) observeTextLocked(direction string, payload []byte) {
	startType, terminalType := classifyWebSocketText(payload)
	if direction == "client_to_upstream" {
		switch startType {
		case "conversation.item.create":
			if r.current == nil {
				r.startTurnLocked(startType)
			}
		case "response.create":
			if r.current == nil {
				r.startTurnLocked(startType)
			} else if r.current.startType != "conversation.item.create" {
				r.current.incomplete = true
				r.emitCurrentLocked("next_turn_start")
				r.startTurnLocked(startType)
			}
		case "turn/start":
			if r.current != nil {
				r.current.incomplete = true
				r.emitCurrentLocked("next_turn_start")
			}
			r.startTurnLocked(startType)
		case "turn/steer":
			if r.current == nil {
				r.startTurnLocked(startType)
			}
		}
		if r.current != nil {
			r.current.requestMessages.Add(direction, "text", payload)
		}
		return
	}

	if r.current == nil {
		return
	}
	if r.current.firstResponseUS == 0 {
		r.current.firstResponseUS = time.Since(r.current.startedAt).Microseconds()
	}
	r.current.responseMessages.Add(direction, "text", payload)
	if terminalType != "" {
		r.emitCurrentLocked(terminalType)
	}
}

func (r *webSocketTurnRecorder) observeBinaryLocked(direction string, payload []byte) {
	if r.current == nil {
		return
	}
	if direction == "client_to_upstream" {
		r.current.requestMessages.Add(direction, "binary", payload)
		return
	}
	if r.current.firstResponseUS == 0 {
		r.current.firstResponseUS = time.Since(r.current.startedAt).Microseconds()
	}
	r.current.responseMessages.Add(direction, "binary", payload)
}

func (r *webSocketTurnRecorder) startTurnLocked(startType string) {
	r.turnIndex++
	r.current = &webSocketTurn{
		index:     r.turnIndex,
		startType: startType,
		startedAt: time.Now().UTC(),
		requestMessages: websocketMessageBuffer{
			maxPayloadBytes: r.config.maxBodySize,
		},
		responseMessages: websocketMessageBuffer{
			maxPayloadBytes: r.config.maxBodySize,
		},
	}
}

func (r *webSocketTurnRecorder) emitCurrentLocked(terminalType string) {
	turn := r.current
	if turn == nil {
		return
	}
	r.current = nil
	turn.terminalType = terminalType

	exchange := r.config.base
	exchange.StartedAt = turn.startedAt
	exchange.StatusCode = http.StatusSwitchingProtocols
	if r.config.responseHeaders != nil {
		exchange.ResponseHeaders = r.config.responseHeaders()
	} else {
		exchange.ResponseHeaders = r.config.base.ResponseHeaders.Clone()
	}
	exchange.RequestBody = turn.requestMessages.Bytes()
	exchange.RequestBodyBytes = turn.requestMessages.payloadBytes
	exchange.RequestBodyTruncated = turn.requestMessages.truncated
	exchange.ResponseBody = turn.responseMessages.Bytes()
	exchange.ResponseBodyBytes = turn.responseMessages.payloadBytes
	exchange.ResponseBodyTruncated = turn.responseMessages.truncated
	exchange.StreamChunks = turn.requestMessages.count + turn.responseMessages.count
	exchange.DurationMS = time.Since(turn.startedAt).Milliseconds()
	exchange.TimeToFirstTokenUS = turn.firstResponseUS
	exchange.TimeToFirstTokenMS = microsecondsToRoundedMilliseconds(turn.firstResponseUS)
	exchange.WebSocketConnectionID = r.connectionID
	exchange.WebSocketTurnIndex = turn.index
	exchange.WebSocketTurnStartType = turn.startType
	exchange.WebSocketTurnTerminalType = turn.terminalType
	exchange.WebSocketRequestMessages = turn.requestMessages.count
	exchange.WebSocketResponseMessages = turn.responseMessages.count
	exchange.WebSocketTurnIncomplete = turn.incomplete
	exchange.WebSocketCloseCode = r.closeCode

	r.config.sink(&exchange)
}

func (b *websocketMessageBuffer) Add(direction, opcode string, payload []byte) {
	if b == nil {
		return
	}
	b.count++
	b.payloadBytes += int64(len(payload))

	captured, truncated := b.capturePayload(payload)
	if len(captured) == 0 && len(payload) > 0 && truncated {
		return
	}

	message := websocketCapturedMessage{
		Direction: direction,
		Opcode:    opcode,
		Bytes:     len(payload),
		Truncated: truncated,
	}
	if opcode == "binary" {
		message.PayloadBase64 = base64.StdEncoding.EncodeToString(captured)
	} else {
		message.Payload = decodeTextPayload(captured, truncated)
	}
	b.messages = append(b.messages, message)
}

func (b *websocketMessageBuffer) capturePayload(payload []byte) ([]byte, bool) {
	if b.maxPayloadBytes == 0 {
		copied := append([]byte(nil), payload...)
		b.capturedBytes += len(copied)
		return copied, false
	}
	remaining := b.maxPayloadBytes - b.capturedBytes
	if remaining <= 0 {
		b.truncated = true
		return nil, true
	}
	captured := payload
	truncated := false
	if len(captured) > remaining {
		captured = captured[:remaining]
		truncated = true
		b.truncated = true
	}
	copied := append([]byte(nil), captured...)
	b.capturedBytes += len(copied)
	return copied, truncated
}

func (b *websocketMessageBuffer) Bytes() []byte {
	if b == nil || len(b.messages) == 0 {
		return nil
	}
	data, err := json.Marshal(b.messages)
	if err != nil {
		return nil
	}
	return data
}

func decodeTextPayload(payload []byte, truncated bool) any {
	text := string(payload)
	if !utf8.ValidString(text) {
		text = strings.ToValidUTF8(text, "")
	}
	if truncated {
		return text
	}
	var decoded any
	if err := json.Unmarshal([]byte(text), &decoded); err == nil {
		return decoded
	}
	return text
}

func classifyWebSocketText(payload []byte) (startType string, terminalType string) {
	var message map[string]any
	if err := json.Unmarshal(payload, &message); err != nil {
		return "", ""
	}
	if rawType, ok := message["type"].(string); ok {
		switch strings.TrimSpace(rawType) {
		case "conversation.item.create", "response.create":
			return strings.TrimSpace(rawType), ""
		case "response.done", "response.completed", "response.failed", "error":
			return "", strings.TrimSpace(rawType)
		}
	}
	if method, ok := message["method"].(string); ok {
		switch strings.TrimSpace(method) {
		case "turn/start":
			return "turn/start", ""
		case "turn/steer":
			return "turn/steer", ""
		case "turn/completed", "turn/aborted", "turn/failed":
			return "", strings.TrimSpace(method)
		}
	}
	return "", ""
}

func (p *websocketFrameParser) Add(data []byte) []websocketFrame {
	if p == nil || p.failed || len(data) == 0 {
		return nil
	}
	p.buffer = append(p.buffer, data...)
	frames := make([]websocketFrame, 0)

	for {
		frame, consumed, ok := p.nextFrame()
		if !ok {
			return frames
		}
		p.buffer = p.buffer[consumed:]
		if frame.opcode == websocketOpcodeContinuation {
			if p.fragOpcode == 0 {
				continue
			}
			p.fragPayload = append(p.fragPayload, frame.payload...)
			if frame.fin {
				frames = append(frames, websocketFrame{
					opcode:  p.fragOpcode,
					fin:     true,
					payload: append([]byte(nil), p.fragPayload...),
				})
				p.fragOpcode = 0
				p.fragPayload = nil
			}
			continue
		}
		if frame.opcode == websocketOpcodeText || frame.opcode == websocketOpcodeBinary {
			if frame.fin {
				frames = append(frames, frame)
				continue
			}
			p.fragOpcode = frame.opcode
			p.fragPayload = append(p.fragPayload[:0], frame.payload...)
			continue
		}
		frames = append(frames, frame)
	}
}

func (p *websocketFrameParser) nextFrame() (websocketFrame, int, bool) {
	if len(p.buffer) < 2 {
		return websocketFrame{}, 0, false
	}
	first := p.buffer[0]
	second := p.buffer[1]
	fin := first&0x80 != 0
	masked := second&0x80 != 0
	payloadLen := uint64(second & 0x7f)
	offset := 2
	switch payloadLen {
	case 126:
		if len(p.buffer) < offset+2 {
			return websocketFrame{}, 0, false
		}
		payloadLen = uint64(binary.BigEndian.Uint16(p.buffer[offset : offset+2]))
		offset += 2
	case 127:
		if len(p.buffer) < offset+8 {
			return websocketFrame{}, 0, false
		}
		payloadLen = binary.BigEndian.Uint64(p.buffer[offset : offset+8])
		offset += 8
	}
	if payloadLen > uint64(maxInt()-offset-4) {
		p.failed = true
		p.buffer = nil
		return websocketFrame{}, 0, false
	}
	var maskKey []byte
	if masked {
		if len(p.buffer) < offset+4 {
			return websocketFrame{}, 0, false
		}
		maskKey = p.buffer[offset : offset+4]
		offset += 4
	}
	end := offset + int(payloadLen)
	if len(p.buffer) < end {
		return websocketFrame{}, 0, false
	}
	payload := append([]byte(nil), p.buffer[offset:end]...)
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return websocketFrame{opcode: first & 0x0f, fin: fin, payload: payload}, end, true
}

func websocketCloseCode(payload []byte) int {
	if len(payload) < 2 {
		return 0
	}
	return int(binary.BigEndian.Uint16(payload[:2]))
}

func newWebSocketConnectionID() string {
	return "ws_" + newRandomHex(16)
}

func newRandomHex(size int) string {
	var b = make([]byte, size)
	if _, err := rand.Read(b); err != nil {
		return time.Now().UTC().Format("20060102150405.000000000")
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
