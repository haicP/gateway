package proxy

import (
	"bufio"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestWebSocketProxyPassThroughAndCapturesResponsesTurns(t *testing.T) {
	t.Parallel()

	var upstreamMu sync.Mutex
	var gotPath string
	var gotQuery string
	var gotAuth string
	var gotClientMessages []string

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamMu.Lock()
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		upstreamMu.Unlock()

		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack upstream: %v", err)
			return
		}
		defer conn.Close()

		accept := websocketAccept(r.Header.Get("Sec-WebSocket-Key"))
		fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\n")
		fmt.Fprintf(brw, "Upgrade: websocket\r\n")
		fmt.Fprintf(brw, "Connection: Upgrade\r\n")
		fmt.Fprintf(brw, "Sec-WebSocket-Accept: %s\r\n\r\n", accept)
		if err := brw.Flush(); err != nil {
			t.Errorf("flush handshake: %v", err)
			return
		}

		for i := 0; i < 2; i++ {
			opcode, payload, err := readWebSocketFrame(brw.Reader)
			if err != nil {
				t.Errorf("read upstream frame: %v", err)
				return
			}
			if opcode != websocketOpcodeText {
				t.Errorf("upstream opcode=%d, want text", opcode)
				return
			}
			upstreamMu.Lock()
			gotClientMessages = append(gotClientMessages, string(payload))
			upstreamMu.Unlock()

			response := fmt.Sprintf(`{"type":"response.completed","response":{"usage":{"input_tokens":%d,"output_tokens":%d,"total_tokens":%d}}}`, 10+i, 5+i, 15+2*i)
			if _, err := conn.Write(makeWebSocketFrame(false, websocketOpcodeText, []byte(response))); err != nil {
				t.Errorf("write upstream frame: %v", err)
				return
			}
		}
	}))
	defer upstream.Close()

	var capturedMu sync.Mutex
	var captured []*CapturedExchange
	proxyHandler, err := NewHandler([]Route{{Prefix: "/llm", Upstream: upstream.URL}}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	handler := BodyCaptureMiddleware(BodyCaptureOptions{
		Enabled:      true,
		MaxBodySize:  4096,
		ParseMaxSize: 4096,
	}, func(exchange *CapturedExchange) {
		copied := *exchange
		capturedMu.Lock()
		captured = append(captured, &copied)
		capturedMu.Unlock()
	}, proxyHandler)
	gateway := httptest.NewServer(handler)
	defer gateway.Close()

	conn, reader := openWebSocket(t, gateway.Listener.Addr().String(), "/llm/v1/responses?alpha=1", http.Header{
		"Authorization": []string{"Bearer provider-token"},
		"X-Test":        []string{"preserved"},
	})
	defer conn.Close()

	firstCreate := `{"type":"response.create","model":"gpt-5.5","input":[]}`
	secondCreate := `{"type":"response.create","model":"gpt-5.4","input":[]}`
	if _, err := conn.Write(makeWebSocketFrame(true, websocketOpcodeText, []byte(firstCreate))); err != nil {
		t.Fatalf("write first create: %v", err)
	}
	if _, _, err := readWebSocketFrame(reader); err != nil {
		t.Fatalf("read first response: %v", err)
	}
	if _, err := conn.Write(makeWebSocketFrame(true, websocketOpcodeText, []byte(secondCreate))); err != nil {
		t.Fatalf("write second create: %v", err)
	}
	if _, _, err := readWebSocketFrame(reader); err != nil {
		t.Fatalf("read second response: %v", err)
	}
	_ = conn.Close()

	requireEventually(t, func() bool {
		capturedMu.Lock()
		defer capturedMu.Unlock()
		return len(captured) == 2
	})

	upstreamMu.Lock()
	if gotPath != "/v1/responses" {
		t.Fatalf("upstream path=%q, want /v1/responses", gotPath)
	}
	if gotQuery != "alpha=1" {
		t.Fatalf("upstream query=%q, want alpha=1", gotQuery)
	}
	if gotAuth != "Bearer provider-token" {
		t.Fatalf("upstream auth=%q, want provider token", gotAuth)
	}
	if len(gotClientMessages) != 2 || gotClientMessages[0] != firstCreate || gotClientMessages[1] != secondCreate {
		t.Fatalf("upstream client messages=%v", gotClientMessages)
	}
	upstreamMu.Unlock()

	capturedMu.Lock()
	defer capturedMu.Unlock()
	for i, exchange := range captured {
		if exchange.StatusCode != http.StatusSwitchingProtocols {
			t.Fatalf("captured[%d] status=%d, want 101", i, exchange.StatusCode)
		}
		if exchange.Transport != "websocket" {
			t.Fatalf("captured[%d] transport=%q, want websocket", i, exchange.Transport)
		}
		if exchange.WebSocketTurnIndex != i+1 {
			t.Fatalf("captured[%d] turn index=%d, want %d", i, exchange.WebSocketTurnIndex, i+1)
		}
		if exchange.WebSocketTurnStartType != "response.create" {
			t.Fatalf("captured[%d] start type=%q", i, exchange.WebSocketTurnStartType)
		}
		if exchange.WebSocketTurnTerminalType != "response.completed" {
			t.Fatalf("captured[%d] terminal type=%q", i, exchange.WebSocketTurnTerminalType)
		}
		if exchange.WebSocketRequestMessages != 1 || exchange.WebSocketResponseMessages != 1 {
			t.Fatalf("captured[%d] message counts request=%d response=%d", i, exchange.WebSocketRequestMessages, exchange.WebSocketResponseMessages)
		}
		if !strings.Contains(string(exchange.RequestBody), "response.create") {
			t.Fatalf("captured[%d] request body=%s", i, string(exchange.RequestBody))
		}
		if !strings.Contains(string(exchange.ResponseBody), "response.completed") {
			t.Fatalf("captured[%d] response body=%s", i, string(exchange.ResponseBody))
		}
	}
}

func TestWebSocketProxyStripsCompressionExtensionsFromUpstreamHandshake(t *testing.T) {
	t.Parallel()

	var upstreamMu sync.Mutex
	var gotExtensions string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamMu.Lock()
		gotExtensions = r.Header.Get("Sec-WebSocket-Extensions")
		upstreamMu.Unlock()

		conn, brw, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack upstream: %v", err)
			return
		}
		defer conn.Close()

		accept := websocketAccept(r.Header.Get("Sec-WebSocket-Key"))
		fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\n")
		fmt.Fprintf(brw, "Upgrade: websocket\r\n")
		fmt.Fprintf(brw, "Connection: Upgrade\r\n")
		fmt.Fprintf(brw, "Sec-WebSocket-Accept: %s\r\n\r\n", accept)
		if err := brw.Flush(); err != nil {
			t.Errorf("flush handshake: %v", err)
		}
	}))
	defer upstream.Close()

	proxyHandler, err := NewHandler([]Route{{Prefix: "/llm", Upstream: upstream.URL}}, slog.New(slog.NewTextHandler(io.Discard, nil)), http.NotFoundHandler())
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	gateway := httptest.NewServer(proxyHandler)
	defer gateway.Close()

	conn, _ := openWebSocket(t, gateway.Listener.Addr().String(), "/llm/v1/responses", http.Header{
		"Sec-WebSocket-Extensions": []string{"permessage-deflate; client_max_window_bits"},
	})
	defer conn.Close()

	upstreamMu.Lock()
	defer upstreamMu.Unlock()
	if gotExtensions != "" {
		t.Fatalf("upstream Sec-WebSocket-Extensions=%q, want empty", gotExtensions)
	}
}

func TestWebSocketTurnSplitterHandlesCodexTurnsAndSkipsControlMessages(t *testing.T) {
	t.Parallel()

	var captured []*CapturedExchange
	recorder := &webSocketTurnRecorder{
		config: webSocketCaptureConfig{
			maxBodySize: 4096,
			base: CapturedExchange{
				Method:         http.MethodGet,
				Path:           "/llm/app-server",
				RequestHeaders: http.Header{},
				Transport:      "websocket",
			},
			sink: func(exchange *CapturedExchange) {
				copied := *exchange
				captured = append(captured, &copied)
			},
		},
		connectionID: "ws-test",
	}

	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"method":"initialize","id":0}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"method":"turn/start","id":1,"params":{"threadId":"thr_1","input":[{"type":"text","text":"a"}]}}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"method":"turn/steer","id":2,"params":{"threadId":"thr_1","input":[{"type":"text","text":"b"}]}}`)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"method":"item/agentMessage/delta","params":{"delta":"ok"}}`)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"method":"turn/completed","params":{"threadId":"thr_1"}}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"method":"model/list","id":3}`)))
	recorder.Close()

	if len(captured) != 1 {
		t.Fatalf("captured len=%d, want 1", len(captured))
	}
	exchange := captured[0]
	if exchange.WebSocketTurnStartType != "turn/start" {
		t.Fatalf("turn start type=%q, want turn/start", exchange.WebSocketTurnStartType)
	}
	if exchange.WebSocketTurnTerminalType != "turn/completed" {
		t.Fatalf("turn terminal type=%q, want turn/completed", exchange.WebSocketTurnTerminalType)
	}
	if exchange.WebSocketRequestMessages != 2 {
		t.Fatalf("request messages=%d, want 2", exchange.WebSocketRequestMessages)
	}
	if strings.Contains(string(exchange.RequestBody), "initialize") || strings.Contains(string(exchange.RequestBody), "model/list") {
		t.Fatalf("control messages leaked into request body: %s", string(exchange.RequestBody))
	}
	if !strings.Contains(string(exchange.RequestBody), "turn/steer") {
		t.Fatalf("turn/steer not captured in active turn: %s", string(exchange.RequestBody))
	}
}

func TestWebSocketTurnSplitterCapturesRealtimeConversationItemAndResponseDone(t *testing.T) {
	t.Parallel()

	var captured []*CapturedExchange
	recorder := &webSocketTurnRecorder{
		config: webSocketCaptureConfig{
			maxBodySize: 4096,
			base: CapturedExchange{
				Method:         http.MethodGet,
				Path:           "/llm/v1/responses",
				RequestHeaders: http.Header{},
				Transport:      "websocket",
			},
			sink: func(exchange *CapturedExchange) {
				copied := *exchange
				captured = append(captured, &copied)
			},
		},
		connectionID: "ws-test",
	}

	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"type":"session.update","session":{"modalities":["text"]}}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"type":"conversation.item.create","item":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(`{"type":"response.create","response":{"modalities":["text"]}}`)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"type":"response.output_text.delta","delta":"ok"}`)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"type":"response.done","response":{"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`)))
	recorder.Close()

	if len(captured) != 1 {
		t.Fatalf("captured len=%d, want 1", len(captured))
	}
	exchange := captured[0]
	if exchange.Transport != "websocket" {
		t.Fatalf("transport=%q, want websocket", exchange.Transport)
	}
	if exchange.WebSocketTurnStartType != "conversation.item.create" {
		t.Fatalf("turn start type=%q, want conversation.item.create", exchange.WebSocketTurnStartType)
	}
	if exchange.WebSocketTurnTerminalType != "response.done" {
		t.Fatalf("turn terminal type=%q, want response.done", exchange.WebSocketTurnTerminalType)
	}
	if exchange.WebSocketRequestMessages != 2 {
		t.Fatalf("request messages=%d, want 2", exchange.WebSocketRequestMessages)
	}
	if exchange.WebSocketResponseMessages != 2 {
		t.Fatalf("response messages=%d, want 2", exchange.WebSocketResponseMessages)
	}
	requestBody := string(exchange.RequestBody)
	if !strings.Contains(requestBody, "conversation.item.create") || !strings.Contains(requestBody, "response.create") {
		t.Fatalf("request body=%s, want conversation item and response create", requestBody)
	}
	if strings.Contains(requestBody, "session.update") {
		t.Fatalf("control message leaked into request body: %s", requestBody)
	}
	if !strings.Contains(string(exchange.ResponseBody), "response.done") {
		t.Fatalf("response body=%s, want response.done", string(exchange.ResponseBody))
	}
}

func TestWebSocketTurnCaptureTruncatesPerTurn(t *testing.T) {
	t.Parallel()

	var captured []*CapturedExchange
	recorder := &webSocketTurnRecorder{
		config: webSocketCaptureConfig{
			maxBodySize: 64,
			base:        CapturedExchange{Method: http.MethodGet, Path: "/llm/v1/responses", RequestHeaders: http.Header{}, Transport: "websocket"},
			sink: func(exchange *CapturedExchange) {
				copied := *exchange
				captured = append(captured, &copied)
			},
		},
		connectionID: "ws-test",
	}

	longCreate := `{"type":"response.create","model":"gpt-5.5","input":[{"type":"message","content":"` + strings.Repeat("x", 200) + `"}]}`
	shortCreate := `{"type":"response.create","model":"gpt-5.4","input":[]}`
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(longCreate)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"type":"response.completed"}`)))
	recorder.ObserveClientBytes(makeWebSocketFrame(true, websocketOpcodeText, []byte(shortCreate)))
	recorder.ObserveServerBytes(makeWebSocketFrame(false, websocketOpcodeText, []byte(`{"type":"response.completed"}`)))

	if len(captured) != 2 {
		t.Fatalf("captured len=%d, want 2", len(captured))
	}
	if !captured[0].RequestBodyTruncated {
		t.Fatal("first turn should be truncated")
	}
	if captured[1].RequestBodyTruncated {
		t.Fatal("second turn should not inherit first turn truncation")
	}
	if !strings.Contains(string(captured[1].RequestBody), "gpt-5.4") {
		t.Fatalf("second turn body=%s", string(captured[1].RequestBody))
	}
}

func openWebSocket(t *testing.T, addr, path string, headers http.Header) (net.Conn, *bufio.Reader) {
	t.Helper()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial gateway: %v", err)
	}
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	fmt.Fprintf(conn, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(conn, "Host: %s\r\n", addr)
	fmt.Fprintf(conn, "Connection: Upgrade\r\n")
	fmt.Fprintf(conn, "Upgrade: websocket\r\n")
	fmt.Fprintf(conn, "Sec-WebSocket-Version: 13\r\n")
	fmt.Fprintf(conn, "Sec-WebSocket-Key: %s\r\n", key)
	for name, values := range headers {
		for _, value := range values {
			fmt.Fprintf(conn, "%s: %s\r\n", name, value)
		}
	}
	fmt.Fprintf(conn, "\r\n")

	reader := bufio.NewReader(conn)
	status, err := reader.ReadString('\n')
	if err != nil {
		conn.Close()
		t.Fatalf("read handshake status: %v", err)
	}
	if !strings.Contains(status, "101") {
		conn.Close()
		t.Fatalf("handshake status=%q, want 101", strings.TrimSpace(status))
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			conn.Close()
			t.Fatalf("read handshake header: %v", err)
		}
		if line == "\r\n" {
			break
		}
	}
	return conn, reader
}

func websocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func makeWebSocketFrame(mask bool, opcode byte, payload []byte) []byte {
	header := []byte{0x80 | opcode}
	length := len(payload)
	switch {
	case length < 126:
		header = append(header, byte(length))
	case length <= 0xffff:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], uint64(length))
		header = append(header, 127)
		header = append(header, buf[:]...)
	}
	if !mask {
		return append(header, payload...)
	}
	header[1] |= 0x80
	maskKey := []byte{1, 2, 3, 4}
	out := append(header, maskKey...)
	for i, b := range payload {
		out = append(out, b^maskKey[i%4])
	}
	return out
}

func readWebSocketFrame(r *bufio.Reader) (byte, []byte, error) {
	first, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	second, err := r.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	masked := second&0x80 != 0
	length := int(second & 0x7f)
	switch length {
	case 126:
		var buf [2]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, nil, err
		}
		length = int(binary.BigEndian.Uint16(buf[:]))
	case 127:
		var buf [8]byte
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return 0, nil, err
		}
		length64 := binary.BigEndian.Uint64(buf[:])
		if length64 > uint64(int(^uint(0)>>1)) {
			return 0, nil, fmt.Errorf("frame too large: %d", length64)
		}
		length = int(length64)
	}
	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(r, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= maskKey[i%4]
		}
	}
	return first & 0x0f, payload, nil
}

func requireEventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition was not met before deadline")
}
