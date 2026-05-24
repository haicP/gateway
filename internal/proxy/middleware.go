package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/ongoingai/gateway/internal/auth"
	"github.com/ongoingai/gateway/internal/correlation"
)

type LoggingOptions struct {
	WriteCorrelationHeader func(*http.Request) bool
}

func LoggingMiddleware(logger *slog.Logger, next http.Handler) http.Handler {
	return LoggingMiddlewareWithOptions(logger, next, LoggingOptions{})
}

func LoggingMiddlewareWithOptions(logger *slog.Logger, next http.Handler, options LoggingOptions) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	if next == nil {
		next = http.NotFoundHandler()
	}
	shouldWriteCorrelationHeader := options.WriteCorrelationHeader
	if shouldWriteCorrelationHeader == nil {
		shouldWriteCorrelationHeader = func(*http.Request) bool {
			return true
		}
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var correlationID string
		r, correlationID = correlation.EnsureRequest(r)
		if correlationID != "" && shouldWriteCorrelationHeader(r) {
			w.Header().Set(correlation.HeaderName, correlationID)
		}

		start := time.Now()
		recorder := newStatusResponseWriter(w)
		next.ServeHTTP(recorder, r)
		logger.InfoContext(r.Context(),
			"request complete",
			"correlation_id", correlationID,
			"method", r.Method,
			"path", r.URL.Path,
			"status", recorder.StatusCode(),
			"latency_ms", time.Since(start).Milliseconds(),
		)
	})
}

type BodyCaptureOptions struct {
	Enabled      bool
	ParseBodies  bool
	MaxBodySize  int
	ParseMaxSize int
}

type CapturedExchange struct {
	// Context carries the request context so downstream consumers (e.g. trace
	// enqueue) can create child spans of the HTTP request span.
	Context               context.Context
	StartedAt             time.Time
	Method                string
	Path                  string
	StatusCode            int
	RequestHeaders        http.Header
	RequestBody           []byte
	RequestBodyTruncated  bool
	RequestBodyPath       string
	RequestBodyBytes      int64
	RequestBodySHA256     string
	ResponseHeaders       http.Header
	ResponseBody          []byte
	ResponseBodyTruncated bool
	ResponseBodyPath      string
	ResponseBodyBytes     int64
	ResponseBodySHA256    string
	Streaming             bool
	StreamChunks          int
	TimeToFirstTokenMS    int64
	TimeToFirstTokenUS    int64
	DurationMS            int64
	GatewayOrgID          string
	GatewayWorkspaceID    string
	GatewayKeyID          string
	GatewayTeam           string
	GatewayRole           string
	CorrelationID         string
}

func (e *CapturedExchange) CleanupBodyFiles() {
	if e == nil {
		return
	}
	removeOnce := map[string]struct{}{}
	for _, path := range []string{e.RequestBodyPath, e.ResponseBodyPath} {
		if path == "" {
			continue
		}
		if _, seen := removeOnce[path]; seen {
			continue
		}
		removeOnce[path] = struct{}{}
		_ = os.Remove(path)
	}
	e.RequestBodyPath = ""
	e.ResponseBodyPath = ""
}

type BodyCaptureSink func(*CapturedExchange)

func BodyCaptureMiddleware(options BodyCaptureOptions, sink BodyCaptureSink, next http.Handler) http.Handler {
	if next == nil {
		next = http.NotFoundHandler()
	}
	if sink == nil {
		return next
	}
	if options.MaxBodySize <= 0 {
		options.MaxBodySize = 0
	}
	if options.ParseMaxSize <= 0 {
		options.ParseMaxSize = 1 << 20
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var correlationID string
		r, correlationID = correlation.EnsureRequest(r)

		start := time.Now()

		captureBodies := options.Enabled || options.ParseBodies
		var requestCapture *bodySpool
		if captureBodies {
			var err error
			requestCapture, err = newBodySpool(options.Enabled, options.MaxBodySize, options.ParseMaxSize)
			if err != nil {
				http.Error(w, "failed to initialize request body capture", http.StatusInternalServerError)
				return
			}
			r.Body = requestCapture.WrapReadCloser(r.Body)
		}

		recorder := newCaptureResponseWriter(w, options.MaxBodySize, options.ParseMaxSize, captureBodies, options.Enabled, start)
		next.ServeHTTP(recorder, r)
		if requestCapture != nil {
			if err := requestCapture.Close(); err != nil {
				http.Error(w, "failed to finalize request body capture", http.StatusInternalServerError)
				return
			}
		}
		if err := recorder.Close(); err != nil {
			http.Error(w, "failed to finalize response body capture", http.StatusInternalServerError)
			return
		}

		statusCode := recorder.StatusCode()
		if statusCode == 0 {
			statusCode = http.StatusOK
		}

		streaming := recorder.IsStreaming()
		responseBody := recorder.Body()
		responseBodyTruncated := recorder.BodyTruncated()
		requestBody := []byte(nil)
		requestBodyTruncated := false
		requestBodyPath := ""
		requestBodyBytes := int64(0)
		requestBodySHA256 := ""
		if requestCapture != nil {
			requestBody = requestCapture.Prefix()
			requestBodyTruncated = requestCapture.Truncated()
			requestBodyPath = requestCapture.Path()
			requestBodyBytes = requestCapture.Size()
			requestBodySHA256 = requestCapture.SHA256()
		}
		timeToFirstTokenMS := int64(0)
		timeToFirstTokenUS := int64(0)
		if streaming {
			// TTFT is measured from handler entry to the first upstream write so it
			// reflects perceived latency for streaming clients.
			timeToFirstTokenUS = recorder.TimeToFirstWriteUS()
			timeToFirstTokenMS = microsecondsToRoundedMilliseconds(timeToFirstTokenUS)
		}
		identity, _ := auth.IdentityFromContext(r.Context())

		gatewayOrgID := ""
		gatewayWorkspaceID := ""
		gatewayKeyID := ""
		gatewayTeam := ""
		gatewayRole := ""
		if identity != nil {
			gatewayOrgID = identity.OrgID
			gatewayWorkspaceID = identity.WorkspaceID
			gatewayKeyID = identity.KeyID
			gatewayTeam = identity.Team
			gatewayRole = identity.Role
		}

		sink(&CapturedExchange{
			Context:               r.Context(),
			StartedAt:             start.UTC(),
			Method:                r.Method,
			Path:                  r.URL.Path,
			StatusCode:            statusCode,
			RequestHeaders:        r.Header.Clone(),
			RequestBody:           requestBody,
			RequestBodyTruncated:  requestBodyTruncated,
			RequestBodyPath:       requestBodyPath,
			RequestBodyBytes:      requestBodyBytes,
			RequestBodySHA256:     requestBodySHA256,
			ResponseHeaders:       recorder.Header().Clone(),
			ResponseBody:          responseBody,
			ResponseBodyTruncated: responseBodyTruncated,
			ResponseBodyPath:      recorder.BodyPath(),
			ResponseBodyBytes:     recorder.BodyBytes(),
			ResponseBodySHA256:    recorder.BodySHA256(),
			Streaming:             streaming,
			StreamChunks:          recorder.StreamChunkCount(),
			TimeToFirstTokenMS:    timeToFirstTokenMS,
			TimeToFirstTokenUS:    timeToFirstTokenUS,
			DurationMS:            time.Since(start).Milliseconds(),
			GatewayOrgID:          gatewayOrgID,
			GatewayWorkspaceID:    gatewayWorkspaceID,
			GatewayKeyID:          gatewayKeyID,
			GatewayTeam:           gatewayTeam,
			GatewayRole:           gatewayRole,
			CorrelationID:         correlationID,
		})
	})
}

type bodySpool struct {
	captureBody  bool
	maxBodySize  int
	parseMaxSize int
	file         *os.File
	path         string
	hasher       hash.Hash
	prefix       bytes.Buffer
	size         int64
	captured     int64
	truncated    bool
}

func newBodySpool(captureBody bool, maxBodySize, parseMaxSize int) (*bodySpool, error) {
	spool := &bodySpool{
		captureBody:  captureBody,
		maxBodySize:  maxBodySize,
		parseMaxSize: parseMaxSize,
		hasher:       sha256.New(),
	}
	if captureBody {
		file, err := os.CreateTemp("", "ongoingai-body-*")
		if err != nil {
			return nil, err
		}
		spool.file = file
		spool.path = file.Name()
	}
	return spool, nil
}

func (s *bodySpool) WrapReadCloser(body io.ReadCloser) io.ReadCloser {
	if body == nil {
		body = http.NoBody
	}
	return &spoolingReadCloser{body: body, spool: s}
}

func (s *bodySpool) Write(p []byte) {
	if s == nil || len(p) == 0 {
		return
	}
	s.size += int64(len(p))
	prefixLimit := s.prefixLimit()
	if prefixLimit > 0 && s.prefix.Len() < prefixLimit {
		remaining := prefixLimit - s.prefix.Len()
		if remaining > len(p) {
			remaining = len(p)
		}
		_, _ = s.prefix.Write(p[:remaining])
	}
	if s.captureBody {
		if s.maxBodySize <= 0 {
			_, _ = s.hasher.Write(p)
			_, _ = s.file.Write(p)
			s.captured += int64(len(p))
			return
		}
		remaining := int64(s.maxBodySize) - s.captured
		if remaining <= 0 {
			s.truncated = true
			return
		}
		writeLen := len(p)
		if int64(writeLen) > remaining {
			writeLen = int(remaining)
			s.truncated = true
		}
		_, _ = s.hasher.Write(p[:writeLen])
		_, _ = s.file.Write(p[:writeLen])
		s.captured += int64(writeLen)
	}
}

func (s *bodySpool) Close() error {
	if s == nil || s.file == nil {
		return nil
	}
	err := s.file.Close()
	if s.captured <= int64(s.parseMaxSize) && !s.truncated {
		_ = os.Remove(s.path)
		s.path = ""
	}
	return err
}

func (s *bodySpool) Prefix() []byte {
	if s == nil {
		return nil
	}
	return limitBytes(s.prefix.Bytes(), s.prefixLimit())
}

func (s *bodySpool) prefixLimit() int {
	if s == nil {
		return 0
	}
	if s.captureBody && s.maxBodySize > 0 && s.maxBodySize < s.parseMaxSize {
		return s.maxBodySize
	}
	return s.parseMaxSize
}

func (s *bodySpool) Truncated() bool {
	if s == nil {
		return false
	}
	return s.truncated
}

func (s *bodySpool) Path() string {
	if s == nil || s.captured == 0 {
		return ""
	}
	return s.path
}

func (s *bodySpool) Size() int64 {
	if s == nil {
		return 0
	}
	return s.captured
}

func (s *bodySpool) SHA256() string {
	if s == nil || !s.captureBody || s.size == 0 {
		return ""
	}
	return hex.EncodeToString(s.hasher.Sum(nil))
}

type spoolingReadCloser struct {
	body  io.ReadCloser
	spool *bodySpool
}

func (r *spoolingReadCloser) Read(p []byte) (int, error) {
	n, err := r.body.Read(p)
	if n > 0 {
		r.spool.Write(p[:n])
	}
	return n, err
}

func (r *spoolingReadCloser) Close() error {
	return r.body.Close()
}

type readerWithCloser struct {
	io.Reader
	closer io.Closer
}

func (r *readerWithCloser) Close() error {
	if r.closer == nil {
		return nil
	}
	return r.closer.Close()
}

func captureRequestBody(body io.ReadCloser, maxBodySize int) ([]byte, io.ReadCloser, bool, error) {
	if body == nil {
		return nil, http.NoBody, false, nil
	}
	if maxBodySize <= 0 {
		data, err := io.ReadAll(body)
		if err != nil {
			_ = body.Close()
			return nil, nil, false, err
		}
		restored := &readerWithCloser{
			Reader: bytes.NewReader(data),
			closer: body,
		}
		return limitBytes(data, len(data)), restored, false, nil
	}

	limited := &io.LimitedReader{R: body, N: int64(maxBodySize) + 1}
	prefix, err := io.ReadAll(limited)
	if err != nil {
		_ = body.Close()
		return nil, nil, false, err
	}

	captured := limitBytes(prefix, maxBodySize)
	truncated := len(prefix) > maxBodySize
	// Replay bytes consumed for capture, then continue streaming from the original body.
	restored := &readerWithCloser{
		Reader: io.MultiReader(bytes.NewReader(prefix), body),
		closer: body,
	}
	return captured, restored, truncated, nil
}

func limitBytes(data []byte, max int) []byte {
	if len(data) <= max {
		copied := make([]byte, len(data))
		copy(copied, data)
		return copied
	}
	copied := make([]byte, max)
	copy(copied, data[:max])
	return copied
}

type captureResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	maxBodySize  int
	parseMaxSize int
	captureBody  bool
	persistBody  bool
	body         bytes.Buffer
	streaming    bool
	stream       StreamBuffer
	spool        *bodySpool
	truncated    bool
	startedAt    time.Time
	firstWriteUS int64
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func newStatusResponseWriter(w http.ResponseWriter) *statusResponseWriter {
	return &statusResponseWriter{ResponseWriter: w}
}

func (w *statusResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *statusResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *statusResponseWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	return w.ResponseWriter.Write(p)
}

func (w *statusResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, http.ErrNotSupported
	}
	return hijacker.Hijack()
}

func (w *statusResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *statusResponseWriter) StatusCode() int {
	if w.statusCode == 0 {
		return http.StatusOK
	}
	return w.statusCode
}

func newCaptureResponseWriter(w http.ResponseWriter, maxBodySize, parseMaxSize int, captureBody, persistBody bool, startedAt time.Time) *captureResponseWriter {
	var spool *bodySpool
	if captureBody {
		spool, _ = newBodySpool(persistBody, maxBodySize, parseMaxSize)
	}
	return &captureResponseWriter{
		ResponseWriter: w,
		maxBodySize:    maxBodySize,
		parseMaxSize:   parseMaxSize,
		captureBody:    captureBody,
		persistBody:    persistBody,
		stream:         newStreamBuffer(parseMaxSize),
		spool:          spool,
		startedAt:      startedAt,
		firstWriteUS:   -1,
	}
}

func (w *captureResponseWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *captureResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
	w.streaming = IsSSE(w.Header())
	w.ResponseWriter.WriteHeader(statusCode)
}

func (w *captureResponseWriter) Write(p []byte) (int, error) {
	if w.statusCode == 0 {
		w.statusCode = http.StatusOK
	}
	if !w.streaming {
		w.streaming = IsSSE(w.Header())
	}

	n, err := w.ResponseWriter.Write(p)
	if n > 0 {
		if w.firstWriteUS < 0 {
			w.firstWriteUS = time.Since(w.startedAt).Microseconds()
		}
		w.capture(p[:n])
	}
	return n, err
}

func (w *captureResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *captureResponseWriter) StatusCode() int {
	return w.statusCode
}

func (w *captureResponseWriter) Body() []byte {
	if !w.captureBody {
		return nil
	}
	if w.spool != nil {
		return w.spool.Prefix()
	}
	return limitBytes(w.body.Bytes(), w.parseMaxSize)
}

func (w *captureResponseWriter) capture(p []byte) {
	if !w.captureBody {
		return
	}
	if w.streaming {
		w.stream.Add(p)
	}
	if w.spool != nil {
		w.spool.Write(p)
		if w.spool.Truncated() {
			w.truncated = true
		}
		return
	}

	remaining := w.maxBodySize - w.body.Len()
	if remaining <= 0 {
		if len(p) > 0 {
			w.truncated = true
		}
		return
	}
	if len(p) > remaining {
		w.truncated = true
		p = p[:remaining]
	}
	_, _ = w.body.Write(p)
}

func (w *captureResponseWriter) Close() error {
	if w == nil || w.spool == nil {
		return nil
	}
	return w.spool.Close()
}

func (w *captureResponseWriter) BodyPath() string {
	if w == nil || w.spool == nil {
		return ""
	}
	return w.spool.Path()
}

func (w *captureResponseWriter) BodyBytes() int64 {
	if w == nil || w.spool == nil {
		return 0
	}
	return w.spool.Size()
}

func (w *captureResponseWriter) BodySHA256() string {
	if w == nil || w.spool == nil {
		return ""
	}
	return w.spool.SHA256()
}

func (w *captureResponseWriter) IsStreaming() bool {
	return w.streaming || IsSSE(w.Header())
}

func (w *captureResponseWriter) StreamChunkCount() int {
	return w.stream.Count()
}

func (w *captureResponseWriter) TimeToFirstWriteUS() int64 {
	if w.firstWriteUS < 0 {
		return 0
	}
	return w.firstWriteUS
}

func (w *captureResponseWriter) BodyTruncated() bool {
	return w.truncated
}

func microsecondsToRoundedMilliseconds(us int64) int64 {
	if us <= 0 {
		return 0
	}
	return (us + 999) / 1000
}
