package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ongoingai/gateway/internal/trace"
)

const (
	llmResponseContentEnrichBufferSize = 1024
	llmResponseContentEnrichTimeout    = 2 * time.Second
	llmResponseContentShutdownTimeout  = 5 * time.Second
)

type llmResponseContentEnricher struct {
	store                  trace.TraceStore
	responseContentUpdater trace.LLMResponseContentUpdater
	requestPromptUpdater   trace.LLMRequestPromptUpdater
	logger                 *slog.Logger
	queue                  chan string
	timeout                time.Duration

	queueMu  sync.RWMutex
	wg       sync.WaitGroup
	stopped  atomic.Bool
	stopOnce sync.Once
}

func newLLMResponseContentEnricher(store trace.TraceStore, logger *slog.Logger, bufferSize int, timeout time.Duration) *llmResponseContentEnricher {
	if store == nil {
		return nil
	}
	responseUpdater, hasResponseUpdater := store.(trace.LLMResponseContentUpdater)
	requestUpdater, hasRequestUpdater := store.(trace.LLMRequestPromptUpdater)
	if !hasResponseUpdater && !hasRequestUpdater {
		return nil
	}
	if bufferSize <= 0 {
		bufferSize = llmResponseContentEnrichBufferSize
	}
	if timeout <= 0 {
		timeout = llmResponseContentEnrichTimeout
	}
	return &llmResponseContentEnricher{
		store:                  store,
		responseContentUpdater: responseUpdater,
		requestPromptUpdater:   requestUpdater,
		logger:                 logger,
		queue:                  make(chan string, bufferSize),
		timeout:                timeout,
	}
}

func (e *llmResponseContentEnricher) Start(_ context.Context) {
	if e == nil {
		return
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for traceID := range e.queue {
			e.enrich(traceID)
		}
	}()
}

func (e *llmResponseContentEnricher) EnqueueTraceIDs(ids []string) {
	if e == nil || len(ids) == 0 {
		return
	}
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		e.queueMu.RLock()
		if e.stopped.Load() {
			e.queueMu.RUnlock()
			return
		}
		select {
		case e.queue <- id:
		default:
			if e.logger != nil {
				e.logger.Warn("llm response content enrich queue is full; dropping enrich task", "trace_id", id)
			}
		}
		e.queueMu.RUnlock()
	}
}

func (e *llmResponseContentEnricher) Shutdown(ctx context.Context) error {
	if e == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	e.stopOnce.Do(func() {
		e.stopped.Store(true)
		e.queueMu.Lock()
		close(e.queue)
		e.queueMu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		e.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *llmResponseContentEnricher) enrich(traceID string) {
	traceID = strings.TrimSpace(traceID)
	if traceID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	item, err := e.store.GetTrace(ctx, traceID)
	if err != nil {
		if e.logger != nil && !errors.Is(err, trace.ErrNotFound) {
			e.logger.WarnContext(ctx, "failed to load trace for llm response content enrich", "trace_id", traceID, "error", err)
		}
		return
	}
	if !traceEligibleForLLMResponseContent(item) {
		e.enrichRequestPrompt(ctx, traceID, item)
		return
	}
	e.enrichRequestPrompt(ctx, traceID, item)
	metadata := trace.DecodeMetadataMap(item.Metadata)
	streaming, _ := trace.MetadataBool(metadata, "streaming")
	content, ok := extractLLMResponseContentJSON([]byte(item.ResponseBody), streaming)
	if !ok {
		return
	}
	if e.responseContentUpdater == nil {
		return
	}
	if err := e.responseContentUpdater.UpdateLLMResponseContent(ctx, traceID, content); err != nil {
		if e.logger != nil {
			e.logger.WarnContext(ctx, "failed to update llm response content", "trace_id", traceID, "error", err)
		}
	}
}

func (e *llmResponseContentEnricher) enrichRequestPrompt(ctx context.Context, traceID string, item *trace.Trace) {
	if e == nil || e.requestPromptUpdater == nil || !traceEligibleForLLMRequestPrompt(item) {
		return
	}
	prompt, ok := extractLLMRequestPrompt([]byte(item.RequestBody))
	if !ok {
		return
	}
	if err := e.requestPromptUpdater.UpdateLLMRequestPrompt(ctx, traceID, prompt); err != nil {
		if e.logger != nil {
			e.logger.WarnContext(ctx, "failed to update llm request prompt", "trace_id", traceID, "error", err)
		}
	}
}

func traceEligibleForLLMResponseContent(item *trace.Trace) bool {
	if item == nil || strings.TrimSpace(item.ResponseBody) == "" || strings.TrimSpace(item.LLMResponseContent) != "" || traceHasStorageRedactionSkip(item) {
		return false
	}
	return true
}

func traceEligibleForLLMRequestPrompt(item *trace.Trace) bool {
	if item == nil || strings.TrimSpace(item.RequestBody) == "" || strings.TrimSpace(item.LLMRequestPrompt) != "" || traceHasStorageRedactionSkip(item) {
		return false
	}
	return true
}

func traceHasStorageRedactionSkip(item *trace.Trace) bool {
	if item == nil {
		return true
	}
	metadata := trace.DecodeMetadataMap(item.Metadata)
	if status := trace.MetadataString(metadata, "body_pii_status"); status == "skipped_large_body" {
		return true
	}
	for _, key := range []string{"redaction_storage_skipped", "redaction_storage_drop"} {
		if value, ok := trace.MetadataBool(metadata, key); ok && value {
			return true
		}
	}
	return false
}

func shutdownLLMResponseContentEnricher(logger *slog.Logger, enricher *llmResponseContentEnricher, timeout time.Duration) {
	if enricher == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := enricher.Shutdown(ctx); err != nil && logger != nil {
		logger.Warn("failed to flush llm response content enrich tasks before shutdown", "error", fmt.Sprintf("%v", err), "timeout", timeout.String())
	}
}
