package trace

import (
	"context"
	"errors"
	"strings"
	"time"
)

var ErrNotImplemented = errors.New("trace store method not implemented")
var ErrNotFound = errors.New("trace store record not found")
var ErrInvalidCursor = errors.New("trace cursor is invalid")

type TraceStore interface {
	WriteTrace(ctx context.Context, trace *Trace) error
	WriteBatch(ctx context.Context, traces []*Trace) error
	GetTrace(ctx context.Context, id string) (*Trace, error)
	QueryTraces(ctx context.Context, filter TraceFilter) (*TraceResult, error)
	CountTraces(ctx context.Context) (int64, error)
}

type TraceCounter interface {
	CountTraces(ctx context.Context) (int64, error)
}

// TraceRetentionDeleter deletes traces before a retention cutoff.
type TraceRetentionDeleter interface {
	DeleteTracesBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

// LLMResponseContentUpdater updates asynchronously derived semantic LLM output.
type LLMResponseContentUpdater interface {
	UpdateLLMResponseContent(ctx context.Context, traceID, content string) error
}

// LLMRequestPromptUpdater updates asynchronously derived request prompt text.
type LLMRequestPromptUpdater interface {
	UpdateLLMRequestPrompt(ctx context.Context, traceID, prompt string) error
}

// TraceExporter streams trace records in stable timestamp order for backups or
// external archival jobs.
type TraceExporter interface {
	ExportTraces(ctx context.Context, filter TraceExportFilter) (*TraceExportResult, error)
}

type TraceFilter struct {
	OrgID          string
	WorkspaceID    string
	TraceGroupID   string
	ThreadID       string
	RunID          string
	Provider       string
	ProviderPrefix string
	Model          string
	EndpointPaths  []string
	APIKeyHash     string
	StatusCode     int
	MinTokens      int
	MaxTokens      int
	From           time.Time
	To             time.Time
	Limit          int
	Cursor         string
}

func normalizeFilterProviderPrefix(raw string) string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return ""
	}
	if !strings.HasPrefix(value, "/") {
		value = "/" + value
	}
	if len(value) > 1 {
		value = strings.TrimRight(value, "/")
	}
	if value == "/" {
		return ""
	}
	return value
}

// TraceExportFilter selects a stable, forward-only page of traces for export.
type TraceExportFilter struct {
	OrgID        string
	WorkspaceID  string
	TraceGroupID string
	ThreadID     string
	RunID        string
	Provider     string
	Model        string
	APIKeyHash   string
	StatusCode   int
	MinTokens    int
	MaxTokens    int
	From         time.Time
	To           time.Time
	Limit        int
	Cursor       string
}

type TraceResult struct {
	Items      []*Trace
	NextCursor string
	TotalCount int64
}

// TraceExportResult contains one page of exported traces and the next-page cursor.
type TraceExportResult struct {
	Items      []*Trace
	NextCursor string
}
