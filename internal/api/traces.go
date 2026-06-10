package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ongoingai/gateway/internal/pathutil"
	"github.com/ongoingai/gateway/internal/trace"
)

type tracesResponse struct {
	Items      []traceSummary `json:"items"`
	NextCursor string         `json:"next_cursor,omitempty"`
	TotalCount int64          `json:"total_count"`
}

type traceSummary struct {
	ID                 string    `json:"id"`
	Timestamp          time.Time `json:"timestamp"`
	Provider           string    `json:"provider"`
	ProviderPrefix     string    `json:"provider_prefix,omitempty"`
	Model              string    `json:"model"`
	RequestMethod      string    `json:"request_method"`
	RequestPath        string    `json:"request_path"`
	Endpoint           string    `json:"endpoint"`
	ResponseStatus     int       `json:"response_status"`
	UserAgent          string    `json:"user_agent,omitempty"`
	InputTokens        int       `json:"input_tokens"`
	OutputTokens       int       `json:"output_tokens"`
	TotalTokens        int       `json:"total_tokens"`
	LatencyMS          int64     `json:"latency_ms"`
	TimeToFirstTokenMS int64     `json:"time_to_first_token_ms"`
	TimeToFirstTokenUS int64     `json:"time_to_first_token_us"`
	APIKeyHash         string    `json:"api_key_hash,omitempty"`
	EstimatedCostUSD   float64   `json:"estimated_cost_usd"`
	CreatedAt          time.Time `json:"created_at"`
}

type traceDetail struct {
	ID                 string        `json:"id"`
	TraceGroupID       string        `json:"trace_group_id,omitempty"`
	Lineage            *traceLineage `json:"lineage,omitempty"`
	Timestamp          time.Time     `json:"timestamp"`
	Provider           string        `json:"provider"`
	ProviderPrefix     string        `json:"provider_prefix,omitempty"`
	Model              string        `json:"model"`
	RequestMethod      string        `json:"request_method"`
	RequestPath        string        `json:"request_path"`
	Endpoint           string        `json:"endpoint"`
	RequestHeaders     any           `json:"request_headers,omitempty"`
	RequestBody        string        `json:"request_body,omitempty"`
	ResponseStatus     int           `json:"response_status"`
	ResponseHeaders    any           `json:"response_headers,omitempty"`
	ResponseBody       string        `json:"response_body,omitempty"`
	LLMRequestPrompt   string        `json:"llm_request_prompt,omitempty"`
	LLMResponseContent any           `json:"llm_response_content,omitempty"`
	InputTokens        int           `json:"input_tokens"`
	OutputTokens       int           `json:"output_tokens"`
	TotalTokens        int           `json:"total_tokens"`
	LatencyMS          int64         `json:"latency_ms"`
	TimeToFirstTokenMS int64         `json:"time_to_first_token_ms"`
	TimeToFirstTokenUS int64         `json:"time_to_first_token_us"`
	APIKeyHash         string        `json:"api_key_hash,omitempty"`
	EstimatedCostUSD   float64       `json:"estimated_cost_usd"`
	Metadata           any           `json:"metadata,omitempty"`
	CreatedAt          time.Time     `json:"created_at"`
}

type traceLineage struct {
	GroupID            string `json:"group_id,omitempty"`
	ThreadID           string `json:"thread_id,omitempty"`
	RunID              string `json:"run_id,omitempty"`
	CheckpointID       string `json:"checkpoint_id,omitempty"`
	ParentCheckpointID string `json:"parent_checkpoint_id,omitempty"`
	CheckpointSeq      int64  `json:"checkpoint_seq,omitempty"`
	Immutable          bool   `json:"immutable"`
}

type traceReplayResponse struct {
	SourceTraceID      string                  `json:"source_trace_id"`
	TargetCheckpointID string                  `json:"target_checkpoint_id"`
	Lineage            *traceLineage           `json:"lineage,omitempty"`
	Checkpoints        []traceReplayCheckpoint `json:"checkpoints"`
	Truncated          bool                    `json:"truncated"`
	TargetTrace        traceDetail             `json:"target_trace"`
}

type traceReplayCheckpoint struct {
	ID             string        `json:"id"`
	Timestamp      time.Time     `json:"timestamp"`
	Provider       string        `json:"provider"`
	Model          string        `json:"model"`
	RequestMethod  string        `json:"request_method"`
	RequestPath    string        `json:"request_path"`
	ResponseStatus int           `json:"response_status"`
	TotalTokens    int           `json:"total_tokens"`
	LatencyMS      int64         `json:"latency_ms"`
	CreatedAt      time.Time     `json:"created_at"`
	Lineage        *traceLineage `json:"lineage,omitempty"`
}

type traceForkRequest struct {
	CheckpointID string `json:"checkpoint_id"`
	ThreadID     string `json:"thread_id"`
	RunID        string `json:"run_id"`
}

type traceForkResponse struct {
	ForkID             string            `json:"fork_id"`
	SourceTraceID      string            `json:"source_trace_id"`
	SourceCheckpointID string            `json:"source_checkpoint_id"`
	Lineage            traceForkLineage  `json:"lineage"`
	Headers            map[string]string `json:"headers"`
}

type traceForkLineage struct {
	GroupID            string `json:"group_id"`
	ThreadID           string `json:"thread_id"`
	RunID              string `json:"run_id"`
	ParentCheckpointID string `json:"parent_checkpoint_id"`
	CheckpointSeq      int64  `json:"checkpoint_seq"`
}

type tracePathRoute struct {
	ID     string
	Action string
}

const (
	lineageTraceGroupHeader       = "X-OngoingAI-Trace-Group-ID"
	lineageThreadHeader           = "X-OngoingAI-Thread-ID"
	lineageRunHeader              = "X-OngoingAI-Run-ID"
	lineageParentCheckpointHeader = "X-OngoingAI-Parent-Checkpoint-ID"
	lineageCheckpointSeqHeader    = "X-OngoingAI-Checkpoint-Seq"
	traceForkBodyLimit            = 16 << 10
)

func TracesHandler(store trace.TraceStore, providerPrefixes []string) http.Handler {
	prefixes := normalizeDashboardProviderPrefixes(providerPrefixes)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !requireMethod(w, r, http.MethodGet) {
			return
		}
		if store == nil {
			writeError(w, http.StatusServiceUnavailable, "trace store is not configured")
			return
		}

		filter, err := parseTraceFilter(r, prefixes)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}

		result, err := store.QueryTraces(r.Context(), filter)
		if err != nil {
			switch {
			case errors.Is(err, trace.ErrInvalidCursor):
				writeError(w, http.StatusBadRequest, err.Error())
			case errors.Is(err, trace.ErrNotImplemented):
				writeError(w, http.StatusNotImplemented, "trace query is not implemented")
			default:
				writeError(w, http.StatusInternalServerError, "failed to query traces")
			}
			return
		}

		items := make([]traceSummary, 0, len(result.Items))
		for _, item := range result.Items {
			items = append(items, summarizeTrace(item, prefixes))
		}

		writeJSON(w, http.StatusOK, tracesResponse{
			Items:      items,
			NextCursor: result.NextCursor,
			TotalCount: result.TotalCount,
		})
	})
}

func TraceDetailHandler(store trace.TraceStore, providerPrefixes []string) http.Handler {
	prefixes := normalizeDashboardProviderPrefixes(providerPrefixes)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if store == nil {
			writeError(w, http.StatusServiceUnavailable, "trace store is not configured")
			return
		}

		route, ok := parseTracePathRoute(r.URL.Path)
		if !ok {
			http.NotFound(w, r)
			return
		}

		switch route.Action {
		case "":
			if !requireMethod(w, r, http.MethodGet) {
				return
			}
			item, ok := loadScopedTrace(w, r, store, route.ID)
			if !ok {
				return
			}
			writeJSON(w, http.StatusOK, detailTrace(item, prefixes))
		case "replay":
			if !requireMethod(w, r, http.MethodGet) {
				return
			}
			handleTraceReplay(w, r, store, route.ID, prefixes)
		case "fork":
			if !requireMethod(w, r, http.MethodPost) {
				return
			}
			handleTraceFork(w, r, store, route.ID)
		default:
			http.NotFound(w, r)
			return
		}
	})
}

func parseTraceFilter(r *http.Request, providerPrefixes []string) (trace.TraceFilter, error) {
	query := r.URL.Query()
	limit, err := parseIntQuery(query.Get("limit"), "limit", 0, 200)
	if err != nil {
		return trace.TraceFilter{}, err
	}
	statusCode, err := parseIntQuery(query.Get("status"), "status", 100, 599)
	if err != nil {
		return trace.TraceFilter{}, err
	}
	minTokens, err := parseIntQuery(query.Get("min_tokens"), "min_tokens", 0, 10_000_000)
	if err != nil {
		return trace.TraceFilter{}, err
	}
	maxTokens, err := parseIntQuery(query.Get("max_tokens"), "max_tokens", 0, 10_000_000)
	if err != nil {
		return trace.TraceFilter{}, err
	}
	if minTokens > 0 && maxTokens > 0 && maxTokens < minTokens {
		return trace.TraceFilter{}, fmt.Errorf("max_tokens must be greater than or equal to min_tokens")
	}

	from, err := parseTimeQuery(query.Get("from"), false)
	if err != nil {
		return trace.TraceFilter{}, fmt.Errorf("invalid from: %w", err)
	}
	to, err := parseTimeQuery(query.Get("to"), true)
	if err != nil {
		return trace.TraceFilter{}, fmt.Errorf("invalid to: %w", err)
	}
	if !from.IsZero() && !to.IsZero() && to.Before(from) {
		return trace.TraceFilter{}, fmt.Errorf("to must be greater than or equal to from")
	}

	providerPrefix, err := parseProviderPrefixFilter(query.Get("prefix"), providerPrefixes)
	if err != nil {
		return trace.TraceFilter{}, err
	}

	filter := trace.TraceFilter{
		TraceGroupID:   strings.TrimSpace(query.Get("trace_group_id")),
		ThreadID:       strings.TrimSpace(query.Get("thread_id")),
		RunID:          strings.TrimSpace(query.Get("run_id")),
		Provider:       strings.TrimSpace(query.Get("provider")),
		ProviderPrefix: providerPrefix,
		Model:          strings.TrimSpace(query.Get("model")),
		EndpointPaths:  traceEndpointPathCandidates(query.Get("endpoint"), providerPrefixes),
		APIKeyHash:     strings.TrimSpace(query.Get("api_key_hash")),
		StatusCode:     statusCode,
		MinTokens:      minTokens,
		MaxTokens:      maxTokens,
		From:           from,
		To:             to,
		Limit:          limit,
		Cursor:         strings.TrimSpace(query.Get("cursor")),
	}
	applyTraceTenantScope(r, &filter)

	return filter, nil
}

func summarizeTrace(item *trace.Trace, providerPrefixes []string) traceSummary {
	return traceSummary{
		ID:                 item.ID,
		Timestamp:          item.Timestamp,
		Provider:           item.Provider,
		ProviderPrefix:     traceProviderPrefix(item.RequestPath, providerPrefixes),
		Model:              item.Model,
		RequestMethod:      item.RequestMethod,
		RequestPath:        item.RequestPath,
		Endpoint:           displayEndpoint(item.RequestPath, providerPrefixes),
		ResponseStatus:     item.ResponseStatus,
		UserAgent:          extractTraceUserAgent(item.RequestHeaders),
		InputTokens:        item.InputTokens,
		OutputTokens:       item.OutputTokens,
		TotalTokens:        item.TotalTokens,
		LatencyMS:          item.LatencyMS,
		TimeToFirstTokenMS: item.TimeToFirstTokenMS,
		TimeToFirstTokenUS: item.TimeToFirstTokenUS,
		APIKeyHash:         item.APIKeyHash,
		EstimatedCostUSD:   item.EstimatedCostUSD,
		CreatedAt:          item.CreatedAt,
	}
}

func detailTrace(item *trace.Trace, providerPrefixes []string) traceDetail {
	metadata := decodeJSONField(item.Metadata)
	return traceDetail{
		ID:                 item.ID,
		TraceGroupID:       item.TraceGroupID,
		Lineage:            buildTraceLineage(item, metadata, true),
		Timestamp:          item.Timestamp,
		Provider:           item.Provider,
		ProviderPrefix:     traceProviderPrefix(item.RequestPath, providerPrefixes),
		Model:              item.Model,
		RequestMethod:      item.RequestMethod,
		RequestPath:        item.RequestPath,
		Endpoint:           displayEndpoint(item.RequestPath, providerPrefixes),
		RequestHeaders:     decodeJSONField(item.RequestHeaders),
		RequestBody:        item.RequestBody,
		ResponseStatus:     item.ResponseStatus,
		ResponseHeaders:    decodeJSONField(item.ResponseHeaders),
		ResponseBody:       item.ResponseBody,
		LLMRequestPrompt:   item.LLMRequestPrompt,
		LLMResponseContent: decodeJSONField(item.LLMResponseContent),
		InputTokens:        item.InputTokens,
		OutputTokens:       item.OutputTokens,
		TotalTokens:        item.TotalTokens,
		LatencyMS:          item.LatencyMS,
		TimeToFirstTokenMS: item.TimeToFirstTokenMS,
		TimeToFirstTokenUS: item.TimeToFirstTokenUS,
		APIKeyHash:         item.APIKeyHash,
		EstimatedCostUSD:   item.EstimatedCostUSD,
		Metadata:           metadata,
		CreatedAt:          item.CreatedAt,
	}
}

func parseIntQuery(raw, name string, min, max int) (int, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return 0, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if parsed < min {
		return 0, fmt.Errorf("%s must be >= %d", name, min)
	}
	if max != 0 && parsed > max {
		return 0, fmt.Errorf("%s must be <= %d", name, max)
	}
	return parsed, nil
}

func parseTimeQuery(raw string, endOfDay bool) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, nil
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
	}
	for _, layout := range layouts {
		if layout == "2006-01-02" {
			parsed, err := time.ParseInLocation(layout, value, time.UTC)
			if err == nil {
				if endOfDay {
					return parsed.Add(24*time.Hour - time.Nanosecond), nil
				}
				return parsed, nil
			}
			continue
		}
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("expected RFC3339 or YYYY-MM-DD")
}

func decodeJSONField(raw string) any {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return raw
	}
	return decoded
}

func buildTraceLineage(item *trace.Trace, metadata any, includeDefaults bool) *traceLineage {
	if item == nil {
		return nil
	}

	lineage := &traceLineage{
		GroupID: strings.TrimSpace(item.TraceGroupID),
	}
	immutableSet := false

	if metadataObj, ok := metadataObject(metadata); ok {
		if value := metadataString(metadataObj, "lineage_group_id"); value != "" {
			lineage.GroupID = value
		}
		lineage.ThreadID = metadataString(metadataObj, "lineage_thread_id")
		lineage.RunID = metadataString(metadataObj, "lineage_run_id")
		lineage.CheckpointID = metadataString(metadataObj, "lineage_checkpoint_id")
		lineage.ParentCheckpointID = metadataString(metadataObj, "lineage_parent_checkpoint_id")

		if seq, ok := metadataInt64(metadataObj, "lineage_checkpoint_seq"); ok {
			lineage.CheckpointSeq = seq
		}
		if immutable, ok := metadataBool(metadataObj, "lineage_immutable"); ok {
			lineage.Immutable = immutable
			immutableSet = true
		}
	}

	if includeDefaults {
		if lineage.CheckpointID == "" {
			lineage.CheckpointID = strings.TrimSpace(item.ID)
		}
		if !immutableSet {
			lineage.Immutable = true
		}
	}

	if !includeDefaults &&
		lineage.GroupID == "" &&
		lineage.ThreadID == "" &&
		lineage.RunID == "" &&
		lineage.CheckpointID == "" &&
		lineage.ParentCheckpointID == "" &&
		lineage.CheckpointSeq == 0 &&
		!lineage.Immutable {
		return nil
	}

	return lineage
}

func metadataObject(value any) (map[string]any, bool) {
	typed, ok := value.(map[string]any)
	return typed, ok
}

func metadataString(metadata map[string]any, key string) string {
	raw, ok := metadata[key]
	if !ok {
		return ""
	}
	value, ok := raw.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(value)
}

func metadataInt64(metadata map[string]any, key string) (int64, bool) {
	raw, ok := metadata[key]
	if !ok {
		return 0, false
	}

	switch typed := raw.(type) {
	case float64:
		return int64(typed), true
	case float32:
		return int64(typed), true
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case int32:
		return int64(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return parsed, true
	case string:
		parsed, err := strconv.ParseInt(strings.TrimSpace(typed), 10, 64)
		if err != nil {
			return 0, false
		}
		return parsed, true
	default:
		return 0, false
	}
}

func metadataBool(metadata map[string]any, key string) (bool, bool) {
	raw, ok := metadata[key]
	if !ok {
		return false, false
	}
	switch typed := raw.(type) {
	case bool:
		return typed, true
	case string:
		value := strings.ToLower(strings.TrimSpace(typed))
		if value == "true" {
			return true, true
		}
		if value == "false" {
			return false, true
		}
	}
	return false, false
}

func parseTracePathRoute(path string) (tracePathRoute, bool) {
	prefix := "/api/traces/"
	if !strings.HasPrefix(path, prefix) {
		return tracePathRoute{}, false
	}
	suffix := strings.Trim(strings.TrimPrefix(path, prefix), "/")
	if suffix == "" {
		return tracePathRoute{}, false
	}
	parts := strings.Split(suffix, "/")
	if len(parts) > 2 {
		return tracePathRoute{}, false
	}
	if strings.TrimSpace(parts[0]) == "" {
		return tracePathRoute{}, false
	}
	route := tracePathRoute{
		ID: parts[0],
	}
	if len(parts) == 2 {
		route.Action = strings.TrimSpace(parts[1])
		if route.Action == "" {
			return tracePathRoute{}, false
		}
	}
	return route, true
}

func loadScopedTrace(w http.ResponseWriter, r *http.Request, store trace.TraceStore, id string) (*trace.Trace, bool) {
	item, err := store.GetTrace(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, trace.ErrNotFound):
			writeError(w, http.StatusNotFound, "trace not found")
		case errors.Is(err, trace.ErrNotImplemented):
			writeError(w, http.StatusNotImplemented, "trace detail is not implemented")
		default:
			writeError(w, http.StatusInternalServerError, "failed to read trace")
		}
		return nil, false
	}
	if item == nil || !traceVisibleInTenantScope(r, item) {
		writeError(w, http.StatusNotFound, "trace not found")
		return nil, false
	}
	return item, true
}

func applyTraceTenantScope(r *http.Request, filter *trace.TraceFilter) {
	if filter == nil {
		return
	}
	query := r.URL.Query()
	filter.OrgID = strings.TrimSpace(query.Get("org_id"))
	filter.WorkspaceID = strings.TrimSpace(query.Get("workspace_id"))
}

func traceVisibleInTenantScope(r *http.Request, item *trace.Trace) bool {
	if item == nil {
		return false
	}
	query := r.URL.Query()
	orgID := strings.TrimSpace(query.Get("org_id"))
	workspaceID := strings.TrimSpace(query.Get("workspace_id"))
	if orgID != "" && item.OrgID != orgID {
		return false
	}
	if workspaceID != "" && item.WorkspaceID != workspaceID {
		return false
	}
	return true
}

func handleTraceReplay(w http.ResponseWriter, r *http.Request, store trace.TraceStore, sourceTraceID string, providerPrefixes []string) {
	source, ok := loadScopedTrace(w, r, store, sourceTraceID)
	if !ok {
		return
	}

	query := r.URL.Query()
	targetCheckpointID := strings.TrimSpace(query.Get("checkpoint_id"))
	if targetCheckpointID == "" {
		targetCheckpointID = source.ID
	}
	overrideThreadID := strings.TrimSpace(query.Get("thread_id"))
	overrideRunID := strings.TrimSpace(query.Get("run_id"))
	replayLimit, err := parseIntQuery(query.Get("limit"), "limit", 0, 500)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if replayLimit <= 0 {
		replayLimit = 200
	}

	sourceMetadata := decodeJSONField(source.Metadata)
	sourceLineage := buildTraceLineage(source, sourceMetadata, true)
	if sourceLineage == nil {
		sourceLineage = &traceLineage{
			CheckpointID: source.ID,
			Immutable:    true,
		}
	}
	threadID := sourceLineage.ThreadID
	runID := sourceLineage.RunID
	if overrideThreadID != "" {
		threadID = overrideThreadID
	}
	if overrideRunID != "" {
		runID = overrideRunID
	}

	replayItems := make([]*trace.Trace, 0, replayLimit)
	replayTruncated := false
	groupID := strings.TrimSpace(sourceLineage.GroupID)
	if groupID != "" {
		filter := trace.TraceFilter{
			TraceGroupID: groupID,
			ThreadID:     threadID,
			RunID:        runID,
		}
		applyTraceTenantScope(r, &filter)
		replayItems, replayTruncated, err = queryReplayItems(r.Context(), store, filter, replayLimit)
		if err != nil {
			if errors.Is(err, trace.ErrNotImplemented) {
				writeError(w, http.StatusNotImplemented, "trace replay is not implemented")
				return
			}
			writeError(w, http.StatusInternalServerError, "failed to query replay checkpoints")
			return
		}
	}

	if len(replayItems) == 0 {
		replayItems = append(replayItems, source)
	}
	if !containsTraceID(replayItems, source.ID) {
		replayItems = append(replayItems, source)
	}
	sortReplayItems(replayItems)

	targetIndex := indexTraceID(replayItems, targetCheckpointID)
	if targetIndex < 0 {
		writeError(w, http.StatusNotFound, "checkpoint not found")
		return
	}

	targetTrace := replayItems[targetIndex]
	if fullTarget, err := store.GetTrace(r.Context(), targetTrace.ID); err == nil && traceVisibleInTenantScope(r, fullTarget) {
		targetTrace = fullTarget
	} else if err != nil && !errors.Is(err, trace.ErrNotFound) {
		writeError(w, http.StatusInternalServerError, "failed to read target trace")
		return
	}
	targetMetadata := decodeJSONField(targetTrace.Metadata)
	targetLineage := buildTraceLineage(targetTrace, targetMetadata, true)
	if targetLineage == nil {
		targetLineage = &traceLineage{
			CheckpointID: targetTrace.ID,
			Immutable:    true,
		}
	}
	if targetLineage.GroupID == "" && sourceLineage.GroupID != "" {
		targetLineage.GroupID = sourceLineage.GroupID
	}
	if targetLineage.ThreadID == "" {
		targetLineage.ThreadID = threadID
	}
	if targetLineage.RunID == "" {
		targetLineage.RunID = runID
	}

	checkpointHistory := replayItems[:targetIndex+1]
	checkpoints := make([]traceReplayCheckpoint, 0, len(checkpointHistory))
	for _, item := range checkpointHistory {
		metadata := decodeJSONField(item.Metadata)
		checkpoints = append(checkpoints, traceReplayCheckpoint{
			ID:             item.ID,
			Timestamp:      item.Timestamp,
			Provider:       item.Provider,
			Model:          item.Model,
			RequestMethod:  item.RequestMethod,
			RequestPath:    item.RequestPath,
			ResponseStatus: item.ResponseStatus,
			TotalTokens:    item.TotalTokens,
			LatencyMS:      item.LatencyMS,
			CreatedAt:      item.CreatedAt,
			Lineage:        buildTraceLineage(item, metadata, true),
		})
	}

	writeJSON(w, http.StatusOK, traceReplayResponse{
		SourceTraceID:      source.ID,
		TargetCheckpointID: targetTrace.ID,
		Lineage:            targetLineage,
		Checkpoints:        checkpoints,
		Truncated:          replayTruncated && targetIndex == len(replayItems)-1,
		TargetTrace:        detailTrace(targetTrace, providerPrefixes),
	})
}

func handleTraceFork(w http.ResponseWriter, r *http.Request, store trace.TraceStore, sourceTraceID string) {
	source, ok := loadScopedTrace(w, r, store, sourceTraceID)
	if !ok {
		return
	}

	var req traceForkRequest
	if r.Body != nil && r.Body != http.NoBody {
		defer r.Body.Close()
		r.Body = http.MaxBytesReader(w, r.Body, traceForkBodyLimit)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			var maxBytesErr *http.MaxBytesError
			if errors.As(err, &maxBytesErr) {
				writeError(w, http.StatusRequestEntityTooLarge, "fork request body too large")
				return
			}
			writeError(w, http.StatusBadRequest, "invalid fork request body")
			return
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid fork request body")
			return
		}
	}

	sourceMetadata := decodeJSONField(source.Metadata)
	sourceLineage := buildTraceLineage(source, sourceMetadata, true)
	if sourceLineage == nil {
		sourceLineage = &traceLineage{
			CheckpointID: source.ID,
			Immutable:    true,
		}
	}
	checkpointID := strings.TrimSpace(req.CheckpointID)
	if checkpointID == "" {
		checkpointID = source.ID
	}

	checkpoint := source
	checkpointLineage := sourceLineage
	if checkpointID != source.ID {
		candidate, ok := loadScopedTrace(w, r, store, checkpointID)
		if !ok {
			return
		}
		candidateLineage := buildTraceLineage(candidate, decodeJSONField(candidate.Metadata), true)
		if candidateLineage == nil {
			candidateLineage = &traceLineage{
				CheckpointID: candidate.ID,
				Immutable:    true,
			}
		}
		sourceGroupID := strings.TrimSpace(sourceLineage.GroupID)
		candidateGroupID := strings.TrimSpace(candidateLineage.GroupID)
		if sourceGroupID != "" && candidateGroupID != "" && sourceGroupID != candidateGroupID {
			writeError(w, http.StatusBadRequest, "checkpoint must belong to the same trace lineage group")
			return
		}
		checkpoint = candidate
		checkpointLineage = candidateLineage
	}

	groupID := firstNonEmpty(
		strings.TrimSpace(checkpointLineage.GroupID),
		strings.TrimSpace(sourceLineage.GroupID),
		strings.TrimSpace(source.TraceGroupID),
		checkpoint.ID,
	)
	threadID := firstNonEmpty(strings.TrimSpace(req.ThreadID), strings.TrimSpace(checkpointLineage.ThreadID), strings.TrimSpace(sourceLineage.ThreadID), "thread_"+checkpoint.ID)
	runID := strings.TrimSpace(req.RunID)
	if runID == "" {
		runID = newGeneratedID("run")
	}

	nextCheckpointSeq := int64(1)
	if checkpointLineage.CheckpointSeq > 0 {
		nextCheckpointSeq = checkpointLineage.CheckpointSeq + 1
	}

	headers := map[string]string{
		lineageTraceGroupHeader:       groupID,
		lineageThreadHeader:           threadID,
		lineageRunHeader:              runID,
		lineageParentCheckpointHeader: checkpoint.ID,
		lineageCheckpointSeqHeader:    strconv.FormatInt(nextCheckpointSeq, 10),
	}

	writeJSON(w, http.StatusOK, traceForkResponse{
		ForkID:             newGeneratedID("fork"),
		SourceTraceID:      source.ID,
		SourceCheckpointID: checkpoint.ID,
		Lineage: traceForkLineage{
			GroupID:            groupID,
			ThreadID:           threadID,
			RunID:              runID,
			ParentCheckpointID: checkpoint.ID,
			CheckpointSeq:      nextCheckpointSeq,
		},
		Headers: headers,
	})
}

func queryReplayItems(ctx context.Context, store trace.TraceStore, filter trace.TraceFilter, totalLimit int) ([]*trace.Trace, bool, error) {
	if totalLimit <= 0 {
		return nil, false, nil
	}
	pageSize := 200
	if pageSize > totalLimit {
		pageSize = totalLimit
	}

	items := make([]*trace.Trace, 0, totalLimit)
	cursor := ""
	truncated := false
	for len(items) < totalLimit {
		filter.Limit = pageSize
		filter.Cursor = cursor
		result, err := store.QueryTraces(ctx, filter)
		if err != nil {
			return nil, false, err
		}
		items = append(items, result.Items...)
		if result.NextCursor == "" || len(result.Items) == 0 {
			break
		}
		if len(items) >= totalLimit {
			truncated = true
			break
		}
		cursor = result.NextCursor
	}

	if len(items) > totalLimit {
		items = items[:totalLimit]
		truncated = true
	}

	return items, truncated, nil
}

func sortReplayItems(items []*trace.Trace) {
	trace.SortLineageTraces(items)
}

func indexTraceID(items []*trace.Trace, traceID string) int {
	needle := strings.TrimSpace(traceID)
	for i, item := range items {
		if strings.TrimSpace(item.ID) == needle {
			return i
		}
	}
	return -1
}

func containsTraceID(items []*trace.Trace, traceID string) bool {
	return indexTraceID(items, traceID) >= 0
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}

func normalizeDashboardProviderPrefixes(prefixes []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		normalized := pathutil.NormalizePrefix(prefix)
		if normalized == "/" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

func parseProviderPrefixFilter(raw string, providerPrefixes []string) (string, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return "", nil
	}
	normalized := pathutil.NormalizePrefix(value)
	if normalized == "/" {
		return "", fmt.Errorf("prefix must be one of the configured provider prefixes")
	}
	for _, prefix := range providerPrefixes {
		if normalized == prefix {
			return normalized, nil
		}
	}
	return "", fmt.Errorf("prefix must be one of the configured provider prefixes")
}

func traceEndpointPathCandidates(raw string, providerPrefixes []string) []string {
	endpoint := normalizeEndpointPath(raw)
	if endpoint == "" {
		return nil
	}
	seen := map[string]struct{}{endpoint: {}}
	out := []string{endpoint}
	for _, prefix := range providerPrefixes {
		prefixed := pathutil.NormalizePrefix(prefix) + endpoint
		if _, ok := seen[prefixed]; ok {
			continue
		}
		seen[prefixed] = struct{}{}
		out = append(out, prefixed)
	}
	return out
}

func normalizeEndpointPath(raw string) string {
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
	return value
}

func displayEndpoint(requestPath string, providerPrefixes []string) string {
	path := normalizeEndpointPath(requestPath)
	if path == "" {
		return "/"
	}
	for _, prefix := range providerPrefixes {
		if pathutil.HasPathPrefix(path, prefix) {
			return pathutil.StripPathPrefix(path, prefix)
		}
	}
	return path
}

func traceProviderPrefix(requestPath string, providerPrefixes []string) string {
	path := normalizeEndpointPath(requestPath)
	if path == "" {
		return ""
	}
	for _, prefix := range providerPrefixes {
		if pathutil.HasPathPrefix(path, prefix) {
			return prefix
		}
	}
	return ""
}

func extractTraceUserAgent(rawHeaders string) string {
	decoded := decodeJSONField(rawHeaders)
	headers, ok := decoded.(map[string]any)
	if !ok {
		return ""
	}
	for key, value := range headers {
		if !strings.EqualFold(key, "User-Agent") {
			continue
		}
		switch typed := value.(type) {
		case []any:
			parts := make([]string, 0, len(typed))
			for _, item := range typed {
				if part, ok := item.(string); ok && strings.TrimSpace(part) != "" {
					parts = append(parts, strings.TrimSpace(part))
				}
			}
			return strings.Join(parts, ", ")
		case string:
			return strings.TrimSpace(typed)
		default:
			return strings.TrimSpace(fmt.Sprint(typed))
		}
	}
	return ""
}

func newGeneratedID(prefix string) string {
	var bytes [8]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return prefix + "_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return prefix + "_" + hex.EncodeToString(bytes[:])
}
