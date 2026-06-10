package trace

import (
	"os"
	"time"
)

type Trace struct {
	ID                    string
	TraceGroupID          string
	OrgID                 string
	WorkspaceID           string
	Timestamp             time.Time
	Provider              string
	Model                 string
	RequestMethod         string
	RequestPath           string
	RequestHeaders        string
	RequestBody           string
	RequestBodyPath       string
	RequestBodyBytes      int64
	RequestBodySHA256     string
	RequestBodyTruncated  bool
	ResponseStatus        int
	ResponseHeaders       string
	ResponseBody          string
	LLMRequestPrompt      string
	LLMResponseContent    string
	ResponseBodyPath      string
	ResponseBodyBytes     int64
	ResponseBodySHA256    string
	ResponseBodyTruncated bool
	InputTokens           int
	OutputTokens          int
	TotalTokens           int
	LatencyMS             int64
	TimeToFirstTokenMS    int64
	TimeToFirstTokenUS    int64
	APIKeyHash            string
	GatewayKeyID          string
	EstimatedCostUSD      float64
	Metadata              string
	CreatedAt             time.Time
}

// CleanupBodyFiles removes temporary body spool files attached to the trace.
func (t *Trace) CleanupBodyFiles() {
	if t == nil {
		return
	}
	removeOnce := map[string]struct{}{}
	for _, path := range []string{t.RequestBodyPath, t.ResponseBodyPath} {
		if path == "" {
			continue
		}
		if _, seen := removeOnce[path]; seen {
			continue
		}
		removeOnce[path] = struct{}{}
		_ = os.Remove(path)
	}
	t.RequestBodyPath = ""
	t.ResponseBodyPath = ""
}
