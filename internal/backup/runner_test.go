package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ongoingai/gateway/internal/trace"
)

func TestRunnerWritesGzipNDJSONNamesDeletesPartsAndUploadsReadmeLast(t *testing.T) {
	t.Parallel()

	exporter := fakeExporter{traces: []*trace.Trace{
		{ID: "trace-1", Provider: "openai", Timestamp: time.Date(2026, 5, 24, 1, 0, 0, 0, time.UTC)},
		{ID: "trace-2", Provider: "anthropic", Timestamp: time.Date(2026, 5, 24, 2, 0, 0, 0, time.UTC)},
		{ID: "trace-3", Provider: "openai", Timestamp: time.Date(2026, 5, 24, 3, 0, 0, 0, time.UTC)},
	}}
	uploader := &recordingUploader{}
	runner := newTestRunner(t, exporter, uploader, RunnerOptions{
		LocalDir:          t.TempDir(),
		KeyPrefix:         "archives/team-a",
		MaxPartBytes:      1,
		MaxUploadAttempts: 2,
		Location:          time.UTC,
	})

	result, err := runner.RunDate(context.Background(), time.Date(2026, 5, 24, 12, 30, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("RunDate() error = %v", err)
	}

	uploads := uploader.uploads()
	if len(uploads) != 4 {
		t.Fatalf("uploads=%d, want 4", len(uploads))
	}
	for i := 0; i < 3; i++ {
		hhmm := fmt.Sprintf("%02d00", i+1)
		wantName := fmt.Sprintf("request_details_260524_part_%06d_%s_%s.ndjson.gz", i+1, hhmm, hhmm)
		wantKey := "archives/team-a/260524/" + wantName
		if uploads[i].key != wantKey {
			t.Fatalf("upload[%d] key=%q, want %q", i, uploads[i].key, wantKey)
		}
		if _, err := os.Stat(uploads[i].localPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("part %s still exists after upload, stat err=%v", uploads[i].localPath, err)
		}
		records := decodeUploadedTraceIDs(t, uploads[i].body)
		if len(records) != 1 {
			t.Fatalf("part %d records=%d, want 1", i+1, len(records))
		}
	}
	if uploads[3].key != "archives/team-a/260524/readme_260524.txt" {
		t.Fatalf("last upload key=%q, want readme last", uploads[3].key)
	}
	if _, err := os.Stat(uploads[3].localPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("readme still exists after upload, stat err=%v", err)
	}

	readme := string(uploads[3].body)
	for _, want := range []string{
		"part_count: 3",
		"total_records: 3",
		"total_compressed_bytes:",
		"s3_key: archives/team-a/260524/request_details_260524_part_000001_0100_0100.ndjson.gz",
		"sha256:",
		"window_start: 2026-05-24T00:00:00Z",
		"window_end: 2026-05-25T00:00:00Z",
	} {
		if !strings.Contains(readme, want) {
			t.Fatalf("readme missing %q:\n%s", want, readme)
		}
	}
	if result.TotalRecords != 3 || len(result.Parts) != 3 {
		t.Fatalf("result records=%d parts=%d, want records=3 parts=3", result.TotalRecords, len(result.Parts))
	}
}

func TestRunnerRetriesUploadAndDeletesPartAfterSuccess(t *testing.T) {
	t.Parallel()

	uploader := &recordingUploader{failuresBeforeSuccess: map[string]int{
		"260524/request_details_260524_part_000001_0000_0000.ndjson.gz": 2,
	}}
	runner := newTestRunner(t, fakeExporter{traces: []*trace.Trace{{ID: "trace-1"}}}, uploader, RunnerOptions{
		LocalDir:          t.TempDir(),
		MaxPartBytes:      1024 * 1024,
		MaxUploadAttempts: 3,
		Location:          time.UTC,
	})

	if _, err := runner.RunDate(context.Background(), time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC)); err != nil {
		t.Fatalf("RunDate() error = %v", err)
	}

	if attempts := uploader.attempts("260524/request_details_260524_part_000001_0000_0000.ndjson.gz"); attempts != 3 {
		t.Fatalf("part upload attempts=%d, want 3", attempts)
	}
	uploads := uploader.uploads()
	if len(uploads) != 2 {
		t.Fatalf("successful uploads=%d, want part and readme", len(uploads))
	}
	if _, err := os.Stat(uploads[0].localPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("part still exists after retry success, stat err=%v", err)
	}
}

func TestRunnerStopsAfterRetriesAndPreservesFailedPart(t *testing.T) {
	t.Parallel()

	const partKey = "260524/request_details_260524_part_000001_0000_0000.ndjson.gz"
	uploader := &recordingUploader{failuresBeforeSuccess: map[string]int{partKey: 99}}
	runner := newTestRunner(t, fakeExporter{traces: []*trace.Trace{{ID: "trace-1"}}}, uploader, RunnerOptions{
		LocalDir:          t.TempDir(),
		MaxPartBytes:      1024 * 1024,
		MaxUploadAttempts: 2,
		Location:          time.UTC,
	})

	_, err := runner.RunDate(context.Background(), time.Date(2026, 5, 24, 0, 0, 0, 0, time.UTC))
	if err == nil {
		t.Fatal("RunDate() error = nil, want upload failure")
	}
	if attempts := uploader.attempts(partKey); attempts != 2 {
		t.Fatalf("part upload attempts=%d, want 2", attempts)
	}
	if uploader.successfulUploadCount() != 0 {
		t.Fatalf("successful uploads=%d, want 0", uploader.successfulUploadCount())
	}
	if _, err := os.Stat(uploader.lastPath(partKey)); err != nil {
		t.Fatalf("failed part was not preserved: %v", err)
	}
	if strings.Contains(err.Error(), "readme") {
		t.Fatalf("error suggests readme upload was attempted after part failure: %v", err)
	}
}

type fakeExporter struct {
	traces []*trace.Trace
}

func (f fakeExporter) ExportTraces(ctx context.Context, window Window, yield func(*trace.Trace) error) error {
	for _, tr := range f.traces {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if err := yield(tr); err != nil {
			return err
		}
	}
	return nil
}

type uploadRecord struct {
	key       string
	localPath string
	body      []byte
}

type recordingUploader struct {
	mu                    sync.Mutex
	failuresBeforeSuccess map[string]int
	attemptByKey          map[string]int
	lastPathByKey         map[string]string
	records               []uploadRecord
}

func (u *recordingUploader) Upload(_ context.Context, object UploadObject) error {
	u.mu.Lock()
	defer u.mu.Unlock()

	if u.attemptByKey == nil {
		u.attemptByKey = map[string]int{}
	}
	if u.lastPathByKey == nil {
		u.lastPathByKey = map[string]string{}
	}
	u.attemptByKey[object.Key]++
	u.lastPathByKey[object.Key] = object.LocalPath
	if remaining := u.failuresBeforeSuccess[object.Key]; remaining > 0 {
		u.failuresBeforeSuccess[object.Key] = remaining - 1
		return errors.New("temporary upload failure")
	}

	body, err := os.ReadFile(object.LocalPath)
	if err != nil {
		return err
	}
	u.records = append(u.records, uploadRecord{
		key:       object.Key,
		localPath: object.LocalPath,
		body:      body,
	})
	return nil
}

func (u *recordingUploader) uploads() []uploadRecord {
	u.mu.Lock()
	defer u.mu.Unlock()
	records := make([]uploadRecord, len(u.records))
	copy(records, u.records)
	return records
}

func (u *recordingUploader) attempts(key string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.attemptByKey[key]
}

func (u *recordingUploader) lastPath(key string) string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.lastPathByKey[key]
}

func (u *recordingUploader) successfulUploadCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.records)
}

func newTestRunner(t *testing.T, exporter TraceExporter, uploader Uploader, opts RunnerOptions) *Runner {
	t.Helper()
	runner, err := NewRunner(exporter, uploader, opts)
	if err != nil {
		t.Fatalf("NewRunner() error = %v", err)
	}
	return runner
}

func decodeUploadedTraceIDs(t *testing.T, body []byte) []string {
	t.Helper()
	reader, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip.NewReader() error = %v", err)
	}
	defer reader.Close()

	decoder := json.NewDecoder(reader)
	var ids []string
	for {
		var tr trace.Trace
		if err := decoder.Decode(&tr); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("Decode() error = %v", err)
		}
		ids = append(ids, tr.ID)
	}
	return ids
}
