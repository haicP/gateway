package backup

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/ongoingai/gateway/internal/trace"
)

const (
	defaultMaxPartBytes      = int64(128 * 1024 * 1024)
	defaultMaxUploadAttempts = 3
)

// Window identifies the trace time range included in a backup run.
type Window struct {
	Start time.Time
	End   time.Time
}

// TraceExporter streams traces for a backup window.
type TraceExporter interface {
	ExportTraces(ctx context.Context, window Window, yield func(*trace.Trace) error) error
}

// UploadObject describes a local file that should be uploaded to object storage.
type UploadObject struct {
	LocalPath   string
	Key         string
	ContentType string
}

// Uploader stores backup artifacts under their object keys.
type Uploader interface {
	Upload(ctx context.Context, object UploadObject) error
}

// RunnerOptions controls backup artifact generation and upload behavior.
type RunnerOptions struct {
	LocalDir          string
	KeyPrefix         string
	MaxPartBytes      int64
	MaxUploadAttempts int
	RetryBackoff      time.Duration
	RetryMaxBackoff   time.Duration
	Location          *time.Location
}

// Runner exports traces, writes gzip NDJSON parts, and uploads archive artifacts.
type Runner struct {
	exporter TraceExporter
	uploader Uploader
	opts     RunnerOptions
}

// PartSummary records the uploaded metadata for one gzip NDJSON part.
type PartSummary struct {
	FileName        string
	LocalPath       string
	Key             string
	Records         int
	CompressedBytes int64
	SHA256          string
	Window          Window
}

// Result summarizes one completed backup run.
type Result struct {
	Window               Window
	Date                 time.Time
	Parts                []PartSummary
	TotalRecords         int
	TotalCompressedBytes int64
	ReadmeKey            string
	ReadmePath           string
}

// NewRunner constructs a trace backup runner.
func NewRunner(exporter TraceExporter, uploader Uploader, opts RunnerOptions) (*Runner, error) {
	if exporter == nil {
		return nil, errors.New("backup exporter is required")
	}
	if uploader == nil {
		return nil, errors.New("backup uploader is required")
	}
	if strings.TrimSpace(opts.LocalDir) == "" {
		return nil, errors.New("backup local dir is required")
	}
	if opts.MaxPartBytes <= 0 {
		opts.MaxPartBytes = defaultMaxPartBytes
	}
	if opts.MaxUploadAttempts <= 0 {
		opts.MaxUploadAttempts = defaultMaxUploadAttempts
	}
	if opts.Location == nil {
		opts.Location = time.UTC
	}
	opts.KeyPrefix = strings.Trim(strings.TrimSpace(opts.KeyPrefix), "/")

	return &Runner{
		exporter: exporter,
		uploader: uploader,
		opts:     opts,
	}, nil
}

// RunDate backs up traces from midnight at date through the next local midnight.
func (r *Runner) RunDate(ctx context.Context, date time.Time) (*Result, error) {
	localDate := date.In(r.opts.Location)
	year, month, day := localDate.Date()
	start := time.Date(year, month, day, 0, 0, 0, 0, r.opts.Location)
	return r.RunWindow(ctx, Window{Start: start, End: start.AddDate(0, 0, 1)})
}

// RunWindow backs up traces for window and uploads readme metadata last.
func (r *Runner) RunWindow(ctx context.Context, window Window) (*Result, error) {
	if !window.Start.Before(window.End) {
		return nil, fmt.Errorf("backup window start must be before end: %s >= %s", window.Start.Format(time.RFC3339), window.End.Format(time.RFC3339))
	}
	if err := os.MkdirAll(r.opts.LocalDir, 0o700); err != nil {
		return nil, fmt.Errorf("create backup local dir: %w", err)
	}

	result := &Result{
		Window: window,
		Date:   window.Start.In(r.opts.Location),
	}
	writer := &partWriter{
		ctx:    ctx,
		runner: r,
		window: window,
		result: result,
	}

	if err := r.exporter.ExportTraces(ctx, window, writer.writeTrace); err != nil {
		_ = writer.closeWithoutUpload()
		return nil, fmt.Errorf("export traces: %w", err)
	}
	if err := writer.closeAndUpload(ctx); err != nil {
		return nil, err
	}
	if err := r.writeAndUploadReadme(ctx, result); err != nil {
		return nil, err
	}
	return result, nil
}

type partWriter struct {
	ctx    context.Context
	runner *Runner
	window Window
	result *Result

	partNumber int
	current    *openPart
}

type openPart struct {
	fileName  string
	localPath string
	file      *os.File
	gzip      *gzip.Writer
	encoder   *json.Encoder
	records   int
	firstTime time.Time
	lastTime  time.Time
}

func (w *partWriter) writeTrace(tr *trace.Trace) error {
	if tr == nil {
		return nil
	}
	if w.current == nil {
		if err := w.openNextPart(); err != nil {
			return err
		}
	}
	if err := w.current.encoder.Encode(tr); err != nil {
		return fmt.Errorf("write trace ndjson: %w", err)
	}
	w.current.records++
	traceTime := tr.Timestamp
	if traceTime.IsZero() {
		traceTime = w.window.Start
	}
	if w.current.firstTime.IsZero() {
		w.current.firstTime = traceTime
	}
	w.current.lastTime = traceTime
	w.result.TotalRecords++

	if err := w.current.gzip.Flush(); err != nil {
		return fmt.Errorf("flush gzip part: %w", err)
	}
	size, err := fileSize(w.current.localPath)
	if err != nil {
		return err
	}
	if size >= w.runner.opts.MaxPartBytes {
		return w.closeAndUpload(w.ctx)
	}
	return nil
}

func (w *partWriter) openNextPart() error {
	w.partNumber++
	fileName := tempPartFileName(w.window, w.partNumber, w.runner.opts.Location)
	localPath := filepath.Join(w.runner.opts.LocalDir, fileName)
	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create backup part %s: %w", fileName, err)
	}
	gz := gzip.NewWriter(file)
	w.current = &openPart{
		fileName:  fileName,
		localPath: localPath,
		file:      file,
		gzip:      gz,
		encoder:   json.NewEncoder(gz),
	}
	return nil
}

func (w *partWriter) closeAndUpload(ctx context.Context) error {
	if w.current == nil {
		return nil
	}
	part, err := w.closeCurrent()
	if err != nil {
		return err
	}
	if part.Records == 0 {
		if err := os.Remove(part.LocalPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove empty backup part: %w", err)
		}
		return nil
	}
	if err := w.runner.uploadWithRetry(ctx, UploadObject{
		LocalPath:   part.LocalPath,
		Key:         part.Key,
		ContentType: "application/gzip",
	}); err != nil {
		return err
	}
	if err := os.Remove(part.LocalPath); err != nil {
		return fmt.Errorf("remove uploaded backup part %s: %w", part.FileName, err)
	}
	w.result.Parts = append(w.result.Parts, part)
	w.result.TotalCompressedBytes += part.CompressedBytes
	return nil
}

func (w *partWriter) closeWithoutUpload() error {
	if w.current == nil {
		return nil
	}
	current := w.current
	w.current = nil
	gzipErr := current.gzip.Close()
	fileErr := current.file.Close()
	if gzipErr != nil {
		return gzipErr
	}
	return fileErr
}

func (w *partWriter) closeCurrent() (PartSummary, error) {
	current := w.current
	w.current = nil
	if err := current.gzip.Close(); err != nil {
		_ = current.file.Close()
		return PartSummary{}, fmt.Errorf("close gzip part %s: %w", current.fileName, err)
	}
	if err := current.file.Close(); err != nil {
		return PartSummary{}, fmt.Errorf("close backup part %s: %w", current.fileName, err)
	}
	size, err := fileSize(current.localPath)
	if err != nil {
		return PartSummary{}, err
	}
	finalFileName := partFileName(w.window, w.partNumber, current.firstTime, current.lastTime, w.runner.opts.Location)
	finalPath := filepath.Join(w.runner.opts.LocalDir, finalFileName)
	if err := os.Rename(current.localPath, finalPath); err != nil {
		return PartSummary{}, fmt.Errorf("rename backup part %s to %s: %w", current.fileName, finalFileName, err)
	}
	hash, err := sha256File(finalPath)
	if err != nil {
		return PartSummary{}, err
	}
	return PartSummary{
		FileName:        finalFileName,
		LocalPath:       finalPath,
		Key:             w.runner.objectKey(w.window, finalFileName),
		Records:         current.records,
		CompressedBytes: size,
		SHA256:          hash,
		Window:          w.window,
	}, nil
}

func (r *Runner) writeAndUploadReadme(ctx context.Context, result *Result) error {
	fileName := readmeFileName(result.Window, r.opts.Location)
	localPath := filepath.Join(r.opts.LocalDir, fileName)
	result.ReadmePath = localPath
	result.ReadmeKey = r.objectKey(result.Window, fileName)

	file, err := os.OpenFile(localPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create backup readme: %w", err)
	}
	if err := writeReadme(file, result); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close backup readme: %w", err)
	}
	if err := r.uploadWithRetry(ctx, UploadObject{
		LocalPath:   localPath,
		Key:         result.ReadmeKey,
		ContentType: "text/plain; charset=utf-8",
	}); err != nil {
		return err
	}
	if err := os.Remove(localPath); err != nil {
		return fmt.Errorf("remove uploaded backup readme %s: %w", fileName, err)
	}
	return nil
}

func writeReadme(w io.Writer, result *Result) error {
	if _, err := fmt.Fprintf(w, "backup_date: %s\n", result.Window.Start.Format("2006-01-02")); err != nil {
		return fmt.Errorf("write backup readme: %w", err)
	}
	lines := []string{
		"window_start: " + result.Window.Start.Format(time.RFC3339),
		"window_end: " + result.Window.End.Format(time.RFC3339),
		fmt.Sprintf("part_count: %d", len(result.Parts)),
		fmt.Sprintf("total_records: %d", result.TotalRecords),
		fmt.Sprintf("total_compressed_bytes: %d", result.TotalCompressedBytes),
		"",
		"parts:",
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return fmt.Errorf("write backup readme: %w", err)
		}
	}
	for _, part := range result.Parts {
		lines := []string{
			"- file: " + part.FileName,
			fmt.Sprintf("  records: %d", part.Records),
			fmt.Sprintf("  compressed_bytes: %d", part.CompressedBytes),
			"  s3_key: " + part.Key,
			"  sha256: " + part.SHA256,
			"  window_start: " + part.Window.Start.Format(time.RFC3339),
			"  window_end: " + part.Window.End.Format(time.RFC3339),
		}
		for _, line := range lines {
			if _, err := fmt.Fprintln(w, line); err != nil {
				return fmt.Errorf("write backup readme: %w", err)
			}
		}
	}
	return nil
}

func (r *Runner) uploadWithRetry(ctx context.Context, object UploadObject) error {
	var lastErr error
	for attempt := 1; attempt <= r.opts.MaxUploadAttempts; attempt++ {
		if err := r.uploader.Upload(ctx, object); err == nil {
			return nil
		} else {
			lastErr = err
		}

		if attempt == r.opts.MaxUploadAttempts {
			break
		}
		backoff := r.retryBackoff(attempt)
		if backoff <= 0 {
			continue
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return fmt.Errorf("upload backup object %s after %d attempts: %w", object.Key, r.opts.MaxUploadAttempts, lastErr)
}

func (r *Runner) retryBackoff(attempt int) time.Duration {
	if r.opts.RetryBackoff <= 0 {
		return 0
	}
	backoff := r.opts.RetryBackoff
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if r.opts.RetryMaxBackoff > 0 && backoff >= r.opts.RetryMaxBackoff {
			return r.opts.RetryMaxBackoff
		}
	}
	if r.opts.RetryMaxBackoff > 0 && backoff > r.opts.RetryMaxBackoff {
		return r.opts.RetryMaxBackoff
	}
	return backoff
}

func (r *Runner) objectKey(window Window, fileName string) string {
	dateDir := window.Start.In(r.opts.Location).Format("060102")
	if r.opts.KeyPrefix == "" {
		return path.Join(dateDir, fileName)
	}
	return path.Join(r.opts.KeyPrefix, dateDir, fileName)
}

func tempPartFileName(window Window, partNumber int, loc *time.Location) string {
	start := window.Start.In(loc)
	return fmt.Sprintf("request_details_%s_part_%06d.tmp.ndjson.gz", start.Format("060102"), partNumber)
}

func partFileName(window Window, partNumber int, first, last time.Time, loc *time.Location) string {
	start := window.Start.In(loc)
	if first.IsZero() {
		first = window.Start
	}
	if last.IsZero() {
		last = first
	}
	first = first.In(loc)
	last = last.In(loc)
	return fmt.Sprintf("request_details_%s_part_%06d_%s_%s.ndjson.gz",
		start.Format("060102"),
		partNumber,
		first.Format("1504"),
		last.Format("1504"),
	)
}

func readmeFileName(window Window, loc *time.Location) string {
	return fmt.Sprintf("readme_%s.txt", window.Start.In(loc).Format("060102"))
}

func fileSize(localPath string) (int64, error) {
	info, err := os.Stat(localPath)
	if err != nil {
		return 0, fmt.Errorf("stat backup part: %w", err)
	}
	return info.Size(), nil
}

func sha256File(localPath string) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open backup part for sha256: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash backup part: %w", err)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
