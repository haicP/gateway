package main

import (
	"testing"
	"time"
)

func TestNextTraceRetentionCleanupRunAt(t *testing.T) {
	t.Parallel()

	loc := time.UTC
	runAt := 2*time.Hour + 30*time.Minute

	before := time.Date(2026, 2, 12, 2, 0, 0, 0, loc)
	got := nextTraceRetentionCleanupRunAt(before, runAt, loc)
	want := time.Date(2026, 2, 12, 2, 30, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("next before run_at=%s, want %s", got, want)
	}

	after := time.Date(2026, 2, 12, 3, 0, 0, 0, loc)
	got = nextTraceRetentionCleanupRunAt(after, runAt, loc)
	want = time.Date(2026, 2, 13, 2, 30, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("next after run_at=%s, want %s", got, want)
	}
}
