package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/ongoingai/gateway/internal/config"
	"github.com/ongoingai/gateway/internal/trace"
)

type traceRetentionCleanupScheduler interface {
	Start(ctx context.Context)
	Shutdown(ctx context.Context) error
}

type traceRetentionCleanupSchedulerRunner struct {
	cfg     config.TraceRetentionConfig
	deleter trace.TraceRetentionDeleter
	logger  *slog.Logger
	loc     *time.Location
	runAt   time.Duration

	mu      sync.Mutex
	cancel  context.CancelFunc
	done    chan error
	started bool
}

func newTraceRetentionCleanupSchedulerFromConfig(cfg config.TraceRetentionConfig, store trace.TraceStore, logger *slog.Logger) (traceRetentionCleanupScheduler, error) {
	deleter, ok := store.(trace.TraceRetentionDeleter)
	if !ok {
		return nil, fmt.Errorf("trace store does not support retention cleanup")
	}
	loc, err := time.LoadLocation(strings.TrimSpace(cfg.CleanupTimezone))
	if err != nil {
		return nil, fmt.Errorf("load trace cleanup timezone: %w", err)
	}
	runAt, err := parseRequestDetailsBackupDailyAt(cfg.CleanupDailyAt)
	if err != nil {
		return nil, fmt.Errorf("parse trace cleanup daily_at: %w", err)
	}
	return &traceRetentionCleanupSchedulerRunner{
		cfg:     cfg,
		deleter: deleter,
		logger:  logger,
		loc:     loc,
		runAt:   runAt,
	}, nil
}

func (r *traceRetentionCleanupSchedulerRunner) Start(ctx context.Context) {
	if r == nil || r.deleter == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	workerCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan error, 1)
	r.started = true
	r.mu.Unlock()

	go func() {
		err := r.run(workerCtx)
		r.done <- err
		close(r.done)
		if err != nil && workerCtx.Err() == nil && r.logger != nil {
			r.logger.Error("trace retention cleanup scheduler stopped", "error", err)
		}
	}()
}

func (r *traceRetentionCleanupSchedulerRunner) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	r.mu.Lock()
	cancel := r.cancel
	done := r.done
	started := r.started
	r.mu.Unlock()
	if !started {
		return nil
	}
	if cancel != nil {
		cancel()
	}

	select {
	case err := <-done:
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *traceRetentionCleanupSchedulerRunner) run(ctx context.Context) error {
	for {
		now := time.Now().In(r.loc)
		next := nextTraceRetentionCleanupRunAt(now, r.runAt, r.loc)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		cutoff := time.Now().In(r.loc).AddDate(0, 0, -r.cfg.Days).UTC()
		deleted, err := r.deleter.DeleteTracesBefore(ctx, cutoff)
		if err != nil {
			if r.logger != nil && ctx.Err() == nil {
				r.logger.Error("trace retention cleanup failed", "error", err, "cutoff", cutoff.Format(time.RFC3339))
			}
			continue
		}
		if r.logger != nil {
			r.logger.Info("trace retention cleanup completed", "deleted", deleted, "cutoff", cutoff.Format(time.RFC3339))
		}
	}
}

func nextTraceRetentionCleanupRunAt(now time.Time, runAt time.Duration, loc *time.Location) time.Time {
	if loc == nil {
		loc = time.UTC
	}
	year, month, day := now.In(loc).Date()
	next := time.Date(year, month, day, 0, 0, 0, 0, loc).Add(runAt)
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
