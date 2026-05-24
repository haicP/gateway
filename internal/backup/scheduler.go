package backup

import (
	"context"
	"fmt"
	"time"
)

// SchedulerOptions controls daily backup scheduling.
type SchedulerOptions struct {
	Location     *time.Location
	RunAt        time.Duration
	BackupLag    time.Duration
	StopOnError  bool
	ErrorHandler func(error)
}

// Scheduler runs one backup per day for the completed date window.
type Scheduler struct {
	runner *Runner
	opts   SchedulerOptions
}

// NewScheduler constructs a daily backup scheduler.
func NewScheduler(runner *Runner, opts SchedulerOptions) (*Scheduler, error) {
	if runner == nil {
		return nil, fmt.Errorf("backup runner is required")
	}
	if opts.Location == nil {
		opts.Location = time.UTC
	}
	if opts.RunAt < 0 || opts.RunAt >= 24*time.Hour {
		return nil, fmt.Errorf("backup run_at must be within one day")
	}
	return &Scheduler{runner: runner, opts: opts}, nil
}

// Start blocks and runs backups on the configured daily schedule until ctx ends.
func (s *Scheduler) Start(ctx context.Context) error {
	for {
		now := time.Now().In(s.opts.Location)
		next := nextRunAt(now, s.opts.RunAt, s.opts.Location)
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}

		backupDate := next.Add(-s.opts.BackupLag).AddDate(0, 0, -1)
		if _, err := s.runner.RunDate(ctx, backupDate); err != nil {
			if s.opts.ErrorHandler != nil && ctx.Err() == nil {
				s.opts.ErrorHandler(err)
			}
			if s.opts.StopOnError {
				return err
			}
		}
	}
}

func nextRunAt(now time.Time, runAt time.Duration, loc *time.Location) time.Time {
	year, month, day := now.In(loc).Date()
	next := time.Date(year, month, day, 0, 0, 0, 0, loc).Add(runAt)
	if !next.After(now) {
		next = next.AddDate(0, 0, 1)
	}
	return next
}
