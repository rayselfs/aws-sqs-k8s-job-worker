package utils

import (
	"context"
	"time"

	"aws-sqs-k8s-job-worker/internal/k8s"
	"aws-sqs-k8s-job-worker/internal/logger"
)

type JobDetail struct {
	Status   k8s.CallbackStatus
	Duration time.Duration
}

type JobStatus int

const (
	StatusJobInit JobStatus = iota
	StatusJobCreated
	StatusJobRunning
	StatusJobDone
)

func (s JobStatus) String() string {
	switch s {
	case StatusJobInit:
		return "JobInit"
	case StatusJobCreated:
		return "JobCreated"
	case StatusJobRunning:
		return "JobRunning"
	case StatusJobDone:
		return "JobDone"
	default:
		return "Unknown"
	}
}

// Generic retry function for transient errors.
// T is the return type (e.g. *Pod, *Job, error-only, etc).
func Retry[T any](ctx context.Context, attempts int, delay time.Duration, fn func() (T, error)) (T, error) {
	var zero T
	var lastErr error

	for i := 0; i < attempts; i++ {
		result, err := fn()
		if err == nil {
			return result, nil
		}
		lastErr = err
		logger.WarnCtx(ctx, "retry attempt %d/%d failed: %v", i+1, attempts, err)

		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return zero, lastErr
}
