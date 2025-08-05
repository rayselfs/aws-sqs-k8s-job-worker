package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"aws-sqs-k8s-job-worker/internal/pkg/logger"
)

type CallbackClient struct {
	URL         string       // Callback URL
	RequestBody *RequestBody // Request body for the callback
}

// RequestBody represents the body of a callback request.
type RequestBody struct {
	ID     string         `json:"id"`
	Status Status         `json:"status"`
	Detail map[string]any `json:"detail"`
}

// ErrorDetail provides error code and message for callback responses.
type ErrorDetail struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
}

type Status int

const (
	StatusJobCreated   Status = 0
	StatusPodRunning   Status = 1
	StatusJobCompleted Status = 2
	StatusJobFailed    Status = 3
	StatusException    Status = 99
)

var (
	// ERROR_CODE_JOB_NAME_INVALID           = "A101"
	ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE = "A102"
	ERROR_CODE_JOB_CREATE_FAILED          = "A103"
	ERROR_CODE_JOB_POD_START_FAILED       = "A104"
	ERROR_CODE_JOB_POD_START_TIMEOUT      = "A105"
	ERROR_CODE_JOB_WATCH_FAILED           = "A106"
	ERROR_CODE_JOB_GET_DETAIL_FAILED      = "A107"
	ERROR_CODE_JOB_RUN_FAILED             = "A108"
	ERROR_CODE_JOB_GET                    = "A109"
)

// Post sends the callback request to the specified URL with retry and timeout logic.
func (c CallbackClient) Post(ctx context.Context) (*http.Response, error) {
	jsonData, err := json.Marshal(c.RequestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request body: %w", err)
	}

	logger.InfoCtx(ctx, "starting callback POST to URL: %s, payload: %s", c.URL, string(jsonData))

	maxRetries := 3
	baseDelay := 1 * time.Second
	maxDelay := 10 * time.Second
	totalTimeout := 120 * time.Second  // 120 seconds
	requestTimeout := 30 * time.Second // Individual request timeout

	timeoutCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	client := &http.Client{
		Timeout: requestTimeout,
	}
	var resp *http.Response
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		logger.InfoCtx(ctx, "callback attempt %d/%d to URL: %s", attempt, maxRetries, c.URL)

		req, err := http.NewRequestWithContext(timeoutCtx, "POST", c.URL, bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = fmt.Errorf("failed to create request: %w", err)
			logger.ErrorCtx(ctx, "failed to create HTTP request: %v", err)
			break
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "aws-sqs-k8s-job-worker/1.0")

		start := time.Now()
		resp, err = client.Do(req)
		duration := time.Since(start)

		if resp != nil {
			logger.InfoCtx(ctx, "callback response received in %v, status: %d", duration, resp.StatusCode)
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				logger.InfoCtx(ctx, "callback successful on attempt %d", attempt)
				return resp, nil
			}
			// 關閉 body，避免多次 resp 泄漏
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
			logger.WarnCtx(ctx, "callback failed with status %d on attempt %d", resp.StatusCode, attempt)
		}

		if err != nil {
			lastErr = fmt.Errorf("HTTP request failed: %w", err)
			logger.WarnCtx(ctx, "callback request failed on attempt %d: %v", attempt, err)
		}

		if ctx.Err() != nil {
			logger.ErrorCtx(ctx, "callback context cancelled: %v", ctx.Err())
			break
		}

		if attempt < maxRetries {
			delay := baseDelay << (attempt - 1)
			if delay > maxDelay {
				delay = maxDelay
			}
			logger.InfoCtx(ctx, "retrying callback in %v", delay)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
				// retry
			}
		}
	}

	if lastErr == nil {
		lastErr = errors.New("request failed after multiple attempts")
	}
	logger.ErrorCtx(ctx, "callback ultimately failed after %d attempts: %v", maxRetries, lastErr)
	return nil, lastErr
}
