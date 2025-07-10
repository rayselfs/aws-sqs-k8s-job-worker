package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/k8s"
	"aws-sqs-k8s-job-worker/internal/logger"
)

// RequestBody represents the body of a callback request.
type RequestBody struct {
	ID     string             `json:"id"`
	Status k8s.CallbackStatus `json:"status"`
	Detail map[string]any     `json:"detail"`
}

// ErrorDetail provides error code and message for callback responses.
type ErrorDetail struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
}

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

func Send(ctx context.Context, jobMsg k8s.JobMessage, status k8s.CallbackStatus, detail map[string]any) {
	if jobMsg.Webhook == nil {
		return
	}

	requestBody := RequestBody{
		ID:     jobMsg.ID,
		Status: status,
		Detail: detail,
	}

	resp, err := requestBody.Post(ctx, jobMsg.Webhook.URL)
	if err != nil {
		logger.ErrorCtx(ctx, "callback error for job %s: %s", jobMsg.ID, err.Error())
		return
	}

	logger.InfoCtx(ctx, "callback sent, status: %d, jobID: %s", status, jobMsg.ID)
	resp.Body.Close()
}

// Post sends the callback request to the specified URL with retry and timeout logic.
func (body RequestBody) Post(ctx context.Context, url string) (*http.Response, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	maxRetries := configs.Env.CallbackMaxRetries
	baseDelay := configs.Env.CallbackBaseDelayDuration
	maxDelay := configs.Env.CallbackMaxDelayDuration
	totalTimeout := configs.Env.CallbackTotalTimeoutDuration

	timeoutCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	client := &http.Client{}
	var resp *http.Response
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(timeoutCtx, "POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")
		// 其他 header 可以再補

		resp, err = client.Do(req)
		if resp != nil {
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return resp, nil
			}
			// 關閉 body，避免多次 resp 泄漏
			resp.Body.Close()
			lastErr = fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}

		if err != nil {
			lastErr = err
		}

		if ctx.Err() != nil {
			break
		}

		delay := baseDelay << (attempt - 1)
		if delay > maxDelay {
			delay = maxDelay
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
			// retry
		}
	}

	if lastErr == nil {
		lastErr = errors.New("request failed after multiple attempts")
	}
	return nil, lastErr
}
