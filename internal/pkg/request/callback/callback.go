package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"aws-sqs-k8s-job-worker/config"
)

type RequestBody struct {
	ID     string         `json:"id"`
	Status int            `json:"status"`
	Detail map[string]any `json:"detail"`
}

type ErrorDetail struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
}

var (
	ERROR_CODE_JOB_NAME_INVALID           = "A101"
	ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE = "A102"
	ERROR_CODE_JOB_CREATE_FAILED          = "A103"
	ERROR_CODE_JOB_POD_START_FAILED       = "A104"
	ERROR_CODE_JOB_POD_START_TIMEOUT      = "A105"
	ERROR_CODE_JOB_WATCH_FAILED           = "A106"
	ERROR_CODE_JOB_GET_DETAIL_FAILED      = "A107"
	ERROR_CODE_JOB_RUN_FAILED             = "A108"
)

func (body RequestBody) Post(url string) (*http.Response, error) {
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	maxRetries := config.Env.CallbackMaxRetries
	baseDelay := time.Duration(config.Env.CallbackBaseDelay) * time.Second
	maxDelay := time.Duration(config.Env.CallbackMaxDelay) * time.Second
	totalTimeout := time.Duration(config.Env.CallbackTotalTimeout) * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	client := &http.Client{}
	var resp *http.Response
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonData))
		if err != nil {
			lastErr = err
			break
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err = client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return resp, nil
		}

		lastErr = err

		if ctx.Err() != nil {
			break
		}

		delay := min(baseDelay<<(attempt-1), maxDelay)
		time.Sleep(delay)
	}

	if lastErr == nil {
		lastErr = errors.New("request failed after multiple attempts")
	}
	return resp, lastErr
}
