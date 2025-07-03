package callback

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"

	"go.uber.org/zap"
)

// RequestBody represents the JSON body structure.
type RequestBody struct {
	ID     string                 `json:"id"`
	Status int                    `json:"status"`
	Detail map[string]interface{} `json:"detail"`
}

type ErrorDetail struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
}

var (
	ERROR_CODE_JOB_NAME_INVALID                         = "A101"
	ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE               = "A102"
	ERROR_CODE_JOB_NOT_EXIST                            = "A103"
	ERROR_CODE_GET_JOB_POD_FAILED                       = "A104"
	ERROR_CODE_JOB_CREATE_FAILED                        = "A105"
	ERROR_CODE_GET_JOB_FAILED                           = "A106"
	ERROR_CODE_JOB_TTL_SECONDS_AFTER_FINISHED_TOO_SMALL = "A107"
	ERROR_CODE_JOB_ACTIVE_DEADLINE_SECONDS_TOO_LARGE    = "A108"
	ERROR_CODE_JOB_CAST_OBJECT_FAILED                   = "A107"
	ERROR_CODE_JOB_RUN_FAILED                           = "A109"
)

func (body RequestBody) Post(url string) (*http.Response, error) {
	// Marshal the request body to JSON.
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	// 從 config 取得 callback 參數
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
		// 每次重試都建立新的 request
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
		logger.Error("request failed", zap.Error(err), zap.Int("attempt", attempt))

		// 若 context 已過期則不再重試
		if ctx.Err() != nil {
			break
		}

		// 指數退避，最大不超過 maxDelay
		delay := baseDelay << (attempt - 1)
		if delay > maxDelay {
			delay = maxDelay
		}
		time.Sleep(delay)
	}

	if lastErr == nil {
		lastErr = errors.New("request failed after multiple attempts")
	}
	return resp, lastErr
}
