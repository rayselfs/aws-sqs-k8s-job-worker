package callback

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"k8s.io/klog/v2"
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
	ERROR_CODE_JOB_NAME_INVALID                      = "JOB_NAME_INVALID"
	ERROR_CODE_JOB_EXIST                             = "JOB_EXIST"
	ERROR_CODE_JOB_NOT_EXIST                         = "JOB_NOT_EXIST"
	ERROR_CODE_JOB_CREATE_FAILED                     = "JOB_CREATE_FAILED"
	ERROR_CODE_JOB_GET_FAILED                        = "JOB_GET_FAILED"
	ERROR_CODE_JOB_ACTIVE_DEADLINE_SECONDS_TOO_LARGE = "JOB_ACTIVE_DEADLINE_SECONDS_TOO_LARGE"
	ERROR_CODE_JOB_CAST_OBJECT_FAILED                = "JOB_CAST_OBJECT_FAILED"
	ERROR_CODE_JOB_RUN_FAILED                        = "JOB_RUN_FAILED"
)

func (body RequestBody) Post(url string) (*http.Response, error) {
	// Marshal the request body to JSON.
	jsonData, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	// Create a new POST request with the JSON body.
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	// Set the Content-Type header.
	req.Header.Set("Content-Type", "application/json")

	// Define the maximum number of retries and the delay between retries.
	const maxRetries = 10
	const retryDelay = 30 * time.Second

	// Create a new HTTP client.
	client := &http.Client{}

	// Attempt the request, retrying if necessary.
	var resp *http.Response
	for attempt := 1; attempt <= maxRetries; attempt++ {
		resp, err = client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			// Successful response
			return resp, nil
		}

		// If not successful and it's the last attempt, break and return the error.
		if attempt == maxRetries {
			break
		}

		klog.Errorf("request failed: %v, retry: %v", err, attempt)

		// Wait for a bit before retrying.
		time.Sleep(retryDelay)
	}

	// If we reach here, it means all retries failed.
	if err == nil {
		err = errors.New("request failed after multiple attempts")
	}
	return resp, err
}
