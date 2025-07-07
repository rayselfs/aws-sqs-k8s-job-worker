package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"aws-sqs-k8s-job-worker/internal/pkg/request/callback"
)

type Record struct {
	SQSMessage types.Message  `json:"sqsMessage"`
	JobMessage k8s.JobMessage `json:"jobMessage"`
	Status     int            `json:"status"`
}

const (
	StatusJobInit    = 0
	StatusJobCreated = 1
	StatusPodRunning = 2
	StatusJobDone    = 3
)

// marshalRecord marshals a Record to JSON string (utility function for DRY).
func marshalRecord(record Record) string {
	data, _ := json.Marshal(record)
	return string(data)
}

func Execution(record Record, cacheClient cache.Client, logCtx context.Context) {
	jobMsg := record.JobMessage

	// check job name
	jobName, err := jobMsg.CheckJobName()
	if err != nil {
		logger.ErrorCtx(logCtx, "job name invalid: %s", err.Error())
		sendCallback(jobMsg, k8s.StatusException, map[string]any{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NAME_INVALID,
				Message:   err.Error(),
			},
		}, logCtx)
		return
	}

	// apply job, status 0
	if record.Status == StatusJobInit {
		if jobMsg.JobExists(jobName) {
			logger.ErrorCtx(logCtx, "job already exists")
			sendCallback(jobMsg, k8s.StatusException, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: callback.ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE,
					Message:   fmt.Sprintf("job %s already exists in namespace %s", jobName, jobMsg.Job.Namespace),
				},
			}, logCtx)
			return
		}

		err := jobMsg.ApplyJob(jobName)
		if err != nil {
			logger.ErrorCtx(logCtx, "client apply job error")
			sendCallback(jobMsg, k8s.StatusException, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: callback.ERROR_CODE_JOB_CREATE_FAILED,
					Message:   err.Error(),
				},
			}, logCtx)
			return
		}

		job, _ := jobMsg.GetJob(jobName)
		sendCallback(jobMsg, k8s.StatusJobCreated, map[string]any{
			"jobId":   job.GetObjectMeta().GetUID(),
			"jobName": job.GetObjectMeta().GetName(),
		}, logCtx)

		record.Status = StatusJobCreated
		recordData := marshalRecord(record)
		err = cacheClient.Set(config.Env.CacheJobKeyPrefix+jobMsg.ID, recordData, time.Second*time.Duration(jobMsg.Job.ActiveDeadlineSeconds))
		if err != nil {
			logger.ErrorCtx(logCtx, "cache set error")
			return
		}
	}

	// get job pod, status 1
	if record.Status == StatusJobCreated {
		logger.InfoCtx(logCtx, "watching pod running")
		pod, err := jobMsg.WatchPodRunning(logCtx, jobName)
		if err != nil {
			errorCode := callback.ERROR_CODE_JOB_POD_START_FAILED
			if errors.Is(err, k8s.ErrPodStartTimeout) {
				errorCode = callback.ERROR_CODE_JOB_POD_START_TIMEOUT
			}

			logger.ErrorCtx(logCtx, "watch pod running error: %s", err.Error())
			sendCallback(jobMsg, k8s.StatusException, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: errorCode,
					Message:   err.Error(),
				},
			}, logCtx)
			return
		}

		sendCallback(jobMsg, k8s.StatusPodRunning, map[string]any{
			"podId":   pod.GetObjectMeta().GetUID(),
			"podName": pod.GetObjectMeta().GetName(),
		}, logCtx)

		record.Status = StatusPodRunning
		recordData := marshalRecord(record)
		err = cacheClient.Set(config.Env.CacheJobKeyPrefix+record.JobMessage.ID, recordData, time.Second*time.Duration(jobMsg.Job.ActiveDeadlineSeconds))
		if err != nil {
			logger.ErrorCtx(logCtx, "cache set error")
			return
		}
	}

	// wait job completed, status 2
	if record.Status == StatusPodRunning {
		logger.InfoCtx(logCtx, "watching job completion")
		err := jobMsg.WatchJobCompletion(logCtx, jobName)
		if !errors.Is(err, k8s.ErrJobFailed) {
			sendCallback(jobMsg, k8s.StatusException, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: callback.ERROR_CODE_JOB_WATCH_FAILED,
					Message:   err.Error(),
				},
			}, logCtx)
			return
		}

		record.Status = StatusJobDone
		recordData := marshalRecord(record)
		err = cacheClient.Set(config.Env.CacheJobKeyPrefix+record.JobMessage.ID, recordData, time.Second*time.Duration(jobMsg.Job.ActiveDeadlineSeconds))
		if err != nil {
			logger.ErrorCtx(logCtx, "cache set error")
			return
		}
	}

	if record.Status == StatusJobDone {
		jobStatus, duration, err := jobMsg.GetJobDetail(jobName)
		if err != nil {
			logger.ErrorCtx(logCtx, "get job detail, error: %s", err.Error())
			sendCallback(jobMsg, k8s.StatusException, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: callback.ERROR_CODE_JOB_GET_DETAIL_FAILED,
					Message:   err.Error(),
				},
			}, logCtx)
			return
		}

		if jobStatus == k8s.StatusJobFailed {
			jobFailureDetail, err := jobMsg.GetJobFailureDetail(jobName)
			if err != nil {
				logger.ErrorCtx(logCtx, "get job failure detail error: %s", err.Error())
				sendCallback(jobMsg, k8s.StatusException, map[string]any{
					"error": &callback.ErrorDetail{
						ErrorCode: callback.ERROR_CODE_JOB_GET_DETAIL_FAILED,
						Message:   err.Error(),
					},
				}, logCtx)
				return
			}

			jobFailureString, _ := json.Marshal(jobFailureDetail)
			logger.WarnCtx(logCtx, "job failed, issue detail: %s", string(jobFailureString))
			sendCallback(jobMsg, jobStatus, map[string]any{
				"error": &callback.ErrorDetail{
					ErrorCode: callback.ERROR_CODE_JOB_RUN_FAILED,
					Message:   string(jobFailureString),
				},
			}, logCtx)
		}

		logger.InfoCtx(logCtx, "getting job detail, status: %d, duration: %d", jobStatus, duration.Seconds())
		sendCallback(jobMsg, jobStatus, map[string]any{
			"duration": duration.Seconds(),
		}, logCtx)
	}
}

func sendCallback(jobMsg k8s.JobMessage, status int, detail map[string]any, logCtx context.Context) {
	if jobMsg.Webhook == nil {
		return
	}

	requestBody := callback.RequestBody{
		ID:     jobMsg.ID,
		Status: status,
		Detail: detail,
	}

	resp, err := requestBody.Post(jobMsg.Webhook.URL)
	if err != nil {
		logger.ErrorCtx(logCtx, "start job callback error")
		return
	}
	// Print the response status.
	logger.InfoCtx(logCtx, "callback response")
	resp.Body.Close()
}
