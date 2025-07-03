package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"aws-sqs-k8s-job-worker/internal/pkg/rdb"
	"aws-sqs-k8s-job-worker/internal/pkg/request/callback"

	"go.uber.org/zap"
	batchV1 "k8s.io/api/batch/v1"
)

type Record struct {
	SQSMessage types.Message  `json:"sqsMessage"`
	JobMessage k8s.JobMessage `json:"jobMessage"`
	Status     int            `json:"status"`
}

const (
	StatusInit       = 0
	StatusJobCreated = 1
	StatusPodRunning = 2
	StatusJobDone    = 3
)

func Execution(record Record) {
	requestBody := GetCallbackRequest(record.JobMessage)
	jobMsg := record.JobMessage

	// Start job
	jobName, _ := jobMsg.CheckJobName()

	if record.Status == StatusInit {
		status, detail := validation(jobMsg)
		if status != 0 {
			requestBody.Status = status
			requestBody.Detail = detail
			sendCallback(jobMsg, requestBody)
			return
		}

		job, errorDetail := jobMsg.ApplyJob(jobName)
		if errorDetail != nil {
			logger.Error("apply job error",
				zap.String("jobId", jobMsg.ID),
				zap.String("error", errorDetail.Message),
			)
			requestBody.Status = k8s.StatusException
			requestBody.Detail = map[string]interface{}{
				"error": errorDetail,
			}
			sendCallback(jobMsg, requestBody)
			return
		}

		// Send started callback
		requestBody.Status = k8s.StatusJobCreated
		requestBody.Detail = map[string]interface{}{
			"jobId":   job.GetObjectMeta().GetUID(),
			"jobName": job.GetObjectMeta().GetName(),
		}
		sendCallback(jobMsg, requestBody)

		record.Status = StatusJobCreated
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	// check job exist
	job, errorDetail := jobMsg.JobExists(jobName)
	if errorDetail == nil {
		logger.Error("job not exists",
			zap.String("jobId", jobMsg.ID),
			zap.String("jobName", jobName),
			zap.String("namespace", jobMsg.Job.Namespace),
		)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NOT_EXIST,
				Message:   fmt.Sprintf("job %s not exists in namespace %s", jobName, jobMsg.Job.Namespace),
			},
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// process after job created
	if record.Status == StatusJobCreated {
		status := WaitPodRunning(jobMsg, job, requestBody)
		if status == k8s.StatusException {
			return
		}

		record.Status = StatusPodRunning
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+record.JobMessage.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	// process after job's pod running
	if record.Status == StatusPodRunning {
		status := WaitJobCompletion(jobMsg, job, requestBody)
		if status == k8s.StatusJobFailed || status == k8s.StatusException {
			return
		}

		record.Status = StatusJobDone
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+record.JobMessage.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	if record.Status == StatusJobDone {
		jobStatus, errorDetail := jobMsg.CheckJobStatus(jobName)
		if errorDetail != nil {
			logger.Error("check job status error",
				zap.String("jobId", jobMsg.ID),
				zap.String("error", errorDetail.Message),
			)
			requestBody.Status = k8s.StatusException
			requestBody.Detail = map[string]interface{}{
				"error": errorDetail,
			}
			sendCallback(jobMsg, requestBody)
			return
		}

		GetJobDurection(jobMsg, job, jobStatus, requestBody)
	}
}

func GetCallbackRequest(jobMsg k8s.JobMessage) (requestBody callback.RequestBody) {
	requestBody = callback.RequestBody{
		ID: jobMsg.ID,
	}
	return
}

// validation validates the job
func validation(jobMsg k8s.JobMessage) (int, map[string]interface{}) {
	jobName, err := jobMsg.CheckJobName()
	if err != nil {
		logger.Error("check job name error",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", err.Error()),
		)
		return k8s.StatusException, map[string]interface{}{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NAME_INVALID,
				Message:   err.Error(),
			},
		}
	}

	if jobMsg.Job.TTLSecondsAfterFinished < 60 {
		logger.Error("job's TTLSecondsAfterFinished is less than 60 seconds",
			zap.String("jobId", jobMsg.ID),
		)
		return k8s.StatusException, map[string]interface{}{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_TTL_SECONDS_AFTER_FINISHED_TOO_SMALL,
				Message:   "job's TTLSecondsAfterFinished is less than 60 seconds",
			},
		}
	}

	_, errorDetail := jobMsg.JobExists(jobName)
	if errorDetail != nil {
		logger.Error("job not exist",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", errorDetail.Message),
		)
		return k8s.StatusException, map[string]interface{}{
			"error": errorDetail,
		}
	}

	errorDetail = jobMsg.CheckActiveDeadlineSeconds()
	if errorDetail != nil {
		logger.Error("check active deadline seconds error",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", errorDetail.Message),
		)
		return k8s.StatusException, map[string]interface{}{
			"error": errorDetail,
		}
	}
	return 0, nil
}

func WaitPodRunning(jobMsg k8s.JobMessage, job *batchV1.Job, requestBody callback.RequestBody) int {
	logger.Info("watching pod running",
		zap.String("jobId", jobMsg.ID),
	)

	jobStatus, detail := jobMsg.WatchPodRunning(job)
	requestBody.Status = jobStatus
	requestBody.Detail = detail
	sendCallback(jobMsg, requestBody)

	return requestBody.Status
}

func WaitJobCompletion(jobMsg k8s.JobMessage, job *batchV1.Job, requestBody callback.RequestBody) int {
	// Watch for job completion
	logger.Info("watching job completion",
		zap.String("jobId", jobMsg.ID),
	)
	jobStatus, errorDetail := jobMsg.WatchJobCompletion(jobMsg.Job.Namespace, job.GetObjectMeta().GetName())
	if errorDetail != nil {
		logger.Error("watch job completion error",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", errorDetail.Message),
		)
		requestBody.Status = jobStatus
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
	}

	return requestBody.Status
}

func GetJobDurection(jobMsg k8s.JobMessage, job *batchV1.Job, jobStatus int, requestBody callback.RequestBody) {
	// Get job duration
	logger.Info("getting job duration",
		zap.String("jobId", jobMsg.ID),
	)
	duration, errorDetail := jobMsg.GetJobDuration(job.GetObjectMeta().GetName())
	if errorDetail != nil {
		logger.Error("get job duration error",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", errorDetail.Message),
		)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// Send finished callback
	requestBody.Status = jobStatus
	requestBody.Detail = map[string]interface{}{
		"duration": duration.Seconds(),
	}
	sendCallback(jobMsg, requestBody)
	return
}

func sendCallback(jobMsg k8s.JobMessage, body callback.RequestBody) {
	if jobMsg.Webhook == nil {
		return
	}

	callback := body
	resp, err := callback.Post(jobMsg.Webhook.URL)
	if err != nil {
		logger.Error("start job callback error",
			zap.String("jobId", jobMsg.ID),
			zap.String("error", err.Error()),
		)
		return
	}
	// Print the response status.
	logger.Info("callback response",
		zap.String("jobId", jobMsg.ID),
		zap.String("status", resp.Status),
	)
	resp.Body.Close()
}
