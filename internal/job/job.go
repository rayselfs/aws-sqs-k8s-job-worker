// Extracted from your original Execution function with improvements
package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/cache"
	"aws-sqs-k8s-job-worker/internal/callback"
	"aws-sqs-k8s-job-worker/internal/k8s"
	"aws-sqs-k8s-job-worker/internal/logger"
	prom "aws-sqs-k8s-job-worker/internal/metrics"
	"aws-sqs-k8s-job-worker/internal/utils"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
)

type Record struct {
	SQSMessage types.Message   `json:"sqsMessage"`
	JobMessage k8s.JobMessage  `json:"jobMessage"`
	JobName    string          `json:"jobName"`
	Status     utils.JobStatus `json:"status"`
}

func (record *Record) Execution(ctx context.Context, cacheClient cache.Client) {
	// maxStep is greater than number of states (currently 4), to allow one retry
	maxStep := 5
	for step := 0; step < maxStep; step++ {
		switch record.Status {
		case utils.StatusJobInit:
			err, errorCode := record.handleJobInit(ctx)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}
			record.Status = utils.StatusJobCreated
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobCreated:
			err, errorCode := record.handleJobCreated(ctx)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}

			record.Status = utils.StatusJobRunning
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobRunning:
			err, errorCode := record.handleJobRunning(ctx)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}

			record.Status = utils.StatusJobDone
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobDone:
			record.handleJobDone(ctx)
			return
		default:
			logger.WarnCtx(ctx, "unexpected job status %d", record.Status.String())
			return
		}
	}

	// optional fallback
	logger.WarnCtx(ctx, "job state loop exceeded maxStep (%d)", maxStep)
}

func (record *Record) handleJobInit(ctx context.Context) (err error, errorCode string) {
	exist, err := utils.Retry(ctx, 2, 3*time.Second, func() (bool, error) {
		return record.JobMessage.JobExists(ctx, record.JobName)
	})
	if err != nil {
		err = fmt.Errorf("failed to check job existence: %w", err)
		errorCode = callback.ERROR_CODE_JOB_GET
		return
	}
	if exist {
		err = fmt.Errorf("job %s already exists", record.JobName)
		errorCode = callback.ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE
		return
	}

	if err = record.JobMessage.ApplyJob(ctx, record.JobName); err != nil {
		errorCode = callback.ERROR_CODE_JOB_CREATE_FAILED
		return
	}

	job, err := utils.Retry(ctx, 2, 2*time.Second, func() (*batchV1.Job, error) {
		return record.JobMessage.GetJob(ctx, record.JobName)
	})
	if err != nil {
		errorCode = callback.ERROR_CODE_JOB_GET
		return
	}

	callback.Send(ctx, record.JobMessage, k8s.StatusJobCreated, map[string]any{
		"jobId":   job.GetObjectMeta().GetUID(),
		"jobName": job.GetObjectMeta().GetName(),
	})
	return
}

func (record *Record) handleJobCreated(ctx context.Context) (err error, errorCode string) {
	logger.InfoCtx(ctx, "watching pod running")
	pod, err := utils.Retry(ctx, 2, 3*time.Second, func() (*coreV1.Pod, error) {
		return record.JobMessage.WatchPodRunning(ctx, record.JobName)
	})
	if err != nil {
		errorCode = callback.ERROR_CODE_JOB_POD_START_FAILED
		if errors.Is(err, k8s.ErrPodStartTimeout) {
			errorCode = callback.ERROR_CODE_JOB_POD_START_TIMEOUT
		}
		return
	}
	callback.Send(ctx, record.JobMessage, k8s.StatusPodRunning, map[string]any{
		"podId":   pod.GetObjectMeta().GetUID(),
		"podName": pod.GetObjectMeta().GetName(),
	})
	return
}

func (record *Record) handleJobRunning(ctx context.Context) (err error, errorCode string) {
	logger.InfoCtx(ctx, "watching job completion")
	err = record.JobMessage.WatchJobCompletion(ctx, record.JobName)
	if err != nil && !errors.Is(err, k8s.ErrJobFailed) {
		errorCode = callback.ERROR_CODE_JOB_WATCH_FAILED
		return
	}
	return
}

func (record *Record) handleJobDone(ctx context.Context) {
	detail, err := utils.Retry(ctx, 2, 2*time.Second, func() (utils.JobDetail, error) {
		status, duration, err := record.JobMessage.GetJobDetail(ctx, record.JobName)
		return utils.JobDetail{Status: status, Duration: duration}, err
	})
	if err != nil {
		record.sendCallbackErr(ctx, callback.ERROR_CODE_JOB_GET_DETAIL_FAILED, err)
		return
	}
	if detail.Status == k8s.StatusJobFailed {
		failureDetail, err := utils.Retry(ctx, 2, 1*time.Second, func() (*k8s.JobFailureDetail, error) {
			return record.JobMessage.GetJobFailureDetail(ctx, record.JobName)
		})
		if err != nil {
			record.sendCallbackErr(ctx, callback.ERROR_CODE_JOB_GET_DETAIL_FAILED, err)
			return
		}
		jobFailureString, err := json.Marshal(failureDetail)
		if err != nil {
			logger.WarnCtx(ctx, "failed to marshal job failure detail: %v", err)
			jobFailureString = []byte(`{"marshal":"failed"}`)
		}
		logger.WarnCtx(ctx, "job failed, issue detail: %s", string(jobFailureString))
		callback.Send(ctx, record.JobMessage, detail.Status, map[string]any{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_RUN_FAILED,
				Message:   string(jobFailureString),
			},
		})
		prom.JobFailure.Inc()
		return
	}
	logger.InfoCtx(ctx, "job succeeded, duration: %.2fs", detail.Duration.Seconds())
	callback.Send(ctx, record.JobMessage, detail.Status, map[string]any{
		"duration": detail.Duration.Seconds(),
	})
	prom.JobSuccess.Inc()
}

func (record *Record) sendCallbackErr(ctx context.Context, errorCode string, err error) {
	logger.ErrorCtx(ctx, "error: %s", err.Error())
	callback.Send(ctx, record.JobMessage, k8s.StatusException, map[string]any{
		"error": &callback.ErrorDetail{
			ErrorCode: errorCode,
			Message:   err.Error(),
		},
	})
}

func (record *Record) TTL() time.Duration {
	return time.Second * time.Duration(record.JobMessage.Job.ActiveDeadlineSeconds)
}

func cacheStore(ctx context.Context, client cache.Client, record *Record) {
	data, err := json.Marshal(record)
	if err != nil {
		logger.WarnCtx(ctx, "marshal error, skip caching job %s, status: %d: %v", record.JobMessage.ID, record.Status.String(), err)
		return
	}
	if err := client.Set(ctx, configs.Env.CacheJobKeyPrefix+record.JobMessage.ID, data, record.TTL()); err != nil {
		logger.WarnCtx(ctx, "cache set error for job %s, status: %d: %v", record.JobMessage.ID, record.Status.String(), err)
	}
}
