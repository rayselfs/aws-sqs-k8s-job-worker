// Extracted from your original Execution function with improvements
package job

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/internal/app/callback"
	"aws-sqs-k8s-job-worker/internal/app/utils"
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	prom "aws-sqs-k8s-job-worker/internal/pkg/observability/metrics"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
)

type Record struct {
	SQSMessage types.Message   `json:"sqsMessage"`
	JobMessage k8s.JobMessage  `json:"jobMessage"`
	JobName    string          `json:"jobName"`
	Status     utils.JobStatus `json:"status"`
}

func (record *Record) Execution(ctx context.Context, k8sClient k8s.Client, cacheClient cache.Client) {
	// maxStep is greater than number of states (currently 4), to allow one retry
	maxStep := 5
	for step := 0; step < maxStep; step++ {
		switch record.Status {
		case utils.StatusJobInit:
			err, errorCode := record.handleJobInit(ctx, k8sClient)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}
			record.Status = utils.StatusJobCreated
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobCreated:
			err, errorCode := record.handleJobCreated(ctx, k8sClient)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}

			record.Status = utils.StatusJobRunning
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobRunning:
			err, errorCode := record.handleJobRunning(ctx, k8sClient)
			if err != nil {
				record.sendCallbackErr(ctx, errorCode, err)
				return
			}

			record.Status = utils.StatusJobDone
			cacheStore(ctx, cacheClient, record)
			logger.InfoCtx(ctx, "status transitioned to %s", record.Status.String())

		case utils.StatusJobDone:
			record.handleJobDone(ctx, k8sClient)
			return
		default:
			logger.WarnCtx(ctx, "unexpected job status %d", record.Status.String())
			return
		}
	}

	// optional fallback
	logger.WarnCtx(ctx, "job state loop exceeded maxStep (%d)", maxStep)
}

func (record *Record) handleJobInit(ctx context.Context, k8sClient k8s.Client) (err error, errorCode string) {
	exist, err := utils.Retry(ctx, 2, 3*time.Second, func() (bool, error) {
		return k8sClient.JobExists(ctx, record.JobMessage.Job.Namespace, record.JobName)
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

	jobSpec := record.JobMessage.GetJobSpec(record.JobName)
	if err = k8sClient.JobCreate(ctx, record.JobMessage.Job.Namespace, jobSpec); err != nil {
		errorCode = callback.ERROR_CODE_JOB_CREATE_FAILED
		return
	}

	job, err := utils.Retry(ctx, 2, 2*time.Second, func() (*batchV1.Job, error) {
		return k8sClient.JobGet(ctx, record.JobMessage.Job.Namespace, record.JobName)
	})
	if err != nil {
		errorCode = callback.ERROR_CODE_JOB_GET
		return
	}

	record.Send(ctx, callback.StatusJobCreated, map[string]any{
		"jobId":   job.GetObjectMeta().GetUID(),
		"jobName": job.GetObjectMeta().GetName(),
	})
	return
}

func (record *Record) handleJobCreated(ctx context.Context, k8sClient k8s.Client) (err error, errorCode string) {
	logger.InfoCtx(ctx, "watching pod running")
	pod, err := utils.Retry(ctx, 2, 3*time.Second, func() (*coreV1.Pod, error) {
		return k8sClient.JobPodRunningWatch(ctx, record.JobMessage.Job.Namespace, record.JobName)
	})
	if err != nil {
		errorCode = callback.ERROR_CODE_JOB_POD_START_FAILED
		if errors.Is(err, k8s.ErrPodStartTimeout) {
			errorCode = callback.ERROR_CODE_JOB_POD_START_TIMEOUT
		}
		return
	}
	record.Send(ctx, callback.StatusPodRunning, map[string]any{
		"podId":   pod.GetObjectMeta().GetUID(),
		"podName": pod.GetObjectMeta().GetName(),
	})
	return
}

func (record *Record) handleJobRunning(ctx context.Context, k8sClient k8s.Client) (err error, errorCode string) {
	logger.InfoCtx(ctx, "watching job completion")
	err = k8sClient.JobCompletionWatch(ctx, record.JobMessage.Job.Namespace, record.JobName, record.JobMessage.Job.ActiveDeadlineSeconds)
	if err != nil && !errors.Is(err, k8s.ErrJobFailed) {
		errorCode = callback.ERROR_CODE_JOB_WATCH_FAILED
		return
	}
	return
}

func (record *Record) handleJobDone(ctx context.Context, k8sClient k8s.Client) {
	detail, err := utils.Retry(ctx, 2, 2*time.Second, func() (utils.JobDetail, error) {
		status, duration, err := k8sClient.JobDetailGet(ctx, record.JobMessage.Job.Namespace, record.JobName)
		return utils.JobDetail{Status: status, Duration: duration}, err
	})
	if err != nil {
		record.sendCallbackErr(ctx, callback.ERROR_CODE_JOB_GET_DETAIL_FAILED, err)
		return
	}
	// metrics.JobDuration.WithLabelValues(record.JobName).Observe(detail.Duration.Seconds())

	if detail.Status == callback.StatusJobFailed {
		failureDetail, err := utils.Retry(ctx, 2, 1*time.Second, func() (*k8s.JobFailureDetail, error) {
			return k8sClient.GetJobFailureDetail(ctx, record.JobMessage.Job.Namespace, record.JobName)
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
		record.Send(ctx, detail.Status, map[string]any{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_RUN_FAILED,
				Message:   string(jobFailureString),
			},
		})
		prom.JobFailure.Inc()
		return
	}
	logger.InfoCtx(ctx, "job succeeded, duration: %.2fs", detail.Duration.Seconds())
	record.Send(ctx, detail.Status, map[string]any{
		"duration": detail.Duration.Seconds(),
	})
	prom.JobSuccess.Inc()
}

func (record *Record) Send(ctx context.Context, status callback.Status, detail map[string]any) {
	if record.JobMessage.Webhook == nil {
		logger.InfoCtx(ctx, "no webhook configured for job %s, skipping callback", record.JobMessage.ID)
		return
	}

	logger.InfoCtx(ctx, "sending callback for job %s to URL: %s, status: %d",
		record.JobMessage.ID, record.JobMessage.Webhook.URL, status)

	callbackClient := callback.CallbackClient{
		URL: record.JobMessage.Webhook.URL,
		RequestBody: &callback.RequestBody{
			ID:     record.JobMessage.ID,
			Status: status,
			Detail: detail,
		},
	}

	resp, err := callbackClient.Post(ctx)
	if err != nil {
		logger.ErrorCtx(ctx, "callback failed for job %s to URL %s: %s",
			record.JobMessage.ID, record.JobMessage.Webhook.URL, err.Error())
		return
	}

	logger.InfoCtx(ctx, "callback sent successfully, status: %d, jobID: %s, response status: %d",
		status, record.JobMessage.ID, resp.StatusCode)
	resp.Body.Close()
}

func (record *Record) sendCallbackErr(ctx context.Context, errorCode string, err error) {
	logger.ErrorCtx(ctx, "error: %s", err.Error())
	record.Send(ctx, callback.StatusException, map[string]any{
		"error": &callback.ErrorDetail{
			ErrorCode: errorCode,
			Message:   err.Error(),
		},
	})
}

func cacheStore(ctx context.Context, client cache.Client, record *Record) {
	data, err := json.Marshal(record)
	if err != nil {
		logger.WarnCtx(ctx, "marshal error, skip caching job %s, status: %d: %v", record.JobMessage.ID, record.Status.String(), err)
		return
	}

	timeout := time.Second * time.Duration(record.JobMessage.Job.ActiveDeadlineSeconds)
	if err := client.Set(ctx, client.CacheJobKeyPrefix()+record.JobMessage.ID, data, timeout); err != nil {
		logger.WarnCtx(ctx, "cache set error for job %s, status: %d: %v", record.JobMessage.ID, record.Status.String(), err)
	}
}
