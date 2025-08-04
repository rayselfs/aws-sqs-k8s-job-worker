package worker

import (
	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/app/job"
	"aws-sqs-k8s-job-worker/internal/app/utils"
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"aws-sqs-k8s-job-worker/internal/pkg/observability/metrics"
	"aws-sqs-k8s-job-worker/internal/pkg/queue"
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type JobWorker struct {
	Queue     queue.QueueClient
	K8sClient k8s.Client
	Cache     cache.Client
	Validator *validator.Validate
	Config    *configs.Config
}

func (w *JobWorker) StartLeaderElection(ctx context.Context) {
	identity := w.Config.PodName
	lock := resourcelock.LeaseLock{
		LeaseMeta: metaV1.ObjectMeta{
			Namespace: w.Config.PodNamespace,
			Name:      w.Config.LeaderElectionLockName,
		},
		Client: w.K8sClient.Clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	electorConfig := leaderelection.LeaderElectionConfig{
		Lock:            &lock,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info("Leader acquired")
				go w.RecoverCachedJobs(ctx)
				go w.handleMessages(ctx)
			},
			OnStoppedLeading: func() {
				logger.Info("Lost leadership")
			},
			OnNewLeader: func(id string) {
				if id == identity {
					logger.Info("Current instance is the leader")
				} else {
					logger.Info("New leader elected: %s", id)
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(electorConfig)
	if err != nil {
		logger.Fatal("Failed to create leader elector: %s", err)
	}

	go elector.Run(ctx)
}

func (w *JobWorker) handleMessages(ctx context.Context) {
	jobs := make(chan types.Message, w.Config.QueueWorkerPoolSize)
	defer close(jobs)

	// Start worker pool
	for i := 0; i < int(w.Config.QueueWorkerPoolSize); i++ {
		go func() {
			defer w.recoverWorker("handleMessages-worker")
			for {
				select {
				case <-ctx.Done():
					logger.Info("Message worker exiting")
					return
				case msg := <-jobs:
					w.processSQSRecord(ctx, msg)
				}
			}
		}()
	}

	// Poll SQS messages
	for {
		select {
		case <-ctx.Done():
			logger.Info("Stopping message polling loop")
			return
		default:
			messages, err := w.Queue.GetMessages(ctx)
			if err != nil {
				logger.Error("Failed to get messages: %s", err)
				continue
			}
			metrics.QueueLength.Set(float64(len(messages)))

			for _, msg := range messages {
				select {
				case jobs <- msg:
				case <-ctx.Done():
					logger.WarnCtx(ctx, "Context cancelled, %d messages may be lost", len(messages))
					return
				default:
					logger.WarnCtx(ctx, "Job queue full, message may be lost")
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * time.Duration(w.Config.PollingInterval)):
			}
		}
	}
}

func (w *JobWorker) RecoverCachedJobs(ctx context.Context) {
	logger.Info("Recovering cached jobs from Redis")

	recoverList, err := w.Cache.ScanPrefix(ctx, w.Config.CacheJobKeyPrefix)
	if err != nil {
		logger.Error("Failed to retrieve cached jobs: %s", err)
		return
	}

	recoverJobs := make(chan string, w.Config.QueueWorkerPoolSize)

	// Start worker pool for recovered jobs
	for i := 0; i < int(w.Config.QueueWorkerPoolSize); i++ {
		go func() {
			defer w.recoverWorker("recover-jobs")
			for {
				select {
				case <-ctx.Done():
					logger.Info("Recover worker exiting")
					return
				case jobData := <-recoverJobs:
					w.processRedisRecord(ctx, jobData)
				}
			}
		}()
	}

	for _, jobData := range recoverList {
		select {
		case recoverJobs <- jobData:
		case <-ctx.Done():
			logger.Info("Stopped enqueuing cached jobs due to shutdown")
			return
		}
	}
}

func (w *JobWorker) processSQSRecord(ctx context.Context, message types.Message) {
	var jobMsg k8s.JobMessage
	if err := json.Unmarshal([]byte(*message.Body), &jobMsg); err != nil {
		logger.Error("Invalid job message: %s", err)
		metrics.MessagesFailed.Inc()
		w.Queue.DeleteMessage(ctx, message)
		return
	}

	record := job.Record{
		SQSMessage: message,
		JobMessage: jobMsg,
		Status:     utils.StatusJobInit,
	}
	w.processRecord(ctx, record, true)
}

func (w *JobWorker) processRedisRecord(ctx context.Context, data string) {
	var record job.Record
	if err := json.Unmarshal([]byte(data), &record); err != nil {
		logger.Error("Invalid cached record: %s", err)
		return
	}
	w.processRecord(ctx, record, false)
}

func (w *JobWorker) processRecord(ctx context.Context, record job.Record, fromSQS bool) {
	key := w.Config.CacheJobKeyPrefix + record.JobMessage.ID
	ctx = logger.WithTraceID(ctx, record.JobMessage.ID)

	if fromSQS {
		// Validate the job message
		if err := w.ValidateJobMessage(ctx, record.JobMessage); err != nil {
			logger.ErrorCtx(ctx, "Validation failed: %s", err.Error())
			metrics.MessagesFailed.Inc()
			w.Queue.DeleteMessage(ctx, record.SQSMessage) // Keep original position
			return
		}

		// Check for duplicate jobs using Redis cache
		if _, err := w.Cache.Get(ctx, key); err == nil {
			logger.WarnCtx(ctx, "Duplicate job detected")
			w.Queue.DeleteMessage(ctx, record.SQSMessage) // Keep original position
			metrics.MessagesFailed.Inc()
			return
		}

		// Cache the job for deduplication
		data, _ := json.Marshal(record)
		if err := w.Cache.Set(ctx, key, string(data),
			time.Second*time.Duration(record.JobMessage.Job.ActiveDeadlineSeconds)); err != nil {
			logger.ErrorCtx(ctx, "Failed to cache job: %s", err)
		}

		// Delete SQS message immediately after caching
		if err := w.Queue.DeleteMessage(ctx, record.SQSMessage); err != nil { // Keep original position
			logger.ErrorCtx(ctx, "Failed to delete SQS message: %s", err)
		}

		// Check and assign job name
		jobName, err := record.JobMessage.CheckJobName()
		if err != nil {
			logger.ErrorCtx(ctx, "Invalid job name: %s", err)
			_ = w.Cache.Delete(ctx, key) // Prevent dedup block for retry
			return
		}
		record.JobName = jobName
	}

	logger.InfoCtx(ctx, "Processing job")
	record.Execution(ctx, w.K8sClient, w.Cache)
	metrics.MessagesProcessed.Inc()

	// Always delete the cache key after execution (success or fail)
	if err := w.Cache.Delete(ctx, key); err != nil {
		logger.ErrorCtx(ctx, "Failed to delete cache key: %s", err)
	}
}

func (w *JobWorker) ValidateJobMessage(ctx context.Context, jobMsg k8s.JobMessage) error {
	if err := w.Validator.Struct(jobMsg); err != nil {
		logger.ErrorCtx(ctx, "Job message validation failed: %s", err)
		return err
	}

	if jobMsg.Job.GpuNumber != nil && (*jobMsg.Job.GpuNumber <= 0 || *jobMsg.Job.GpuNumber > 4) {
		logger.ErrorCtx(ctx, "Invalid gpuNumber: %d", *jobMsg.Job.GpuNumber)
		return fmt.Errorf("gpuNumber must be between 1 and 4")
	}

	return nil
}

func (w *JobWorker) recoverWorker(source string) {
	if r := recover(); r != nil {
		logger.Error("Worker panic in %s: %v\nStack: %s", source, r, string(debug.Stack()))
	}
}
