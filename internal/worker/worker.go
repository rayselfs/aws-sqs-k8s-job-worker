package worker

import (
	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/cache"
	"aws-sqs-k8s-job-worker/internal/job"
	"aws-sqs-k8s-job-worker/internal/k8s"
	"aws-sqs-k8s-job-worker/internal/logger"
	"aws-sqs-k8s-job-worker/internal/metrics"
	"aws-sqs-k8s-job-worker/internal/queue"
	"aws-sqs-k8s-job-worker/internal/utils"
	"context"
	"encoding/json"
	"runtime/debug"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	"k8s.io/client-go/tools/leaderelection"
)

type JobWorker struct {
	Queue     queue.QueueClient
	Cache     cache.Client
	Validator *validator.Validate
}

func (w *JobWorker) StartLeaderElection(ctx context.Context) {
	identity := configs.Env.PodName
	lock := k8s.GetLeaseLock(identity)

	electorConfig := leaderelection.LeaderElectionConfig{
		Lock:            lock,
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
	jobs := make(chan types.Message, configs.Env.QueueWorkerPoolSize)

	for i := 0; i < int(configs.Env.QueueWorkerPoolSize); i++ {
		go func() {
			defer w.recoverWorker("handleMessages")
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
					return
				}
			}

			// ✅ 改這裡：可以被 ctx.Cancel() 終止的 sleep
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * time.Duration(configs.Env.PollingInterval)):
			}
		}
	}
}

func (w *JobWorker) RecoverCachedJobs(ctx context.Context) {
	logger.Info("Recovering cached jobs from Redis")

	recoverList, err := w.Cache.ScanPrefix(ctx, configs.Env.CacheJobKeyPrefix)
	if err != nil {
		logger.Error("Failed to retrieve cached jobs: %s", err)
		return
	}

	recoverJobs := make(chan string, configs.Env.QueueWorkerPoolSize)

	for i := 0; i < int(configs.Env.QueueWorkerPoolSize); i++ {
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
	key := configs.Env.CacheJobKeyPrefix + record.JobMessage.ID
	ctx = logger.WithTraceID(ctx, record.JobMessage.ID)

	if fromSQS {
		if err := w.Validator.Struct(record.JobMessage); err != nil {
			logger.ErrorCtx(ctx, "Validation failed: %s", err)
			metrics.MessagesFailed.Inc()
			w.Queue.DeleteMessage(ctx, record.SQSMessage)
			return
		}

		if _, err := w.Cache.Get(ctx, key); err == nil {
			logger.WarnCtx(ctx, "Duplicate job detected")
			w.Queue.DeleteMessage(ctx, record.SQSMessage)
			metrics.MessagesFailed.Inc()
			return
		}

		data, _ := json.Marshal(record)
		if err := w.Cache.Set(ctx, key, string(data), time.Second*time.Duration(record.JobMessage.Job.ActiveDeadlineSeconds)); err != nil {
			logger.ErrorCtx(ctx, "Failed to cache job: %s", err)
		}

		if err := w.Queue.DeleteMessage(ctx, record.SQSMessage); err != nil {
			logger.ErrorCtx(ctx, "Failed to delete SQS message: %s", err)
		}

		// jobMsg := record.JobMessage
		jobName, err := record.JobMessage.CheckJobName()
		if err != nil {
			logger.ErrorCtx(ctx, "Invalid job name: %s", err)
			return
		}
		record.JobName = jobName
	}

	logger.InfoCtx(ctx, "Processing job")
	record.Execution(ctx, w.Cache)
	metrics.MessagesProcessed.Inc()
	if err := w.Cache.Delete(ctx, key); err != nil {
		logger.ErrorCtx(ctx, "Failed to delete cache key: %s", err)
	}
}

func (w *JobWorker) recoverWorker(source string) {
	if r := recover(); r != nil {
		logger.Error("Worker panic in %s: %v\nStack: %s", source, r, string(debug.Stack()))
	}
}
