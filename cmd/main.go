package main

import (
	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/app/service/job"
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	redisCache "aws-sqs-k8s-job-worker/internal/pkg/cache/redis"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"aws-sqs-k8s-job-worker/internal/pkg/queue"
	redisQueue "aws-sqs-k8s-job-worker/internal/pkg/queue/redis"
	"aws-sqs-k8s-job-worker/internal/pkg/queue/sqs"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	prom "aws-sqs-k8s-job-worker/internal/pkg/prometheus"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/leaderelection"
)

var (
	Queue       queue.QueueClient
	healthy     int32 = 1 // Atomic flag for health status (1 = healthy, 0 = unhealthy)
	CacheClient cache.Client
)

func main() {
	var err error
	if err := logger.Setup(); err != nil {
		panic(fmt.Sprintf("unable to initialize logger: %v", err))
	}

	if err := config.Setup(); err != nil {
		logger.Fatal("unable to set config", zap.Error(err))
	}

	// 決定使用哪個 queue backend
	switch config.Env.QueueType {
	case "redis":
		Queue = redisQueue.New(config.Env.RedisEndpoint, config.Env.RedisJobKeyPrefix, config.Env.RedisDB)
		logger.Info("Using Redis queue")
	case "sqs":
		Queue = sqs.New(config.Env.AWSSQSRegion, config.Env.AWSSQSURL)
		logger.Info("Using AWS SQS queue")
	default:
		logger.Fatal("QUEUE_TYPE must be either 'redis' or 'sqs'", zap.String("QUEUE_TYPE", config.Env.QueueType))
	}

	CacheClient = redisCache.New(config.Env.RedisEndpoint, config.Env.RedisDB)

	if err := k8s.Setup(); err != nil {
		logger.Fatal("unable to set k8s", zap.Error(err))
	}

	prom.Setup()

	http.HandleFunc("/healthz", healthHandler)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logger.Info("Starting health check server", zap.String("port", "8080"))
		log.Fatal(http.ListenAndServe(":8080", nil))
	}()

	id := config.Env.PodName
	lock := k8s.GetLeaseLock(id)

	leaderElectorConfig := leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info("started leading")
				handleRecords()
				handleMessages()
			},
			OnStoppedLeading: func() {
				logger.Info("stopped leading")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					logger.Info("current New leader")
				} else {
					logger.Info("new leader elected", zap.String("identity", identity))
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderElectorConfig)
	if err != nil {
		logger.Fatal("error creating leader elector", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go elector.Run(ctx)

	// Wait indefinitely
	select {}
}

// Health check endpoint
func healthHandler(w http.ResponseWriter, r *http.Request) {
	if atomic.LoadInt32(&healthy) == 1 {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Service Unhealthy"))
	}
}

// handleMessages handle messages in SQS
func handleMessages() {
	logger.Info("start queue polling")
	for {
		messages, err := Queue.GetMessages()
		if err != nil {
			logger.Fatal("unable to get messages from queue", zap.Error(err))
		}
		if len(messages) > 0 {
			message := messages[0]
			go messageProcess(message)
		}
		time.Sleep(time.Second * time.Duration(config.Env.PollingInterval))
	}
}

// handleRecords handle records in redis
func handleRecords() {
	rdbList, err := CacheClient.GetByPrefix(config.Env.RedisJobKeyPrefix)
	if err != nil {
		logger.Fatal("unable to get list in redis", zap.Error(err))
	}

	logger.Info("start record process")
	for _, data := range rdbList {
		go recordProcess(data)
	}
}

// messageProcess process message from SQS
func messageProcess(message types.Message) {
	start := time.Now()

	defer func() {
		duration := time.Since(start).Seconds()
		prom.MessageProcessingTime.Observe(duration)
	}()

	logger.Info("message received", zap.String("id", *message.MessageId))
	logger.Info("message body", zap.String("body", *message.Body))

	var jobMsg k8s.JobMessage
	if err := json.Unmarshal([]byte(*message.Body), &jobMsg); err != nil {
		logger.Error("failed to unmarshal job message", zap.Error(err))
		prom.MessagesFailed.Inc()
		Queue.DeleteMessage(message)
		return
	}

	validator := validator.New()
	if err := validator.Struct(jobMsg); err != nil {
		logger.Error("job message validation failed", zap.Error(err))
		prom.MessagesFailed.Inc()
		Queue.DeleteMessage(message)
		return
	}

	if _, err := CacheClient.Get(config.Env.RedisJobKeyPrefix + jobMsg.ID); err == nil {
		logger.Error("job has been executed", zap.String("jobID", jobMsg.ID))
		Queue.DeleteMessage(message)
		prom.MessagesFailed.Inc()
		return
	}

	jobMsg.Job.BackoffLimit = 0
	if jobMsg.Job.TTLSecondsAfterFinished == 0 {
		jobMsg.Job.TTLSecondsAfterFinished = 60
	}

	record := job.Record{
		SQSMessage: message,
		JobMessage: jobMsg,
		Status:     job.StatusInit,
	}
	recordData, _ := json.Marshal(record)
	CacheClient.Set(config.Env.RedisJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))

	Queue.DeleteMessage(message)

	logger.Info("start message process")
	job.Execution(record, CacheClient)

	prom.MessagesProcessed.Inc()

	if err := CacheClient.Delete(config.Env.RedisJobKeyPrefix + jobMsg.ID); err != nil {
		logger.Error("unable to delete job from redis", zap.String("jobID", jobMsg.ID), zap.Error(err))
	}
}

// recordProcess process message from redis
func recordProcess(recordData string) {
	var record job.Record
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		logger.Error("failed to unmarshal record", zap.Error(err))
		return
	}

	logger.Info("Record received", zap.String("jobID", record.JobMessage.ID))
	job.Execution(record, CacheClient)

	if err := CacheClient.Delete(config.Env.RedisJobKeyPrefix + record.JobMessage.ID); err != nil {
		logger.Error("unable to delete job from redis", zap.String("jobID", record.JobMessage.ID), zap.Error(err))
	}
}
