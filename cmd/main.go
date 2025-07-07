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
	"net/http"
	"os"
	"time"

	prom "aws-sqs-k8s-job-worker/internal/pkg/prometheus"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/tools/leaderelection"
)

var (
	Queue       queue.QueueClient
	CacheClient cache.Client
	validate    *validator.Validate
)

func main() {
	var err error
	// Initialize logger
	if err := logger.Setup(); err != nil {
		panic(fmt.Sprintf("unable to initialize logger: %s", err))
	}

	// Initialize config
	if err := config.Setup(); err != nil {
		logger.Fatal("unable to set config: %s", err.Error())
	}

	// Initialize k8s client
	if err := k8s.Setup(); err != nil {
		logger.Fatal("unable to set k8s, error: %s", err.Error())
	}

	// Initialize queue
	switch config.Env.QueueType {
	case "redis":
		Queue = redisQueue.New(config.Env.QueueRedisEndpoint, config.Env.QueueRedisKeyPrefix, config.Env.QueueRedisDB)
		logger.Info("Using Redis queue")
	case "sqs":
		if Queue, err = sqs.New(config.Env.QueueAwsSqs, config.Env.QueueAwsSqsUrl); err != nil {
			logger.Fatal("unable to initialize SQS queue, error: %s", err.Error())
		}
		logger.Info("Using AWS SQS queue")
	}

	// Initialize cache
	CacheClient = redisCache.New(config.Env.CacheRedisEndpoint, config.Env.CacheRedisDB)

	// Initialize Prometheus metrics
	prom.Setup()

	// Initialize validator
	validate = validator.New()

	// Start http server
	http.HandleFunc("/healthz", healthHandler)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logger.Info("Starting health check server, listening on :8080")
		http.ListenAndServe(":8080", nil)
	}()

	// Start leader election
	electorLockIdentity := config.Env.PodName
	lock := k8s.GetLeaseLock(electorLockIdentity)
	leaderElectorConfig := leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info("started leading")
				go handleRecords()
				go handleMessages()
			},
			OnStoppedLeading: func() {
				logger.Info("stopped leading")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == electorLockIdentity {
					logger.Info("current New leader")
				} else {
					logger.Info("new leader elected, identity: %s", identity)
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderElectorConfig)
	if err != nil {
		logger.Fatal("error creating leader elector, error: %s", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go elector.Run(ctx)

	// Wait indefinitely
	select {}
}

// Health check endpoint
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleMessages handle messages in SQS
func handleMessages() {
	logger.Info("start queue polling")
	jobs := make(chan types.Message, config.Env.QueueWorkerPoolSize)
	for i := 0; i < int(config.Env.QueueWorkerPoolSize); i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("worker panic recovered, error: %v", r)
				}
			}()
			for message := range jobs {
				messageProcess(message)
			}
		}()
	}

	for {
		messages, err := Queue.GetMessages()
		if err != nil {
			logger.Error("unable to get messages from queue, error: %s", err.Error())
		}
		if len(messages) > 0 {
			for _, message := range messages {
				jobs <- message
			}
		}
		time.Sleep(time.Second * time.Duration(config.Env.PollingInterval))
	}
}

// handleRecords handle records in redis
func handleRecords() {
	rdbList, err := CacheClient.GetByPrefix(config.Env.CacheJobKeyPrefix)
	if err != nil {
		logger.Error("unable to get list in redis, error: %s", err.Error())
		return
	}

	logger.Info("start record process")
	records := make(chan string, config.Env.QueueWorkerPoolSize)
	for i := 0; i < int(config.Env.QueueWorkerPoolSize); i++ {
		go func() {
			defer func() {
				if r := recover(); r != nil {
					logger.Error("worker panic recovered, error: %v", r)
				}
			}()
			for data := range records {
				recordProcess(data)
			}
		}()
	}
	for _, data := range rdbList {
		records <- data
	}
}

// messageProcess process message from SQS
func messageProcess(message types.Message) {
	var jobMsg k8s.JobMessage
	if err := json.Unmarshal([]byte(*message.Body), &jobMsg); err != nil {
		logger.Error("failed to unmarshal job message, error: %s", err.Error())
		prom.MessagesFailed.Inc()
		Queue.DeleteMessage(message)
		return
	}

	record := job.Record{
		SQSMessage: message,
		JobMessage: jobMsg,
		Status:     job.StatusJobInit,
	}
	processRecord(record, true)
}

// recordProcess process message from redis
func recordProcess(recordData string) {
	var record job.Record
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		logger.Error("failed to unmarshal record, error: %v", err)
		return
	}
	processRecord(record, false)
}

// processRecord 處理共用邏輯
func processRecord(record job.Record, fromSQS bool) {
	start := time.Now()
	defer func() {
		duration := time.Since(start).Seconds()
		prom.MessageProcessingTime.Observe(duration)
	}()

	logCtx := logger.WithTraceID(context.Background(), record.JobMessage.ID)
	jobMsg := record.JobMessage

	if fromSQS {
		logger.Info("message id: %s", *record.SQSMessage.MessageId)
		logger.Info("message body: %s", *record.SQSMessage.Body)
		if err := validate.Struct(jobMsg); err != nil {
			logger.Error("job message validation failed, error: %s", err.Error())
			prom.MessagesFailed.Inc()
			Queue.DeleteMessage(record.SQSMessage)
			return
		}
		if _, err := CacheClient.Get(config.Env.CacheJobKeyPrefix + jobMsg.ID); err == nil {
			logger.Error("job has been executed")
			Queue.DeleteMessage(record.SQSMessage)
			prom.MessagesFailed.Inc()
			return
		}
		recordData, _ := json.Marshal(record)
		CacheClient.Set(config.Env.CacheJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(jobMsg.Job.ActiveDeadlineSeconds))
		Queue.DeleteMessage(record.SQSMessage)
	}

	logger.InfoCtx(logCtx, "start message process")
	job.Execution(record, CacheClient, logCtx)
	prom.MessagesProcessed.Inc()
	if err := CacheClient.Delete(config.Env.CacheJobKeyPrefix + jobMsg.ID); err != nil {
		logger.ErrorCtx(logCtx, "unable to delete job from redis")
	}
}
