package main

import (
	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/cache"
	redisCache "aws-sqs-k8s-job-worker/internal/cache/redis"
	"aws-sqs-k8s-job-worker/internal/k8s"
	"aws-sqs-k8s-job-worker/internal/logger"
	"aws-sqs-k8s-job-worker/internal/metrics"
	"aws-sqs-k8s-job-worker/internal/queue"
	redisQueue "aws-sqs-k8s-job-worker/internal/queue/redis"
	"aws-sqs-k8s-job-worker/internal/queue/sqs"
	"aws-sqs-k8s-job-worker/internal/server"
	"aws-sqs-k8s-job-worker/internal/worker"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-playground/validator/v10"
)

// main.go
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// handle SIGINT, SIGTERM
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		cancel()
	}()

	if err := logger.Setup(); err != nil {
		panic(fmt.Sprintf("logger setup failed: %s", err))
	}

	if err := configs.Setup(); err != nil {
		logger.Fatal("config setup failed: %s", err.Error())
	}

	if err := k8s.Setup(); err != nil {
		logger.Fatal("k8s setup failed: %s", err.Error())
	}

	queueClient, err := initQueue(ctx)
	if err != nil {
		logger.Fatal("queue init failed: %s", err)
	}

	worker := &worker.JobWorker{
		Queue:     queueClient,
		Cache:     initCache(),
		Validator: initValidator(),
	}

	metrics.Setup()

	server.StartHTTPServer(ctx)

	worker.StartLeaderElection(ctx)

	<-ctx.Done()
	logger.Info("Shutting down gracefully...")
}

func initQueue(ctx context.Context) (queue.QueueClient, error) {
	switch configs.Env.QueueType {
	case "redis":
		logger.Info("Using Redis queue")
		return redisQueue.New(configs.Env.QueueRedisEndpoint, configs.Env.QueueRedisKeyPrefix, configs.Env.QueueRedisDB), nil
	case "sqs":
		q, err := sqs.New(ctx, configs.Env.QueueAwsSqsRegion, configs.Env.QueueAwsSqsUrl)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize SQS: %w", err)
		}
		logger.Info("Using AWS SQS queue")
		return q, nil
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", configs.Env.QueueType)
	}
}

func initCache() cache.Client {
	return redisCache.New(configs.Env.CacheRedisEndpoint, configs.Env.CacheRedisDB)
}

func initValidator() *validator.Validate {
	return validator.New()
}
