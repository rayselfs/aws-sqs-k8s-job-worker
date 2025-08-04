package main

import (
	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/app/http"
	"aws-sqs-k8s-job-worker/internal/app/worker"
	"aws-sqs-k8s-job-worker/internal/pkg/cache"
	redisCache "aws-sqs-k8s-job-worker/internal/pkg/cache/redis"
	"aws-sqs-k8s-job-worker/internal/pkg/k8s"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"aws-sqs-k8s-job-worker/internal/pkg/observability/metrics"
	"aws-sqs-k8s-job-worker/internal/pkg/queue"
	redisQueue "aws-sqs-k8s-job-worker/internal/pkg/queue/redis"
	"aws-sqs-k8s-job-worker/internal/pkg/queue/sqs"
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

	cfg, err := configs.Parse()
	if err != nil {
		logger.Fatal("config setup failed: %s", err.Error())
	}

	clientset, err := k8s.New()
	if err != nil {
		logger.ErrorCtx(ctx, "Failed to create Kubernetes client: %s", err)
		return
	}
	k8sClient := k8s.Client{
		Clientset: clientset,
		Config: &k8s.Config{
			PodStartTimeout: cfg.PodStartTimeoutDuration,
			ClientTimeout:   cfg.KubernetesClientDuration,
		},
	}

	queueClient, err := initQueue(ctx, cfg)
	if err != nil {
		logger.Fatal("queue init failed: %s", err)
	}

	worker := &worker.JobWorker{
		Queue:     queueClient,
		K8sClient: k8sClient,
		Cache:     initCache(cfg),
		Validator: initValidator(),
		Config:    cfg,
	}

	metrics.Setup()

	httpConfig := &http.HttpConfig{
		Port:    cfg.HTTPServerPort,
		Timeout: cfg.HTTPServerTimeout,
	}
	go httpConfig.StartHTTPServer(ctx)

	worker.StartLeaderElection(ctx)

	<-ctx.Done()
	logger.Info("Shutting down gracefully...")
}

func initQueue(ctx context.Context, cfg *configs.Config) (queue.QueueClient, error) {
	switch cfg.QueueType {
	case "redis":
		logger.Info("Using Redis queue")
		client := redisQueue.New(cfg.QueueRedisEndpoint, cfg.QueueRedisDB)
		return &redisQueue.RedisActions{
			Client: client,
			Config: &redisQueue.Config{
				WorkerPoolSize: cfg.QueueWorkerPoolSize,
				Key:            cfg.QueueRedisKeyPrefix,
			},
		}, nil
	case "sqs":
		logger.Info("Using AWS SQS queue")
		client, err := sqs.NewClient(ctx, cfg.QueueAwsSqsRegion, cfg.QueueAwsSqsUrl)
		if err != nil {
			return nil, fmt.Errorf("unable to initialize SQS: %w", err)
		}
		return &sqs.SqsActions{
			SqsClient: client,
			Config: &sqs.Config{
				WorkerPoolSize:  cfg.QueueWorkerPoolSize,
				QueueUrl:        cfg.QueueAwsSqsUrl,
				WaitTimeSeconds: cfg.QueueAwsSqsWaitTimeSeconds,
			},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported queue type: %s", cfg.QueueType)
	}
}

func initCache(cfg *configs.Config) cache.Client {
	client := redisCache.NewClient(cfg.CacheRedisEndpoint, cfg.CacheRedisDB)
	return &redisCache.RedisRepository{Client: client, Config: &redisCache.Config{
		CacheJobKeyPrefix: cfg.CacheJobKeyPrefix,
	}}
}

func initValidator() *validator.Validate {
	return validator.New()
}
