package configs

import (
	"errors"
	"time"

	"github.com/caarlos0/env/v11"
)

// EnvVariable defines all environment variables and derived config for the worker.
type Config struct {
	// Transformed time.Duration fields (not loaded from env directly)
	QueueAwsSqsWaitTimeDuration time.Duration `env:"-"` // SQS wait time (duration)
	PodStartTimeoutDuration     time.Duration `env:"-"` // Pod start timeout (duration)
	KubernetesClientDuration    time.Duration `env:"-"` // Kubernetes client timeout (duration)

	LeaderElectionLockName string `env:"LEADER_ELECTION_LOCK_NAME" envDefault:"aws-sqs-job-worker-lock"` // Name for leader election lock
	PodName                string `env:"POD_NAME,required"`                                              // Current pod name
	PodNamespace           string `env:"POD_NAMESPACE,required"`                                         // Current pod namespace
	LeaderLockName         string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`                       // Lock name for leader election
	PollingInterval        int32  `env:"POLLING_INTERVAL" envDefault:"5"`                                // Queue polling interval (seconds)
	CacheRedisEndpoint     string `env:"CACHE_REDIS_ENDPOINT,required"`                                  // Redis endpoint for cache
	CacheRedisDB           int    `env:"CACHE_REDIS_DB,required"`                                        // Redis DB index for cache
	CacheJobKeyPrefix      string `env:"CACHE_JOB_KEY_PREFIX" envDefault:"job-worker-"`                  // Prefix for job keys in cache

	QueueType                  string `env:"QUEUE_TYPE" envDefault:"redis"`                   // Queue type: redis or sqs
	QueueWorkerPoolSize        int    `env:"QUEUE_WORKER_POOL_SIZE" envDefault:"10"`          // Worker pool size
	QueueAwsSqsRegion          string `env:"QUEUE_AWS_SQS_REGION"`                            // AWS SQS region
	QueueAwsSqsUrl             string `env:"QUEUE_AWS_SQS_URL"`                               // AWS SQS queue URL
	QueueAwsSqsWaitTimeSeconds int32  `env:"QUEUE_AWS_SQS_WAIT_TIME_SECONDS" envDefault:"20"` // SQS long polling wait time (seconds)
	QueueRedisEndpoint         string `env:"REDIS_QUEUE_ENDPOINT"`                            // Redis endpoint for queue
	QueueRedisKeyPrefix        string `env:"REDIS_QUEUE_KEY_PREFIX" envDefault:"queue-"`      // Prefix for queue keys in Redis
	QueueRedisDB               int    `env:"REDIS_QUEUE_DB" envDefault:"0"`                   // Redis DB index for queue

	KubernetesClientTimeout int `env:"KUBERNETES_CLIENT_TIMEOUT" envDefault:"30"` // Kubernetes client timeout (seconds)
	PodStartTimeout         int `env:"POD_START_TIMEOUT" envDefault:"600"`        // Pod start timeout (seconds)
}

// Parse parses environment variables and initializes the Variable config.
// Returns an error if required variables are missing or invalid.
func Parse() (*Config, error) {
	var cfg Config

	err := env.Parse(&cfg)
	if err != nil {
		return nil, err
	}

	if cfg.PodStartTimeout <= 0 {
		return nil, errors.New("PodRunningTimeout must be greater than 0")
	}

	if cfg.QueueType != "redis" && cfg.QueueType != "sqs" {
		return nil, errors.New("QUEUE_TYPE must be 'redis' or 'sqs'")
	}

	if cfg.QueueWorkerPoolSize <= 0 || cfg.QueueWorkerPoolSize > 10 {
		return nil, errors.New("QUEUE_WORKER_POOL_SIZE must be between 1 and 10")
	}

	cfg.QueueAwsSqsWaitTimeDuration = time.Duration(cfg.QueueAwsSqsWaitTimeSeconds) * time.Second
	cfg.PodStartTimeoutDuration = time.Duration(cfg.PodStartTimeout) * time.Second
	cfg.KubernetesClientDuration = time.Duration(cfg.KubernetesClientTimeout) * time.Second

	return &cfg, nil
}
