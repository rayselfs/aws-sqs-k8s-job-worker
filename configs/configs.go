package configs

import (
	"errors"
	"fmt"
	"time"

	"github.com/caarlos0/env/v11"
)

// Config defines all environment variables and derived config for the worker.
type Config struct {
	// Transformed time.Duration fields (not loaded from env directly)
	QueueAwsSqsWaitTimeDuration time.Duration `env:"-"` // SQS wait time (duration)
	PodStartTimeoutDuration     time.Duration `env:"-"` // Pod start timeout (duration)
	KubernetesClientDuration    time.Duration `env:"-"` // Kubernetes client timeout (duration)

	LeaderElectionLockName string `env:"LEADER_ELECTION_LOCK_NAME" envDefault:"aws-sqs-job-worker-lock"`
	PodName                string `env:"POD_NAME,required"`
	PodNamespace           string `env:"POD_NAMESPACE,required"`
	LeaderLockName         string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`
	PollingInterval        int32  `env:"POLLING_INTERVAL" envDefault:"5"`

	CacheRedisEndpoint string `env:"CACHE_REDIS_ENDPOINT,required"`
	CacheRedisDB       int    `env:"CACHE_REDIS_DB,required"`
	CacheJobKeyPrefix  string `env:"CACHE_JOB_KEY_PREFIX" envDefault:"job-worker-"`

	QueueType                  string `env:"QUEUE_TYPE" envDefault:"redis"`
	QueueWorkerPoolSize        int    `env:"QUEUE_WORKER_POOL_SIZE" envDefault:"10"`
	QueueAwsSqsRegion          string `env:"QUEUE_AWS_SQS_REGION"`
	QueueAwsSqsUrl             string `env:"QUEUE_AWS_SQS_URL"`
	QueueAwsSqsWaitTimeSeconds int32  `env:"QUEUE_AWS_SQS_WAIT_TIME_SECONDS" envDefault:"20"`
	QueueRedisEndpoint         string `env:"REDIS_QUEUE_ENDPOINT"`
	QueueRedisKeyPrefix        string `env:"REDIS_QUEUE_KEY_PREFIX" envDefault:"queue-"`
	QueueRedisDB               int    `env:"REDIS_QUEUE_DB" envDefault:"0"`

	KubernetesClientTimeout int `env:"KUBERNETES_CLIENT_TIMEOUT" envDefault:"30"`
	PodStartTimeout         int `env:"POD_START_TIMEOUT" envDefault:"600"`
}

// Parse loads configuration from environment variables, validates and normalizes it.
func Parse() (*Config, error) {
	var cfg Config

	// 1. 解析環境變數
	if err := env.Parse(&cfg); err != nil {
		return nil, fmt.Errorf("failed to parse env: %w", err)
	}

	// 2. 驗證
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	// 3. 計算衍生欄位
	cfg.normalize()

	return &cfg, nil
}

// validate performs all required configuration checks.
func (c *Config) validate() error {
	if c.PodStartTimeout <= 0 {
		return errors.New("POD_START_TIMEOUT must be greater than 0")
	}

	if c.QueueType != "redis" && c.QueueType != "sqs" {
		return errors.New("QUEUE_TYPE must be 'redis' or 'sqs'")
	}

	if c.QueueWorkerPoolSize <= 0 || c.QueueWorkerPoolSize > 10 {
		return errors.New("QUEUE_WORKER_POOL_SIZE must be between 1 and 10")
	}

	if c.QueueType == "sqs" {
		if c.QueueAwsSqsRegion == "" {
			return errors.New("QUEUE_AWS_SQS_REGION is required for SQS queue type")
		}
		if c.QueueAwsSqsUrl == "" {
			return errors.New("QUEUE_AWS_SQS_URL is required for SQS queue type")
		}
	}

	if c.QueueType == "redis" {
		if c.QueueRedisEndpoint == "" {
			return errors.New("REDIS_QUEUE_ENDPOINT is required for Redis queue type")
		}
	}

	return nil
}

// normalize converts int values to duration and sets derived fields.
func (c *Config) normalize() {
	c.QueueAwsSqsWaitTimeDuration = time.Duration(c.QueueAwsSqsWaitTimeSeconds) * time.Second
	c.PodStartTimeoutDuration = time.Duration(c.PodStartTimeout) * time.Second
	c.KubernetesClientDuration = time.Duration(c.KubernetesClientTimeout) * time.Second
}
