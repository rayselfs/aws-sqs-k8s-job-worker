package config

import (
	"errors"
	"time"

	"github.com/caarlos0/env/v11"
)

// Setup parses environment variables and initializes the Env config.
// Returns an error if required variables are missing or invalid.
func Setup() error {
	err := env.Parse(&Env)
	if err != nil {
		return err
	}

	if Env.PodStartTimeout <= 0 {
		return errors.New("PodRunningTimeout must be greater than 0")
	}

	if Env.QueueType != "redis" && Env.QueueType != "sqs" {
		return errors.New("QUEUE_TYPE must be 'redis' or 'sqs'")
	}

	if Env.QueueWorkerPoolSize <= 0 || Env.QueueWorkerPoolSize > 10 {
		return errors.New("QUEUE_WORKER_POOL_SIZE must be between 1 and 10")
	}

	Env.CallbackBaseDelayDuration = time.Duration(Env.CallbackBaseDelay) * time.Second
	Env.CallbackMaxDelayDuration = time.Duration(Env.CallbackMaxDelay) * time.Second
	Env.CallbackTotalTimeoutDuration = time.Duration(Env.CallbackTotalTimeout) * time.Second
	Env.PodStartTimeoutDuration = time.Duration(Env.PodStartTimeout) * time.Second
	Env.KubernetesClientDuration = time.Duration(Env.KubernetesClientTimeout) * time.Second

	return nil
}

// Env holds the global configuration loaded from environment variables.
var Env EnvVariable

// EnvVariable defines all environment variables and derived config for the worker.
type EnvVariable struct {
	// Transformed time.Duration fields (not loaded from env directly)
	CallbackBaseDelayDuration     time.Duration `env:"-"` // Callback base delay (duration)
	CallbackMaxDelayDuration      time.Duration `env:"-"` // Callback max delay (duration)
	CallbackTotalTimeoutDuration  time.Duration `env:"-"` // Callback total timeout (duration)
	QueueRedisWaitTimeoutDuration time.Duration `env:"-"` // Redis queue wait timeout (duration)
	QueueAwsSqsWaitTimeDuration   time.Duration `env:"-"` // SQS wait time (duration)
	PodStartTimeoutDuration       time.Duration `env:"-"` // Pod start timeout (duration)
	KubernetesClientDuration      time.Duration `env:"-"` // Kubernetes client timeout (duration)

	LeaderElectionLockName string `env:"LEADER_ELECTION_LOCK_NAME" envDefault:"aws-sqs-job-worker-lock"` // Name for leader election lock
	PodName                string `env:"POD_NAME,required"`                                              // Current pod name
	PodNamespace           string `env:"POD_NAMESPACE,required"`                                         // Current pod namespace
	LeaderLockName         string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`                       // Lock name for leader election
	PollingInterval        int32  `env:"POLLING_INTERVAL" envDefault:"5"`                                // Queue polling interval (seconds)
	CacheRedisEndpoint     string `env:"CACHE_REDIS_ENDPOINT,required"`                                  // Redis endpoint for cache
	CacheRedisDB           int    `env:"CACHE_REDIS_DB,required"`                                        // Redis DB index for cache
	CacheJobKeyPrefix      string `env:"CACHE_JOB_KEY_PREFIX" envDefault:"job-worker-"`                  // Prefix for job keys in cache

	QueueType                  string `env:"QUEUE_TYPE" envDefault:"redis"`                   // Queue type: redis or sqs
	QueueWorkerPoolSize        int32  `env:"QUEUE_WORKER_POOL_SIZE" envDefault:"10"`          // Worker pool size
	QueueAwsSqsRegion          string `env:"QUEUE_AWS_SQS_REGION"`                            // AWS SQS region
	QueueAwsSqsUrl             string `env:"QUEUE_AWS_SQS_URL"`                               // AWS SQS queue URL
	QueueAwsSqsWaitTimeSeconds int32  `env:"QUEUE_AWS_SQS_WAIT_TIME_SECONDS" envDefault:"20"` // SQS long polling wait time (seconds)
	QueueRedisEndpoint         string `env:"REDIS_QUEUE_ENDPOINT"`                            // Redis endpoint for queue
	QueueRedisKeyPrefix        string `env:"REDIS_QUEUE_KEY_PREFIX" envDefault:"queue-"`      // Prefix for queue keys in Redis
	QueueRedisDB               int    `env:"REDIS_QUEUE_DB" envDefault:"0"`                   // Redis DB index for queue

	KubernetesClientTimeout int `env:"KUBERNETES_CLIENT_TIMEOUT" envDefault:"30"` // Kubernetes client timeout (seconds)
	CallbackMaxRetries      int `env:"CALLBACK_MAX_RETRIES" envDefault:"10"`      // Max callback retries
	CallbackBaseDelay       int `env:"CALLBACK_BASE_DELAY" envDefault:"1"`        // Callback base delay (seconds)
	CallbackMaxDelay        int `env:"CALLBACK_MAX_DELAY" envDefault:"30"`        // Callback max delay (seconds)
	CallbackTotalTimeout    int `env:"CALLBACK_TOTAL_TIMEOUT" envDefault:"60"`    // Callback total timeout (seconds)
	PodStartTimeout         int `env:"POD_START_TIMEOUT" envDefault:"600"`        // Pod start timeout (seconds)
}
