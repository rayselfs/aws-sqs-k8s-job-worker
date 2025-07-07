package config

import (
	"errors"

	"github.com/caarlos0/env/v11"
)

func Setup() error {
	err := env.Parse(&Env)
	if err != nil {
		return err
	}

	if Env.PodStartTimeout <= 0 {
		return errors.New("PodRunningTimeout must be greater than 0")
	}

	return nil
}

var Env EnvVariable

type EnvVariable struct {
	LeaderElectionLockName string `env:"LEADER_ELECTION_LOCK_NAME" envDefault:"aws-sqs-job-worker-lock"`
	PodName                string `env:"POD_NAME,required"`
	PodNamespace           string `env:"POD_NAMESPACE,required"`
	LeaderLockName         string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`
	PollingInterval        int32  `env:"POLLING_INTERVAL" envDefault:"5"`
	CacheRedisEndpoint     string `env:"CACHE_REDIS_ENDPOINT,required"`
	CacheRedisDB           int    `env:"CACHE_REDIS_DB,required"`
	CacheJobKeyPrefix      string `env:"CACHE_JOB_KEY_PREFIX" envDefault:"job-worker-"`

	QueueType           string `env:"QUEUE_TYPE" envDefault:"redis"`
	QueueAwsSqs         string `env:"QUEUE_AWS_SQS_REGION"`
	QueueAwsSqsUrl      string `env:"QUEUE_AWS_SQS_URL"`
	QueueRedisEndpoint  string `env:"REDIS_QUEUE_ENDPOINT"`
	QueueRedisKeyPrefix string `env:"REDIS_QUEUE_KEY_PREFIX" envDefault:"queue-"`
	QueueRedisDB        int    `env:"REDIS_QUEUE_DB" envDefault:"0"`

	CallbackMaxRetries   int `env:"CALLBACK_MAX_RETRIES" envDefault:"10"`
	CallbackBaseDelay    int `env:"CALLBACK_BASE_DELAY" envDefault:"1"`     // seconds
	CallbackMaxDelay     int `env:"CALLBACK_MAX_DELAY" envDefault:"30"`     // seconds
	CallbackTotalTimeout int `env:"CALLBACK_TOTAL_TIMEOUT" envDefault:"60"` // seconds
	WorkerPoolSize       int `env:"WORKER_POOL_SIZE" envDefault:"10"`       // worker pool size
	PodStartTimeout      int `env:"POD_START_TIMEOUT" envDefault:"600"`     // 單位: 秒，預設 10 分鐘
}
