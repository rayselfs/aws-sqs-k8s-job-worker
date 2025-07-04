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

	if Env.PodRunningTimeout <= 0 {
		return errors.New("PodRunningTimeout must be greater than 0")
	}

	return nil
}

var Env EnvVariable

type EnvVariable struct {
	QueueType                string `env:"QUEUE_TYPE" envDefault:"redis"`
	LeaderElectionLockName   string `env:"LEADER_ELECTION_LOCK_NAME" envDefault:"aws-sqs-job-worker-lock"`
	PodName                  string `env:"POD_NAME,required"`
	PodNamespace             string `env:"POD_NAMESPACE,required"`
	LeaderLockName           string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`
	PollingInterval          int32  `env:"POLLING_INTERVAL" envDefault:"5"`
	RedisEndpoint            string `env:"REDIS_ENDPOINT,required"`
	RedisDB                  int    `env:"REDIS_DB,required"`
	RedisJobKeyPrefix        string `env:"REDIS_JOB_KEY_PREFIX" envDefault:"job-worker-"`
	AWSSQSRegion             string `env:"AWS_SQS_REGION,required"`
	AWSSQSURL                string `env:"AWS_SQS_URL,required"`
	ActiveDeadlineSecondsMax int64  `env:"ACTIVE_DEADLINE_SECONDS_MAX" envDefault:"86400"`
	CallbackMaxRetries       int    `env:"CALLBACK_MAX_RETRIES" envDefault:"10"`
	CallbackBaseDelay        int    `env:"CALLBACK_BASE_DELAY" envDefault:"1"`     // seconds
	CallbackMaxDelay         int    `env:"CALLBACK_MAX_DELAY" envDefault:"30"`     // seconds
	CallbackTotalTimeout     int    `env:"CALLBACK_TOTAL_TIMEOUT" envDefault:"60"` // seconds
	WorkerPoolSize           int    `env:"WORKER_POOL_SIZE" envDefault:"10"`       // worker pool size
	PodRunningTimeout        int    `env:"POD_RUNNING_TIMEOUT" envDefault:"600"`   // 單位: 秒，預設 10 分鐘
}
