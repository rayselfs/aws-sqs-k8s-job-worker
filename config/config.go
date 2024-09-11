package config

import (
	"github.com/caarlos0/env/v11"
	"k8s.io/klog/v2"
)

func Setup() error {
	err := env.Parse(&Env)
	if err != nil {
		klog.Errorf("config parse fail, %+v\n", err)
		return err
	}

	return nil
}

var Env EnvVariable

type EnvVariable struct {
	PodName                  string `env:"POD_NAME,required"`
	PodNamespace             string `env:"POD_NAMESPACE,required"`
	LeaderLockName           string `env:"LEADER_LOCK_NAME" envDefault:"job-worker"`
	PollingInterval          int32  `env:"POLLING_INTERVAL" envDefault:"5"`
	RedisEndpoint            string `env:"REDIS_ENDPOINT,required"`
	RedisDB                  int    `env:"REDIS_DB,required"`
	RedisJobKeyPrefix        string `env:"REDIS_JOB_KEY_PREFIX" envDefault:"job-worker-"`
	AWSSQSRegion             string `env:"AWS_SQS_REGION,required"`
	AWSSQSQueue              string `env:"AWS_SQS_QUEUE,required"`
	ActiveDeadlineSecondsMax int64  `env:"ACTIVE_DEADLINE_SECONDS_MAX" envDefault:"86400"`
}
