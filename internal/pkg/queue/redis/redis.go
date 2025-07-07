package redisQueue

import (
	"context"
	"encoding/json"
	"time"

	"aws-sqs-k8s-job-worker/config"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
)

type RedisActions struct {
	Client *redis.Client
	Key    string // redis list key
}

func New(addr, key string, db int) *RedisActions {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &RedisActions{Client: client, Key: key}
}

func (q *RedisActions) GetMessages() ([]types.Message, error) {
	var messages []types.Message

	for i := 0; i < int(config.Env.QueueWorkerPoolSize); i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Env.QueueRedisWaitTimeout)*time.Second)

		res, err := q.Client.LPop(ctx, q.Key).Result()
		cancel()

		if err == redis.Nil {
			break
		}
		if err != nil {
			return messages, err
		}
		var msg types.Message
		if err := json.Unmarshal([]byte(res), &msg); err != nil {
			return messages, err
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, nil
	}
	return messages, nil
}

func (q *RedisActions) DeleteMessage(msg types.Message) error {
	// LPop already removed, no need to delete
	return nil
}
