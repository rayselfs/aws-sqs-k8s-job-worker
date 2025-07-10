package redisQueue

import (
	"aws-sqs-k8s-job-worker/configs"
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
)

// RedisActions provides methods to interact with a Redis queue.
type RedisActions struct {
	Client *redis.Client // Redis client
	Key    string        // Redis list key
}

// New creates a new RedisActions instance.
func New(addr, key string, db int) *RedisActions {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &RedisActions{Client: client, Key: key}
}

// GetMessages pops messages from the Redis queue and unmarshals them into SQS messages.
func (q *RedisActions) GetMessages(ctx context.Context) ([]types.Message, error) {
	var messages []types.Message

	for i := 0; i < int(configs.Env.QueueWorkerPoolSize); i++ {
		res, err := q.Client.LPop(ctx, q.Key).Result()

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

// DeleteMessage is a no-op for Redis since LPop already removes the message.
func (q *RedisActions) DeleteMessage(ctx context.Context, msg types.Message) error {
	// LPop already removed, no need to delete
	return nil
}
