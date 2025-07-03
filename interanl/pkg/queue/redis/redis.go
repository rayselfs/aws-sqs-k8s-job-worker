package redisQeueu

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
)

type RedisQueue struct {
	Client *redis.Client
	Key    string // redis list key
}

func Setup(addr, key string, db int) *RedisQueue {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &RedisQueue{Client: client, Key: key}
}

func (q *RedisQueue) GetMessages() ([]types.Message, error) {
	ctx := context.Background()
	res, err := q.Client.LPop(ctx, q.Key).Result()
	if err == redis.Nil {
		return nil, nil // queue empty
	}
	if err != nil {
		return nil, err
	}
	var msg types.Message
	if err := json.Unmarshal([]byte(res), &msg); err != nil {
		return nil, err
	}
	return []types.Message{msg}, nil
}

func (q *RedisQueue) DeleteMessage(msg types.Message) error {
	// LPop 已經移除，不需額外刪除
	return nil
}
