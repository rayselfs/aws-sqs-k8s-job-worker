package redisQueue

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/redis/go-redis/v9"
)

// RedisActions provides methods to interact with a Redis queue.
type RedisActions struct {
	Client *redis.Client // Redis client
	Config *Config       // Configuration for Redis queue
}

type Config struct {
	Key            string // Redis list key
	WorkerPoolSize int    // Number of workers to process messages
}

// New creates a new RedisActions instance.
func New(addr string, db int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
}

// GetMessages 使用 BRPOPLPUSH 來安全地獲取訊息
// 這樣可以確保訊息在處理失敗時不會遺失
func (q *RedisActions) GetMessages(ctx context.Context) ([]types.Message, error) {
	var messages []types.Message
	processingKey := q.Config.Key + ":processing"

	for i := 0; i < int(q.Config.WorkerPoolSize); i++ {
		// ✅ 使用 BRPOPLPUSH 將訊息從主佇列移到處理佇列
		// 這樣即使處理失敗，訊息還在處理佇列中，可以重新處理
		res, err := q.Client.BRPopLPush(ctx, q.Config.Key, processingKey, 0).Result()

		if err == redis.Nil {
			break
		}
		if err != nil {
			return messages, err
		}
		var msg types.Message
		if err := json.Unmarshal([]byte(res), &msg); err != nil {
			// ✅ 如果解析失敗，將訊息放回主佇列
			q.Client.LRem(ctx, processingKey, 1, res)
			q.Client.LPush(ctx, q.Config.Key, res)
			return messages, err
		}
		messages = append(messages, msg)
	}

	if len(messages) == 0 {
		return nil, nil
	}
	return messages, nil
}

// DeleteMessage 從處理佇列中移除已完成的訊息
func (q *RedisActions) DeleteMessage(ctx context.Context, msg types.Message) error {
	processingKey := q.Config.Key + ":processing"
	msgBytes, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	// ✅ 從處理佇列中移除訊息（表示處理完成）
	return q.Client.LRem(ctx, processingKey, 1, string(msgBytes)).Err()
}
