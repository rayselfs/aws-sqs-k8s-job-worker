package queue

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// QueueClient defines the interface for a generic queue backend (SQS, Redis, etc).
type QueueClient interface {
	// GetMessages retrieves messages from the queue.
	GetMessages(ctx context.Context) ([]types.Message, error)
	// DeleteMessage deletes a message from the queue.
	DeleteMessage(ctx context.Context, msg types.Message) error
}
