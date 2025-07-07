package queue

import "github.com/aws/aws-sdk-go-v2/service/sqs/types"

// QueueClient defines the interface for a generic queue backend (SQS, Redis, etc).
type QueueClient interface {
	// GetMessages retrieves messages from the queue.
	GetMessages() ([]types.Message, error)
	// DeleteMessage deletes a message from the queue.
	DeleteMessage(msg types.Message) error
}
