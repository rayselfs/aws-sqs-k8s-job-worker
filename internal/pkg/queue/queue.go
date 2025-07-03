package queue

import "github.com/aws/aws-sdk-go-v2/service/sqs/types"

type QueueClient interface {
	GetMessages() ([]types.Message, error)
	DeleteMessage(msg types.Message) error
}
