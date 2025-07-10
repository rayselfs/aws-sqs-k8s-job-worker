package sqs

import (
	"context"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/configs"
	"aws-sqs-k8s-job-worker/internal/logger"
)

// SqsActions provides methods to interact with AWS SQS.
type SqsActions struct {
	SqsClient *sqs.Client // AWS SQS client
	QueueURL  *string     // SQS queue URL
}

// New creates a new SqsActions instance.
func New(ctx context.Context, region string, queueUrl string) (*SqsActions, error) {
	// Load the Shared AWS Configuration
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	// Create an SQS service client
	svc := sqs.NewFromConfig(cfg)
	return &SqsActions{SqsClient: svc, QueueURL: &queueUrl}, nil
}

// GetMessages receives messages from the SQS queue.
func (a *SqsActions) GetMessages(ctx context.Context) ([]types.Message, error) {
	var messages []types.Message
	maxNum := int32(10)
	if configs.Env.QueueWorkerPoolSize > 0 && configs.Env.QueueWorkerPoolSize <= 10 {
		maxNum = int32(configs.Env.QueueWorkerPoolSize)
	}
	result, err := a.SqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            a.QueueURL,
		MaxNumberOfMessages: maxNum,
		WaitTimeSeconds:     configs.Env.QueueAwsSqsWaitTimeSeconds,
	})
	if err != nil {
		logger.Error("SQS ReceiveMessage error")
		return nil, err
	}
	messages = result.Messages
	return messages, nil
}

// DeleteMessage deletes a message from the SQS queue.
func (a *SqsActions) DeleteMessage(ctx context.Context, msg types.Message) error {
	_, err := a.SqsClient.DeleteMessage(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      a.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		logger.Error("unable to delete message from queue")
	}
	return err
}
