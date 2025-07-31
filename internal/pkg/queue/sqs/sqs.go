package sqs

import (
	"context"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/internal/pkg/logger"
)

// SqsActions provides methods to interact with AWS SQS.
type SqsActions struct {
	SqsClient *sqs.Client // AWS SQS client
	Config    *Config     // Configuration for SQS
}

type Config struct {
	WorkerPoolSize  int    // Number of workers to process messages
	QueueUrl        string // SQS queue URL
	WaitTimeSeconds int32  // Wait time for SQS messages
}

// NewClient creates a new sqs client
func NewClient(ctx context.Context, region string, queueUrl string) (*sqs.Client, error) {
	// Load the Shared AWS Configuration
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	// Create an SQS service client
	svc := sqs.NewFromConfig(cfg)
	return svc, nil
}

// GetMessages receives messages from the SQS queue.
func (a *SqsActions) GetMessages(ctx context.Context) ([]types.Message, error) {
	var messages []types.Message
	maxNum := int32(a.Config.WorkerPoolSize)
	result, err := a.SqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &a.Config.QueueUrl,
		MaxNumberOfMessages: maxNum,
		WaitTimeSeconds:     a.Config.WaitTimeSeconds,
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
		QueueUrl:      &a.Config.QueueUrl,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		logger.Error("unable to delete message from queue")
	}
	return err
}
