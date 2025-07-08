package sqs

import (
	"context"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/logger"
)

// SqsActions provides methods to interact with AWS SQS.
type SqsActions struct {
	SqsClient *sqs.Client // AWS SQS client
	QueueURL  *string     // SQS queue URL
}

// New creates a new SqsActions instance.
func New(region string, queueUrl string) (*SqsActions, error) {
	// Load the Shared AWS Configuration
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(region))
	if err != nil {
		return nil, err
	}
	// Create an SQS service client
	svc := sqs.NewFromConfig(cfg)
	return &SqsActions{SqsClient: svc, QueueURL: &queueUrl}, nil
}

// GetMessages receives messages from the SQS queue.
func (a *SqsActions) GetMessages() ([]types.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Env.QueueAwsSqsWaitTimeSeconds)*time.Second)
	defer cancel()
	var messages []types.Message
	maxNum := int32(10)
	if config.Env.QueueWorkerPoolSize > 0 && config.Env.QueueWorkerPoolSize <= 10 {
		maxNum = int32(config.Env.QueueWorkerPoolSize)
	}
	result, err := a.SqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            a.QueueURL,
		MaxNumberOfMessages: maxNum,
		WaitTimeSeconds:     config.Env.QueueAwsSqsWaitTimeSeconds,
	})
	if err != nil {
		logger.Error("SQS ReceiveMessage error")
		return nil, err
	}
	messages = result.Messages
	return messages, nil
}

// DeleteMessage deletes a message from the SQS queue.
func (a *SqsActions) DeleteMessage(msg types.Message) error {
	_, err := a.SqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      a.QueueURL,
		ReceiptHandle: msg.ReceiptHandle,
	})
	if err != nil {
		logger.Error("unable to delete message from queue")
	}
	return err
}
