package sqs

import (
	"context"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
)

type SqsActions struct {
	SqsClient *sqs.Client
	QueueURL  *string
}

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

func (a *SqsActions) GetMessages() ([]types.Message, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var messages []types.Message

	result, err := a.SqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            a.QueueURL,
		MaxNumberOfMessages: config.Env.QueueWorkerPoolSize,
		WaitTimeSeconds:     config.Env.QueueAwsSqsWaitTimeSeconds,
	})
	if err != nil {
		logger.Error("SQS ReceiveMessage error")
		return nil, err
	}
	messages = result.Messages
	return messages, nil
}

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
