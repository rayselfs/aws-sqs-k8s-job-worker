package sqs

import (
	"context"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"aws-sqs-k8s-job-worker/interanl/pkg/logger"

	"go.uber.org/zap"
)

type SqsActions struct {
	SqsClient *sqs.Client
	QueueURL  *string
}

var Actor SqsActions

func Setup(region string, queueUrl string) *SqsActions {
	// Load the Shared AWS Configuration
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(), awsconfig.WithRegion(region))
	if err != nil {
		logger.Fatal("unable to load AWS config", zap.Error(err))
		return nil
	}

	// Create an SQS service client
	svc := sqs.NewFromConfig(cfg)
	return &SqsActions{SqsClient: svc, QueueURL: &queueUrl}
}

func (a *SqsActions) GetMessages() ([]types.Message, error) {
	var messages []types.Message
	result, err := a.SqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            a.QueueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     20,
	})
	if err != nil {
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
		logger.Error("unable to delete message from queue", zap.String("queueURL", *a.QueueURL), zap.Error(err))
	}
	return err
}
