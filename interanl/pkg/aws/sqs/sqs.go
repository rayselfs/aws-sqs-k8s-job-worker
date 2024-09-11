package sqs

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"k8s.io/klog/v2"
)

type SqsActions struct {
	SqsClient *sqs.Client
	QueueURL  *string
}

func Setup(region string) *sqs.Client {
	// Load the Shared AWS Configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		klog.Fatalf("unable to load SDK config, %v", err)
	}

	// Create an SQS service client
	svc := sqs.NewFromConfig(cfg)

	return svc
}

func GetQueueUrl(client *sqs.Client, queueName string) (*string, error) {
	result, err := client.GetQueueUrl(context.TODO(), &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	})

	return result.QueueUrl, err
}

// ChangeVisibilityTimeout changes the visibility timeout of a specified message.
func (actor SqsActions) ChangeVisibilityTimeout(msg types.Message, visibilityTimeout int32) error {
	_, err := actor.SqsClient.ChangeMessageVisibility(context.TODO(), &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          actor.QueueURL,    // URL of the SQS queue
		ReceiptHandle:     msg.ReceiptHandle, // Receipt handle of the message to change visibility timeout
		VisibilityTimeout: visibilityTimeout, // New visibility timeout in seconds
	})

	if err != nil {
		klog.Errorf("unable to change visibility timeout for message %v: %v", *msg.MessageId, err)
	}

	return err
}

// GetMessages retrieves messages from the SQS queue.
func (actor SqsActions) GetMessages() ([]types.Message, error) {
	var messages []types.Message

	// Call the ReceiveMessage method on the SQS client
	result, err := actor.SqsClient.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
		QueueUrl:            actor.QueueURL, // URL of the SQS queue
		MaxNumberOfMessages: 1,              // Maximum number of messages to retrieve
		WaitTimeSeconds:     20,             // Wait time in seconds for long polling
	})

	// Handle any errors from the ReceiveMessage call
	if err != nil {
		return nil, err
	}

	messages = result.Messages

	return messages, nil
}

// DeleteMessage deletes a specified message from the SQS queue.
func (actor SqsActions) DeleteMessage(msg types.Message) error {
	// Delete the message using its receipt handle
	_, err := actor.SqsClient.DeleteMessage(context.TODO(), &sqs.DeleteMessageInput{
		QueueUrl:      actor.QueueURL,    // URL of the SQS queue
		ReceiptHandle: msg.ReceiptHandle, // Receipt handle of the message to delete
	})

	// Check if there was an error deleting the message and log it
	if err != nil {
		klog.Errorf("unable to delete message from queue %v, %v", actor.QueueURL, err)
	}

	// Return the error (nil if successful, otherwise the error encountered)
	return err
}
