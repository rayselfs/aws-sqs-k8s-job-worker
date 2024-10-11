package main

import (
	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/interanl/app/service/job"
	"aws-sqs-k8s-job-worker/interanl/pkg/aws/sqs"
	"aws-sqs-k8s-job-worker/interanl/pkg/k8s"
	"aws-sqs-k8s-job-worker/interanl/pkg/rdb"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog/v2"
)

var SqsActions sqs.SqsActions

func main() {
	if err := config.Setup(); err != nil {
		klog.Fatalf("unable to set config: %v", err)
	}

	if err := rdb.Setup(); err != nil {
		klog.Fatalf("unable to set redis: %v", err)
	}

	if err := k8s.Setup(); err != nil {
		klog.Fatalf("unable to set k8s: %v\n", err)
	}

	id := config.Env.PodName
	lock := k8s.GetLeaseLock(id)

	leaderElectorConfig := leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: 15 * time.Second,
		RenewDeadline: 10 * time.Second,
		RetryPeriod:   2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Info("started leading")
				handleRecords()
				handleMessages()
			},
			OnStoppedLeading: func() {
				klog.Info("stopped leading")
				os.Exit(0)
			},
			OnNewLeader: func(identity string) {
				if identity == id {
					klog.Info("current New leader")
				} else {
					klog.Infof("new leader elected: %s\n", identity)
				}
			},
		},
	}

	elector, err := leaderelection.NewLeaderElector(leaderElectorConfig)
	if err != nil {
		log.Fatalf("error creating leader elector: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go elector.Run(ctx)

	// Wait indefinitely
	select {}
}

// handleMessages handle messages in SQS
func handleMessages() {
	sqsClient := sqs.Setup(config.Env.AWSSQSRegion)
	queueUrl, err := sqs.GetQueueUrl(sqsClient, config.Env.AWSSQSQueue)
	if err != nil {
		klog.Fatalf("unable to get queue url: %v", err)
	}

	SqsActions = sqs.SqsActions{
		SqsClient: sqsClient,
		QueueURL:  queueUrl,
	}

	klog.Info("start sqs polling")
	for {
		messages, err := SqsActions.GetMessages()
		if err != nil {
			klog.Fatalf("unable to get messages from queue %v, %v", SqsActions.QueueURL, err)
		}

		if len(messages) > 0 {
			message := messages[0]
			go messageProcess(message)
		}

		time.Sleep(time.Second * time.Duration(config.Env.PollingInterval))
	}
}

// handleRecords handle records in redis
func handleRecords() {
	rdbList, err := rdb.GetByPrefix(config.Env.RedisJobKeyPrefix)
	if err != nil {
		klog.Fatalf("unable to get list in redis: %v", err)
	}

	klog.Info("start record process")
	for _, data := range rdbList {
		go recordProcess(data)
	}
}

// messageProcess process message from SQS
func messageProcess(message types.Message) {
	klog.Info("message received, ID:", *message.MessageId)
	klog.Info("message body:", *message.Body)

	var jobMsg k8s.JobMessage
	if err := json.Unmarshal([]byte(*message.Body), &jobMsg); err != nil {
		klog.Errorf("failed to unmarshal job message: %v", err)
		SqsActions.DeleteMessage(message)
		return
	}

	validator := validator.New()
	if err := validator.Struct(jobMsg); err != nil {
		klog.Errorf("job message validation failed: %v", err)
		SqsActions.DeleteMessage(message)
		return
	}

	if _, err := rdb.Get(config.Env.RedisJobKeyPrefix + jobMsg.ID); err == nil {
		klog.Errorf("job %v has been executed", jobMsg.ID)
		SqsActions.DeleteMessage(message)
		return
	}

	// set backoff limit to 0 to avoid job retry
	jobMsg.Job.BackoffLimit = 0
	if jobMsg.Job.TTLSecondsAfterFinished == 0 {
		jobMsg.Job.TTLSecondsAfterFinished = 60
	}

	record := job.Record{
		SQSMessage: message,
		JobMessage: jobMsg,
		Status:     job.StatusInit,
	}
	recordData, _ := json.Marshal(record)
	rdb.Set(config.Env.RedisJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))

	SqsActions.DeleteMessage(message)

	// process
	klog.Info("start message process")
	job.Execution(record)

	if err := rdb.Delete(config.Env.RedisJobKeyPrefix + jobMsg.ID); err != nil {
		klog.Errorf("unable to delete job %v from redis: %v", jobMsg.ID, err)
	}
}

// recordProcess process message from redis
func recordProcess(recordData string) {
	var record job.Record
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		klog.Errorf("failed to unmarshal record: %v", err)
		return
	}

	klog.Info("Record received, ID:", record.JobMessage.ID)
	job.Execution(record)

	if err := rdb.Delete(config.Env.RedisJobKeyPrefix + record.JobMessage.ID); err != nil {
		klog.Errorf("unable to delete job %v from redis: %v", record.JobMessage.ID, err)
	}
}
