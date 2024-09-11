package main

import (
	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/interanl/app/service/job"
	"aws-sqs-k8s-job-worker/interanl/pkg/aws/sqs"
	"aws-sqs-k8s-job-worker/interanl/pkg/k8s"
	"aws-sqs-k8s-job-worker/interanl/pkg/rdb"
	"encoding/json"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-playground/validator/v10"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"k8s.io/klog/v2"
)

var SqsActions sqs.SqsActions

type Record struct {
	SQSMessage types.Message  `json:"sqsMessage"`
	JobMessage k8s.JobMessage `json:"jobMessage"`
}

func main() {
	if err := config.Setup(); err != nil {
		klog.Fatalf("unable to set config: %v", err)
	}

	if err := rdb.Setup(); err != nil {
		klog.Fatalf("unable to set redis: %v", err)
	}

	recordSetup()

	sqsClient := sqs.Setup(config.Env.AWSSQSRegion)
	queueUrl, err := sqs.GetQueueUrl(sqsClient, config.Env.AWSSQSQueue)
	if err != nil {
		klog.Fatalf("unable to get queue url: %v", err)
	}

	SqsActions = sqs.SqsActions{
		SqsClient: sqsClient,
		QueueURL:  queueUrl,
	}

	klog.Info("start polling")
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

func recordSetup() {
	leaderName := config.Env.PodName

	pool := goredis.NewPool(rdb.Client)
	rs := redsync.New(pool)

	klog.Info("redis lock process")
	mutex := rs.NewMutex(removeLastItem(removeLastItem(leaderName)))
	if err := mutex.Lock(); err != nil {
		klog.Warningf("unable to get leader lock: %v", err)
		return
	}

	// get leader
	klog.Info("get leader list in redis")
	rdbList, err := rdb.GetByPrefix(removeLastItem(leaderName))
	if err != nil {
		klog.Warningf("unable to get leader list in redis: %v", err)
		return
	}
	clientset := k8s.GetClient()
	for key := range rdbList {
		klog.Info("check leader pod exists")
		check, err := k8s.PodExists(clientset, config.Env.PodNamespace, key)
		if err != nil {
			klog.Warningf("unable to check pod: %v", err)
			return
		}

		if check {
			klog.Infof("leader already exists: %v", key)
			return
		}
	}

	klog.Infof("clean old data")
	rdbList, err = rdb.GetByPrefix(removeLastItem(removeLastItem(leaderName)))
	if err != nil {
		klog.Warningf("unable to get leader list in redis: %v", err)
		return
	}
	for key := range rdbList {
		klog.Infof("delete key: %v", key)
		rdb.Delete(key)
	}

	klog.Info("set new leader")
	rdb.Set(leaderName, "enable", 0)

	listAndRunRecord()

	if ok, err := mutex.Unlock(); !ok || err != nil {
		klog.Warningf("unlock failed: %v", err)
		return
	}
}

func removeLastItem(input string) string {
	delimiter := "-"

	// Find the last occurrence of the delimiter
	lastIndex := strings.LastIndex(input, delimiter)
	if lastIndex == -1 {
		// Delimiter not found, return the original string
		return input
	}

	// Slice the string to remove the last item
	result := input[:lastIndex]
	return result
}

func listAndRunRecord() {
	rdbList, err := rdb.GetByPrefix(config.Env.RedisJobKeyPrefix)
	if err != nil {
		klog.Fatalf("unable to get list in redis: %v", err)
	}

	klog.Info("run record process")
	for _, data := range rdbList {
		go recordProcess(data)
	}
}

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
		klog.Errorf("job %v already exists in redis", jobMsg.ID)
		return
	}

	// set backoff limit to 0 to avoid job retry
	jobMsg.Job.BackoffLimit = 0

	record := Record{
		SQSMessage: message,
		JobMessage: jobMsg,
	}
	recordData, _ := json.Marshal(record)
	rdb.Set(config.Env.RedisJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))

	if err := SqsActions.DeleteMessage(message); err != nil {
		klog.Errorf("unable to delete message %v: %v", *message.MessageId, err)
	}

	// process
	job.Execution(jobMsg)

	if err := rdb.Delete(config.Env.RedisJobKeyPrefix + jobMsg.ID); err != nil {
		klog.Errorf("unable to delete job %v from redis: %v", jobMsg.ID, err)
	}
}

func recordProcess(recordData string) {
	var record Record
	if err := json.Unmarshal([]byte(recordData), &record); err != nil {
		klog.Errorf("failed to unmarshal record: %v", err)
		return
	}

	klog.Info("Record received, ID:", record.JobMessage.ID)

	// process
	clientset, requestBody := job.InitInstance(record.JobMessage)
	job.WaitJobCompletion(clientset, record.JobMessage, requestBody)

	if err := rdb.Delete(config.Env.RedisJobKeyPrefix + record.JobMessage.ID); err != nil {
		klog.Errorf("unable to delete job %v from redis: %v", record.JobMessage.ID, err)
	}
}
