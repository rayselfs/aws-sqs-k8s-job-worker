package job

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"k8s.io/klog/v2"

	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/interanl/pkg/k8s"
	"aws-sqs-k8s-job-worker/interanl/pkg/rdb"
	"aws-sqs-k8s-job-worker/interanl/pkg/request/callback"

	batchV1 "k8s.io/api/batch/v1"
)

type Record struct {
	SQSMessage types.Message  `json:"sqsMessage"`
	JobMessage k8s.JobMessage `json:"jobMessage"`
	Status     int            `json:"status"`
}

const (
	StatusInit       = 0
	StatusJobCreated = 1
	StatusPodRunning = 2
	StatusJobDone    = 3
)

func Execution(record Record) {
	requestBody := GetCallbackRequest(record.JobMessage)
	jobMsg := record.JobMessage

	status, detail := validation(jobMsg)
	if status != 0 {
		requestBody.Status = status
		requestBody.Detail = detail
		sendCallback(jobMsg, requestBody)
		return
	}

	// Start job
	jobName, _ := jobMsg.CheckJobName()

	if record.Status == StatusInit {
		job, errorDetail := jobMsg.ApplyJob(jobName)
		if errorDetail != nil {
			klog.Errorf("[%s] apply job error: %v\n", jobMsg.ID, errorDetail.Message)
			requestBody.Status = k8s.StatusException
			requestBody.Detail = map[string]interface{}{
				"error": errorDetail,
			}
			sendCallback(jobMsg, requestBody)
			return
		}

		// Send started callback
		requestBody.Status = k8s.StatusJobCreated
		requestBody.Detail = map[string]interface{}{
			"jobId":   job.GetObjectMeta().GetUID(),
			"jobName": job.GetObjectMeta().GetName(),
		}
		sendCallback(jobMsg, requestBody)

		record.Status = StatusJobCreated
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+jobMsg.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	// check job exist
	job, errorDetail := jobMsg.JobExists(jobName)
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// process after job created
	if record.Status == StatusJobCreated {
		status = WaitPodRunning(jobMsg, job, requestBody)
		if status == k8s.StatusException {
			return
		}

		record.Status = StatusPodRunning
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+record.JobMessage.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	// process after job's pod running
	if record.Status == StatusPodRunning {
		WaitJobCompletion(jobMsg, job, requestBody)

		record.Status = StatusJobDone
		recordData, _ := json.Marshal(record)
		rdb.Set(config.Env.RedisJobKeyPrefix+record.JobMessage.ID, string(recordData), time.Second*time.Duration(config.Env.ActiveDeadlineSecondsMax))
	}

	if record.Status == StatusJobDone {
		jobStatus, errorDetail := jobMsg.CheckJobStatus(jobName)
		if errorDetail != nil {
			klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
			requestBody.Status = k8s.StatusException
			requestBody.Detail = map[string]interface{}{
				"error": errorDetail,
			}
			sendCallback(jobMsg, requestBody)
			return
		}

		GetJobDurection(jobMsg, job, jobStatus, requestBody)
	}
}

func GetCallbackRequest(jobMsg k8s.JobMessage) (requestBody callback.RequestBody) {
	requestBody = callback.RequestBody{
		ID: jobMsg.ID,
	}
	return
}

// validation validates the job
func validation(jobMsg k8s.JobMessage) (int, map[string]interface{}) {
	jobName, err := jobMsg.CheckJobName()
	if err != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, err)
		return k8s.StatusException, map[string]interface{}{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NAME_INVALID,
				Message:   err.Error(),
			},
		}
	}

	if jobMsg.Job.TTLSecondsAfterFinished < 60 {
		klog.Errorf("[%s] job's TTLSecondsAfterFinished is less than 60 seconds \n", jobMsg.ID)
		return k8s.StatusException, map[string]interface{}{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_TTL_SECONDS_AFTER_FINISHED_TOO_SMALL,
				Message:   "job's TTLSecondsAfterFinished is less than 60 seconds",
			},
		}
	}

	_, errorDetail := jobMsg.JobExists(jobName)
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		return k8s.StatusException, map[string]interface{}{
			"error": errorDetail,
		}
	}

	errorDetail = jobMsg.CheckActiveDeadlineSeconds()
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		return k8s.StatusException, map[string]interface{}{
			"error": errorDetail,
		}
	}
	return 0, nil
}

func RecordCheckJobExist(jobMsg k8s.JobMessage, requestBody callback.RequestBody) int {
	jobName, _ := jobMsg.CheckJobName()
	_, errorDetail := jobMsg.JobExists(jobName)
	if errorDetail == nil {
		klog.Errorf("[%s] watch job completion failed, job %s not exists\n", jobMsg.ID, jobName)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NOT_EXIST,
				Message:   "watch job completion failed, job not exists",
			},
		}
		sendCallback(jobMsg, requestBody)
	}

	return requestBody.Status
}

func WaitPodRunning(jobMsg k8s.JobMessage, job *batchV1.Job, requestBody callback.RequestBody) int {
	klog.Infof("[%s] %v\n", jobMsg.ID, "watching pod running")

	jobStatus, detail := jobMsg.WatchPodRunning(job)
	requestBody.Status = jobStatus
	requestBody.Detail = detail
	sendCallback(jobMsg, requestBody)

	return requestBody.Status
}

func WaitJobCompletion(jobMsg k8s.JobMessage, job *batchV1.Job, requestBody callback.RequestBody) {
	// Watch for job completion
	klog.Infof("[%s] %v\n", jobMsg.ID, "watching job completion")
	jobStatus, errorDetail := jobMsg.WatchJobCompletion(jobMsg.Job.Namespace, job.GetObjectMeta().GetName())
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = jobStatus
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	return
}

func GetJobDurection(jobMsg k8s.JobMessage, job *batchV1.Job, jobStatus int, requestBody callback.RequestBody) {
	// Get job duration
	klog.Infof("[%s] %v\n", jobMsg.ID, "getting job duration")
	duration, errorDetail := jobMsg.GetJobDuration(job.GetObjectMeta().GetName())
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// Send finished callback
	requestBody.Status = jobStatus
	requestBody.Detail = map[string]interface{}{
		"duration": duration.Seconds(),
	}
	sendCallback(jobMsg, requestBody)
	return
}

func sendCallback(jobMsg k8s.JobMessage, body callback.RequestBody) {
	if jobMsg.Webhook == nil {
		return
	}

	callback := body
	resp, err := callback.Post(jobMsg.Webhook.URL)
	if err != nil {
		klog.Errorf("[%s] start job callback error: %v\n", jobMsg.ID, err)
		return
	}
	// Print the response status.
	klog.Infof("[%s] response status: %v\n", jobMsg.ID, resp.Status)
	resp.Body.Close()
}
