package job

import (
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"aws-sqs-k8s-job-worker/interanl/pkg/k8s"
	"aws-sqs-k8s-job-worker/interanl/pkg/request/callback"
)

func InitInstance(jobMsg k8s.JobMessage) (clientset *kubernetes.Clientset, requestBody callback.RequestBody) {
	clientset = k8s.GetClient()
	requestBody = callback.RequestBody{
		ID: jobMsg.ID,
	}
	return
}

func Execution(jobMsg k8s.JobMessage) {
	clientset, requestBody := InitInstance(jobMsg)

	jobName, err := jobMsg.CheckJobName()
	if err != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, err)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NAME_INVALID,
				Message:   err.Error(),
			},
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	_, errorDetail := jobMsg.JobExists(clientset, jobName)
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	errorDetail = jobMsg.CheckActiveDeadlineSeconds()
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// Start job
	job, errorDetail := jobMsg.ApplyJob(clientset, jobName)
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

	WaitPodRunning(clientset, jobMsg, requestBody)

	WaitJobCompletion(clientset, jobMsg, requestBody)
}

func WaitPodRunning(clientset *kubernetes.Clientset, jobMsg k8s.JobMessage, requestBody callback.RequestBody) {
	jobName, _ := jobMsg.CheckJobName()
	job, errorDetail := jobMsg.JobExists(clientset, jobName)
	if errorDetail == nil {
		klog.Errorf("[%s] wait pod running failed, job %s not exists\n", jobMsg.ID, jobName)
		requestBody.Status = k8s.StatusException
		requestBody.Detail = map[string]interface{}{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_JOB_NOT_EXIST,
				Message:   "wait pod running failed, job not exists",
			},
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	klog.Infof("[%s] %v\n", jobMsg.ID, "watching pod running")

	jobStatus, detail := jobMsg.WatchPodRunning(clientset, job)
	requestBody.Status = jobStatus
	requestBody.Detail = detail
	sendCallback(jobMsg, requestBody)
}

func WaitJobCompletion(clientset *kubernetes.Clientset, jobMsg k8s.JobMessage, requestBody callback.RequestBody) {
	jobName, _ := jobMsg.CheckJobName()
	job, errorDetail := jobMsg.JobExists(clientset, jobName)
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
		return
	}

	// Watch for job completion
	klog.Infof("[%s] %v\n", jobMsg.ID, "watching job completion")
	jobStatus, errorDetail := jobMsg.WatchJobCompletion(clientset, jobMsg.Job.Namespace, job.GetObjectMeta().GetName())
	if errorDetail != nil {
		klog.Errorf("[%s] %v\n", jobMsg.ID, errorDetail.Message)
		requestBody.Status = jobStatus
		requestBody.Detail = map[string]interface{}{
			"error": errorDetail,
		}
		sendCallback(jobMsg, requestBody)
		return
	}

	// Get job duration
	klog.Infof("[%s] %v\n", jobMsg.ID, "getting job duration")
	duration, errorDetail := jobMsg.GetJobDuration(clientset, job.GetObjectMeta().GetName())
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
	klog.Infof("[%s] Response status: %v\n", jobMsg.ID, resp.Status)
	resp.Body.Close()
}
