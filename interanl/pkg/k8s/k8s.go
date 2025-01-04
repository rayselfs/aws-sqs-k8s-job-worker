package k8s

import (
	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/interanl/pkg/request/callback"
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
)

var Clientset *kubernetes.Clientset

const (
	pvcName            = "persistent-storage"
	StatusJobCreated   = 0
	StatusPodRunning   = 1
	StatusJobCompleted = 2
	StatusJobFailed    = 3
	StatusException    = 99
)

type JobMessage struct {
	ID      string   `json:"id" validate:"required"`
	Service string   `json:"service" validate:"required"`
	Job     Job      `json:"job" validate:"required"`
	Webhook *Webhook `json:"webhook"`
}

type Job struct {
	PrefixName              string        `json:"prefixName" validate:"required"`
	Namespace               string        `json:"namespace" validate:"required"`
	TTLSecondsAfterFinished int32         `json:"ttlSecondsAfterFinished"`
	ActiveDeadlineSeconds   int64         `json:"activeDeadlineSeconds" validate:"required"`
	BackoffLimit            int32         `json:"backoffLimit"`
	Image                   string        `json:"image" validate:"required"`
	Command                 []string      `json:"command" validate:"required"`
	Resources               Resources     `json:"resources" validate:"required"`
	ServiceAccount          *string       `json:"serviceAccount"`
	Volume                  *Volume       `json:"volume"`
	NodeSelector            *NodeSelector `json:"nodeSelector"`
	Toleration              *Toleration   `json:"toleration"`
	Fargate                 *Fargate      `json:"fargate"`
}

type Resources struct {
	Limits   Resource `json:"limits" validate:"required"`
	Requests Resource `json:"requests" validate:"required"`
}

type Resource struct {
	CPU    string `json:"cpu" validate:"required"`
	Memory string `json:"memory" validate:"required"`
}

type Volume struct {
	PVC       string `json:"pvc" validate:"required"`
	MountPath string `json:"mountPath" validate:"required"`
}

type NodeSelector struct {
	MatchKey   string `json:"matchKey" validate:"required"`
	MatchValue string `json:"matchValue" validate:"required"`
}

type Toleration struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

type Fargate struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

type Webhook struct {
	URL string `json:"url" validate:"required"`
}

// SetClient creates a Kubernetes clientset.
func Setup() (err error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return
	}

	Clientset, err = kubernetes.NewForConfig(config)

	return
}

// GetLeaseLock returns a Kubernetes LeaseLock and hostname
func GetLeaseLock(id string) *resourcelock.LeaseLock {
	// Create the lock configuration
	return &resourcelock.LeaseLock{
		LeaseMeta: metaV1.ObjectMeta{
			Namespace: config.Env.PodNamespace,
			Name:      config.Env.LeaderElectionLockName,
		},
		Client: Clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}
}

// CheckJobName generates a job name based on the job ID and checks if it is valid.
func (jobMsg JobMessage) CheckJobName() (string, error) {
	hash := hashStringSHA256(jobMsg.ID)
	jobName := fmt.Sprintf("%s-%s", jobMsg.Job.PrefixName, hash)
	if len(jobName) >= 63 {
		return "", fmt.Errorf("job name too long: %v", jobName)
	}
	return jobName, nil
}

// getJobSpec generates a Kubernetes Job object based on the JobMessage.
func (jobMsg JobMessage) getJobSpec(jobName string) *batchV1.Job {
	return &batchV1.Job{
		ObjectMeta: metaV1.ObjectMeta{
			Name:      jobName,
			Namespace: jobMsg.Job.Namespace,
		},
		Spec: batchV1.JobSpec{
			TTLSecondsAfterFinished: &jobMsg.Job.TTLSecondsAfterFinished,
			ActiveDeadlineSeconds:   &jobMsg.Job.ActiveDeadlineSeconds,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: metaV1.ObjectMeta{
					Labels: jobMsg.getLabels(),
					Annotations: map[string]string{
						"sidecar.istio.io/inject": "false",
					},
				},
				Spec: jobMsg.getPodSpec(),
			},
			BackoffLimit: &jobMsg.Job.BackoffLimit,
		},
	}
}

func (jobMsg JobMessage) getLabels() map[string]string {
	labels := map[string]string{
		"app":   jobMsg.Service,
		"jobId": jobMsg.ID,
	}

	if jobMsg.Job.Fargate != nil {
		labels[jobMsg.Job.Fargate.Key] = jobMsg.Job.Fargate.Value
	}

	return labels
}

// getPodSpec generates a Kubernetes PodSpec object based on the JobMessage.
func (jobMsg JobMessage) getPodSpec() coreV1.PodSpec {
	enableServiceLinks := false
	podSpec := coreV1.PodSpec{
		Containers:         jobMsg.getContainersSpec(),
		RestartPolicy:      coreV1.RestartPolicyNever,
		EnableServiceLinks: &enableServiceLinks,
	}

	if jobMsg.Job.Volume != nil {
		podSpec.Volumes = []coreV1.Volume{
			{
				Name: pvcName,
				VolumeSource: coreV1.VolumeSource{
					PersistentVolumeClaim: &coreV1.PersistentVolumeClaimVolumeSource{
						ClaimName: jobMsg.Job.Volume.PVC,
					},
				},
			},
		}
	}

	if jobMsg.Job.ServiceAccount != nil {
		podSpec.ServiceAccountName = *jobMsg.Job.ServiceAccount
	}

	if jobMsg.Job.NodeSelector != nil {
		podSpec.NodeSelector = map[string]string{
			jobMsg.Job.NodeSelector.MatchKey: jobMsg.Job.NodeSelector.MatchValue,
		}
	}

	if jobMsg.Job.Toleration != nil {
		toleration := coreV1.Toleration{
			Operator: coreV1.TolerationOpEqual,
			Effect:   coreV1.TaintEffectNoSchedule,
			Key:      jobMsg.Job.Toleration.Key,
			Value:    jobMsg.Job.Toleration.Value,
		}
		podSpec.Tolerations = []coreV1.Toleration{toleration}
	}

	return podSpec
}

// getContainersSpec generates a list of Kubernetes Container objects based on the JobMessage.
func (jobMsg JobMessage) getContainersSpec() []coreV1.Container {
	mainSpec := coreV1.Container{
		Name:            jobMsg.Service,
		Image:           jobMsg.Job.Image,
		Command:         jobMsg.Job.Command,
		ImagePullPolicy: coreV1.PullAlways,
		Resources: coreV1.ResourceRequirements{
			Limits: coreV1.ResourceList{
				"cpu":    resource.MustParse(jobMsg.Job.Resources.Limits.CPU),
				"memory": resource.MustParse(jobMsg.Job.Resources.Limits.Memory),
			},
			Requests: coreV1.ResourceList{
				"cpu":    resource.MustParse(jobMsg.Job.Resources.Requests.CPU),
				"memory": resource.MustParse(jobMsg.Job.Resources.Requests.Memory),
			},
		},
	}

	if jobMsg.Job.Volume != nil {
		mainSpec.VolumeMounts = []coreV1.VolumeMount{
			{
				Name:      pvcName,
				MountPath: jobMsg.Job.Volume.MountPath,
			},
		}
	}

	return []coreV1.Container{mainSpec}
}

// ApplyJob creates a Kubernetes Job in the specified namespace.
func (jobMsg JobMessage) ApplyJob(jobName string) (*batchV1.Job, *callback.ErrorDetail) {
	namespace := jobMsg.Job.Namespace
	jobClient := Clientset.BatchV1().Jobs(namespace)

	klog.Infof("[%s] create k8s job\n", jobMsg.ID)

	result, err := jobClient.Create(context.TODO(), jobMsg.getJobSpec(jobName), metaV1.CreateOptions{})
	if err != nil {
		return nil, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_JOB_CREATE_FAILED,
			Message:   err.Error(),
		}
	}

	return result, nil
}

// JobExists checks if a job with the given name already exists in the specified namespace.
func (jobMsg JobMessage) JobExists(jobName string) (job *batchV1.Job, errorDetail *callback.ErrorDetail) {
	// Try to get the Job
	job, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(context.TODO(), jobName, metaV1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			// Job does not exist
			return nil, nil
		}

		// Some other error occurred
		errorDetail = &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   err.Error(),
		}
		return
	}

	// Job exists
	errorDetail = &callback.ErrorDetail{
		ErrorCode: callback.ERROR_CODE_JOB_EXIST_WITH_NEW_MESSAGE,
		Message:   fmt.Sprintf("job %s already exists in namespace %s", jobName, jobMsg.Job.Namespace),
	}
	return
}

// CheckActiveDeadlineSeconds checks if the activeDeadlineSeconds is within the allowed range.
func (jobMsg JobMessage) CheckActiveDeadlineSeconds() *callback.ErrorDetail {
	if jobMsg.Job.ActiveDeadlineSeconds > config.Env.ActiveDeadlineSecondsMax {
		return &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_JOB_ACTIVE_DEADLINE_SECONDS_TOO_LARGE,
			Message:   fmt.Sprintf("activeDeadlineSeconds must be smaller than %v", config.Env.ActiveDeadlineSecondsMax),
		}
	}

	return nil
}

// WatchPodRunning watches for the running phase of a specific pod.
func (jobMsg JobMessage) WatchPodRunning(job *batchV1.Job) (jobStatus int, detail map[string]interface{}) {
	pod, err := jobMsg.getJobPods(job.GetObjectMeta().GetName())
	if err != nil {
		jobStatus = StatusException
		detail = map[string]interface{}{
			"error": &callback.ErrorDetail{
				ErrorCode: callback.ERROR_CODE_GET_JOB_POD_FAILED,
				Message:   err.Error(),
			},
		}
		return
	}

	lw := cache.NewListWatchFromClient(
		Clientset.CoreV1().RESTClient(),
		"pods",
		jobMsg.Job.Namespace,
		fields.OneTermEqualSelector("metadata.name", pod.Name),
	)

	// Create a stopping channel
	stopCh := make(chan struct{})

	// Start the watch and handle the event loop
	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &coreV1.Pod{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				klog.Infof("[%s] pod created\n", jobMsg.ID)
			},
			// Watch for pod updates
			UpdateFunc: func(oldObj, newObj interface{}) {
				pod := newObj.(*coreV1.Pod)

				// Check if the Pod has transitioned to the Running phase
				if pod.Status.Phase == coreV1.PodRunning {
					klog.Infof("[%s] pod running\n", jobMsg.ID)

					jobStatus = StatusPodRunning
					detail = map[string]interface{}{
						"podId":   pod.GetObjectMeta().GetUID(),
						"podName": pod.GetObjectMeta().GetName(),
					}

					// Stop the controller
					close(stopCh)
					return
				}
			},
		},
	})

	// Run the controller in the current thread, no background
	go controller.Run(stopCh)

	// Wait for the controller to stop.
	<-stopCh

	return
}

// getJobPods gets the pods associated with a specific job.
func (jobMsg JobMessage) getJobPods(jobName string) (*coreV1.Pod, error) {
	count := 10

	// List all pods in the namespace
	for i := 0; i < count; i++ {
		pods, err := Clientset.CoreV1().Pods(jobMsg.Job.Namespace).List(context.TODO(), metaV1.ListOptions{
			LabelSelector: fmt.Sprintf("job-name=%s", jobName),
		})
		if err != nil {
			klog.Errorf("[%s] Failed to list pods: %v", jobMsg.ID, err.Error())
			break
		}

		if pods != nil && len(pods.Items) > 0 {
			return &pods.Items[0], nil
		}

		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to get pod for job %s", jobName)
}

// WatchJobCompletion watches for the completion or failure of a specific job.
func (jobMsg JobMessage) WatchJobCompletion(namespace, jobName string) (jobStatus int, errorContent *callback.ErrorDetail) {
	// Create a ListWatch for the job.
	lw := cache.NewListWatchFromClient(
		Clientset.BatchV1().RESTClient(),
		"jobs",
		namespace,
		fields.OneTermEqualSelector("metadata.name", jobName),
	)

	// Create an Informer to watch for job status updates.
	stopCh := make(chan struct{})

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &batchV1.Job{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj interface{}) {
				job, ok := newObj.(*batchV1.Job)
				if !ok {
					klog.Errorf("[%s] Failed to cast obj to Job\n", jobMsg.ID)
					jobStatus = StatusException
					errorContent = &callback.ErrorDetail{
						ErrorCode: callback.ERROR_CODE_JOB_CAST_OBJECT_FAILED,
						Message:   "Failed to cast obj to Job",
					}
					close(stopCh)
					return
				}

				for _, condition := range job.Status.Conditions {
					if condition.Type == batchV1.JobComplete && condition.Status == coreV1.ConditionTrue {
						klog.Infof("[%s] Job %s succeeded\n", jobMsg.ID, jobName)
						jobStatus = StatusJobCompleted
						// Stop the controller
						close(stopCh)
						return
					} else if condition.Type == batchV1.JobFailed && condition.Status == coreV1.ConditionTrue {
						klog.Errorf("[%s] Job %s failed\n", jobMsg.ID, jobName)
						klog.Errorf("[%s] Reason: %s\n", jobMsg.ID, condition.Reason)
						klog.Errorf("[%s] Message: %s\n", jobMsg.ID, condition.Message)
						jobStatus = StatusJobFailed
						errorContent = &callback.ErrorDetail{
							ErrorCode: callback.ERROR_CODE_JOB_RUN_FAILED,
							Message:   fmt.Sprintf("reason: %v, message: %v", condition.Reason, condition.Message),
						}

						// Delete the job
						deletePolicy := metaV1.DeletePropagationForeground
						_ = Clientset.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metaV1.DeleteOptions{
							PropagationPolicy: &deletePolicy,
						})

						// Stop the controller
						close(stopCh)
						return
					}
				}
			},
		},
	})

	// Start the controller.
	go controller.Run(stopCh)

	// Wait for the controller to stop.
	<-stopCh

	return
}

// CheckJobStatus checks the status of a specific job.
func (jobMsg JobMessage) CheckJobStatus(jobName string) (int, *callback.ErrorDetail) {
	// Get the job
	job, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(context.TODO(), jobName, metaV1.GetOptions{})
	if err != nil {
		klog.Errorf("CheckJobStatus get job error: %v", err.Error())
		return 0, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   fmt.Sprintf("get job error: %v", err.Error()),
		}
	}

	// Check the job status
	if job.Status.Succeeded > 0 {
		return StatusJobCompleted, nil
	}

	return StatusJobFailed, nil
}

// GetJobDuration calculates the duration of a job execution.
func (jobMsg JobMessage) GetJobDuration(jobName string) (*time.Duration, *callback.ErrorDetail) {
	// Get the job
	job, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(context.TODO(), jobName, metaV1.GetOptions{})
	if err != nil {
		klog.Errorf("GetJobDuration get job error: %v", err.Error())
		return nil, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   fmt.Sprintf("get job error: %v", err.Error()),
		}
	}

	duration := job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
	klog.Infof("[%s] Job execution duration: %v\n", jobMsg.ID, duration)
	return &duration, nil
}

// hashStringSHA256 hashes a string using SHA256 and returns the first 8 characters.
func hashStringSHA256(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := fmt.Sprintf("%x", h.Sum(nil))
	return hashed[:8]
}
