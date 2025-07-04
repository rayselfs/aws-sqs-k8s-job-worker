package k8s

import (
	"aws-sqs-k8s-job-worker/config"
	"aws-sqs-k8s-job-worker/internal/pkg/request/callback"
	"context"
	"crypto/sha256"
	"fmt"
	"sync"
	"time"

	"aws-sqs-k8s-job-worker/internal/pkg/logger"

	"go.uber.org/zap"
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
	Resources               *Resources    `json:"resources"`
	ServiceAccount          *string       `json:"serviceAccount"`
	Volume                  *Volume       `json:"volume"`
	NodeSelector            *NodeSelector `json:"nodeSelector"`
	Toleration              *Toleration   `json:"toleration"`
	Fargate                 *Fargate      `json:"fargate"`
	GpuEnable               bool          `json:"gpuEnable"`
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
			Labels:    jobMsg.getLabels(),
		},
		Spec: batchV1.JobSpec{
			TTLSecondsAfterFinished: &jobMsg.Job.TTLSecondsAfterFinished,
			ActiveDeadlineSeconds:   &jobMsg.Job.ActiveDeadlineSeconds,
			Template: coreV1.PodTemplateSpec{
				ObjectMeta: metaV1.ObjectMeta{
					Labels: jobMsg.getLabels(),
				},
				Spec: jobMsg.getPodSpec(),
			},
			BackoffLimit: &jobMsg.Job.BackoffLimit,
		},
	}
}

func (jobMsg JobMessage) getLabels() map[string]string {
	labels := map[string]string{
		"app.kubernetes.io/name": jobMsg.Service,
		"jobId":                  jobMsg.ID,
	}

	if jobMsg.Job.GpuEnable {
		labels["gpuType"] = "1"
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

	if jobMsg.Job.GpuEnable {
		podSpec.Affinity = &coreV1.Affinity{
			PodAntiAffinity: &coreV1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []coreV1.PodAffinityTerm{
					{
						LabelSelector: &metaV1.LabelSelector{
							MatchExpressions: []metaV1.LabelSelectorRequirement{
								{
									Key:      "gpuType",
									Operator: metaV1.LabelSelectorOpIn,
									Values:   []string{"1"},
								},
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		}
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
	}

	if jobMsg.Job.GpuEnable {
		mainSpec.Resources = coreV1.ResourceRequirements{
			Limits: coreV1.ResourceList{
				"nvidia.com/gpu": resource.MustParse("1"),
			},
		}
	} else if jobMsg.Job.Resources != nil {
		mainSpec.Resources = coreV1.ResourceRequirements{
			Limits: coreV1.ResourceList{
				"cpu":    resource.MustParse(jobMsg.Job.Resources.Limits.CPU),
				"memory": resource.MustParse(jobMsg.Job.Resources.Limits.Memory),
			},
			Requests: coreV1.ResourceList{
				"cpu":    resource.MustParse(jobMsg.Job.Resources.Requests.CPU),
				"memory": resource.MustParse(jobMsg.Job.Resources.Requests.Memory),
			},
		}
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

	logger.Info("create k8s job", zap.String("id", jobMsg.ID))

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
		return nil, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   err.Error(),
		}
	}
	// Job exists
	return job, nil
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

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	stopCh := make(chan struct{})

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &coreV1.Pod{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				logger.Info("pod created", zap.String("id", jobMsg.ID))
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				pod := newObj.(*coreV1.Pod)
				if pod.Status.Phase == coreV1.PodRunning {
					logger.Info("pod running", zap.String("id", jobMsg.ID))
					jobStatus = StatusPodRunning
					detail = map[string]interface{}{
						"podId":   pod.GetObjectMeta().GetUID(),
						"podName": pod.GetObjectMeta().GetName(),
					}
					close(stopCh)
				}
			},
		},
	})

	go controller.Run(stopCh)

	select {
	case <-stopCh:
		// 正常結束
	case <-ctx.Done():
		jobStatus = StatusException
		detail = map[string]interface{}{
			"error": &callback.ErrorDetail{
				ErrorCode: "POD_RUNNING_TIMEOUT",
				Message:   "Timeout waiting for pod running",
			},
		}
		close(stopCh)
	}
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
			logger.Error("Failed to list pods", zap.String("id", jobMsg.ID), zap.Error(err))
			break
		}

		if pods != nil && len(pods.Items) > 0 {
			return &pods.Items[0], nil
		}

		time.Sleep(1 * time.Second)
	}

	return nil, fmt.Errorf("failed to get pod for job %s", jobName)
}

func (jobMsg JobMessage) WatchJobCompletion(namespace, jobName string) (jobStatus int, errorContent *callback.ErrorDetail) {
	lw := cache.NewListWatchFromClient(
		Clientset.BatchV1().RESTClient(),
		"jobs",
		namespace,
		fields.OneTermEqualSelector("metadata.name", jobName),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 600*time.Second)
	defer cancel()
	stopCh := make(chan struct{})
	var once sync.Once

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &batchV1.Job{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj interface{}) {
				job, ok := newObj.(*batchV1.Job)
				if !ok {
					logger.Error("Failed to cast obj to Job", zap.String("id", jobMsg.ID))
					jobStatus = StatusException
					errorContent = &callback.ErrorDetail{
						ErrorCode: callback.ERROR_CODE_JOB_CAST_OBJECT_FAILED,
						Message:   "Failed to cast obj to Job",
					}
					once.Do(func() { close(stopCh) })
					return
				}

				for _, condition := range job.Status.Conditions {
					if condition.Type == batchV1.JobComplete && condition.Status == coreV1.ConditionTrue {
						logger.Info("Job succeeded", zap.String("id", jobMsg.ID), zap.String("jobName", jobName))
						jobStatus = StatusJobCompleted
						once.Do(func() { close(stopCh) })
						return
					} else if condition.Type == batchV1.JobFailed && condition.Status == coreV1.ConditionTrue {
						logger.Error("Job failed", zap.String("id", jobMsg.ID), zap.String("jobName", jobName))
						logger.Error("Reason: "+condition.Reason, zap.String("id", jobMsg.ID))
						logger.Error("Message: "+condition.Message, zap.String("id", jobMsg.ID))
						jobStatus = StatusJobFailed
						errorContent = &callback.ErrorDetail{
							ErrorCode: callback.ERROR_CODE_JOB_RUN_FAILED,
							Message:   fmt.Sprintf("reason: %v, message: %v", condition.Reason, condition.Message),
						}
						deletePolicy := metaV1.DeletePropagationForeground
						_ = Clientset.BatchV1().Jobs(namespace).Delete(context.TODO(), jobName, metaV1.DeleteOptions{
							PropagationPolicy: &deletePolicy,
						})
						once.Do(func() { close(stopCh) })
						return
					}
				}
			},
		},
	})

	go controller.Run(stopCh)

	select {
	case <-stopCh:
		// 正常結束
	case <-ctx.Done():
		jobStatus = StatusException
		errorContent = &callback.ErrorDetail{
			ErrorCode: "JOB_COMPLETION_TIMEOUT",
			Message:   "Timeout waiting for job completion",
		}
		once.Do(func() { close(stopCh) })
	}
	return
}

// CheckJobStatus checks the status of a specific job.
func (jobMsg JobMessage) CheckJobStatus(jobName string) (int, *callback.ErrorDetail) {
	// Get the job
	job, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(context.TODO(), jobName, metaV1.GetOptions{})
	if err != nil {
		logger.Error("CheckJobStatus get job error", zap.Error(err))
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
		logger.Error("GetJobDuration get job error", zap.Error(err))
		return nil, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   fmt.Sprintf("get job error: %v", err.Error()),
		}
	}
	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		return nil, &callback.ErrorDetail{
			ErrorCode: callback.ERROR_CODE_GET_JOB_FAILED,
			Message:   "job StartTime or CompletionTime is nil",
		}
	}
	duration := job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
	logger.Info("Job execution duration", zap.String("id", jobMsg.ID), zap.Duration("duration", duration))
	return &duration, nil
}

// hashStringSHA256 hashes a string using SHA256 and returns the first 8 characters.
func hashStringSHA256(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := fmt.Sprintf("%x", h.Sum(nil))
	return hashed[:8]
}
