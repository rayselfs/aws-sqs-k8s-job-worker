package k8s

import (
	"aws-sqs-k8s-job-worker/config"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"aws-sqs-k8s-job-worker/internal/pkg/logger"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"

	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Clientset is the global Kubernetes clientset instance.
var Clientset *kubernetes.Clientset

// Common errors for job and pod operations.
var (
	ErrPodStartTimeout = errors.New("pod start timeout")      // Pod did not start in time
	ErrJobTimeout      = errors.New("job completion timeout") // Job did not complete in time
	ErrJobFailed       = errors.New("job failed")             // Job failed
)

const (
	pvcName            = "persistent-storage" // Default PVC name
	StatusJobCreated   = 0                    // Job created
	StatusPodRunning   = 1                    // Pod running
	StatusJobCompleted = 2                    // Job completed
	StatusJobFailed    = 3                    // Job failed
	StatusException    = 99                   // Exception status
)

// JobMessage represents a job request with metadata and webhook.
type JobMessage struct {
	ID      string   `json:"id" validate:"required"`      // Unique job ID
	Service string   `json:"service" validate:"required"` // Service name
	Job     Job      `json:"job" validate:"required"`     // Job spec
	Webhook *Webhook `json:"webhook"`                     // Optional webhook callback
}

// Job defines the Kubernetes Job spec and related options.
type Job struct {
	PrefixName              string        `json:"prefixName" validate:"required"`                           // Job name prefix
	Namespace               string        `json:"namespace" validate:"required"`                            // Namespace
	TTLSecondsAfterFinished int32         `json:"ttlSecondsAfterFinished" validate:"required gt=30,lt=120"` // TTL after job finished
	ActiveDeadlineSeconds   int64         `json:"activeDeadlineSeconds" validate:"required gt=60,lt=86400"` // Max job duration
	BackoffLimit            int32         `json:"backoffLimit"`                                             // Retry limit
	Image                   string        `json:"image" validate:"required"`                                // Container image
	Command                 []string      `json:"command" validate:"required"`                              // Command to run
	Resources               *Resources    `json:"resources"`                                                // Resource requests/limits
	ServiceAccount          *string       `json:"serviceAccount"`                                           // Service account
	Volume                  *Volume       `json:"volume"`                                                   // Volume mount
	NodeSelector            *NodeSelector `json:"nodeSelector"`                                             // Node selector
	Toleration              *Toleration   `json:"toleration"`                                               // Toleration
	Fargate                 *Fargate      `json:"fargate"`                                                  // Fargate profile
	GpuEnable               bool          `json:"gpuEnable"`                                                // Use GPU
}

// Resources defines CPU and memory requests/limits.
type Resources struct {
	Limits   Resource `json:"limits" validate:"required"`
	Requests Resource `json:"requests" validate:"required"`
}

// Resource defines a single resource request/limit.
type Resource struct {
	CPU    string `json:"cpu" validate:"required"`
	Memory string `json:"memory" validate:"required"`
}

// Volume defines a PVC volume mount.
type Volume struct {
	PVC       string `json:"pvc" validate:"required"`
	MountPath string `json:"mountPath" validate:"required"`
}

// NodeSelector defines a node selector for scheduling.
type NodeSelector struct {
	MatchKey   string `json:"matchKey" validate:"required"`
	MatchValue string `json:"matchValue" validate:"required"`
}

// Toleration defines a pod toleration.
type Toleration struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

// Fargate defines a Fargate profile selector.
type Fargate struct {
	Key   string `json:"key" validate:"required"`
	Value string `json:"value" validate:"required"`
}

// Webhook defines a callback URL for job status updates.
type Webhook struct {
	URL string `json:"url" validate:"required"`
}

// JobFailureDetail holds job and pod failure reasons/messages.
type JobFailureDetail struct {
	JobReason   string       // Job failure reason
	JobMessage  string       // Job failure message
	PodFailures []PodFailure // List of pod failures
}

// PodFailure holds details about a failed pod.
type PodFailure struct {
	PodName  string // Pod name
	ExitCode int32  // Exit code
	Reason   string // Failure reason
	Message  string // Failure message
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

// getLabels returns labels for the job and pod.
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
func (jobMsg JobMessage) ApplyJob(jobName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
	defer cancel()
	_, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Create(ctx, jobMsg.getJobSpec(jobName), metaV1.CreateOptions{})
	if err != nil {
		return err
	}

	return nil
}

// GetJob retrieves a Job by name from the specified namespace.
func (jobMsg JobMessage) GetJob(jobName string) (*batchV1.Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
	defer cancel()
	return Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(ctx, jobName, metaV1.GetOptions{})
}

// JobExists checks if a job with the given name already exists in the specified namespace.
func (jobMsg JobMessage) JobExists(jobName string) bool {
	_, err := jobMsg.GetJob(jobName)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			return false
		}

		logger.Error("Failed to get job: %s", err.Error())
		return true // exception case, assume job exists
	}
	return true
}

// WatchPodRunning watches for the running phase of a specific pod.
func (jobMsg JobMessage) WatchPodRunning(logCtx context.Context, jobName string) (pod *coreV1.Pod, err error) {
	pod, err = jobMsg.getJobPods(logCtx, jobName)
	if err != nil {
		return
	}

	lw := cache.NewListWatchFromClient(
		Clientset.CoreV1().RESTClient(),
		"pods",
		jobMsg.Job.Namespace,
		fields.OneTermEqualSelector("metadata.name", pod.Name),
	)

	ctx, cancel := context.WithTimeout(context.Background(), config.Env.PodStartTimeoutDuration)
	defer cancel()
	stopCh := make(chan struct{})

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &coreV1.Pod{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				logger.InfoCtx(logCtx, "pod created: %s", obj.(*coreV1.Pod).GetName())
			},
			UpdateFunc: func(oldObj, newObj any) {
				pod := newObj.(*coreV1.Pod)
				if pod.Status.Phase == coreV1.PodRunning {
					logger.InfoCtx(logCtx, "pod running")
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
		logger.ErrorCtx(logCtx, "pod start timeout")
		err = ErrPodStartTimeout
		close(stopCh)
	}
	return
}

// getJobPods waits for the first pod belonging to a Job, with timeout.
func (jobMsg JobMessage) getJobPods(logCtx context.Context, jobName string) (*coreV1.Pod, error) {
	ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
	defer cancel()

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout waiting for pod of job %s in namespace %s", jobName, jobMsg.Job.Namespace)

		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
			defer cancel()
			pods, err := Clientset.CoreV1().Pods(jobMsg.Job.Namespace).List(ctx, metaV1.ListOptions{
				LabelSelector: fmt.Sprintf("job-name=%s", jobName),
			})
			if err != nil {
				logger.WarnCtx(logCtx, "Failed to list pods, will retry")
				continue
			}

			if pods != nil && len(pods.Items) > 0 {
				logger.InfoCtx(logCtx, "Found pod for job")
				return &pods.Items[0], nil
			}

			logger.InfoCtx(logCtx, "No pods found yet, still waiting")
		}
	}
}

func (jobMsg JobMessage) WatchJobCompletion(logCtx context.Context, jobName string) (err error) {
	lw := cache.NewListWatchFromClient(
		Clientset.BatchV1().RESTClient(),
		"jobs",
		jobMsg.Job.Namespace,
		fields.OneTermEqualSelector("metadata.name", jobName),
	)

	timeout := time.Duration(jobMsg.Job.ActiveDeadlineSeconds)
	ctx, cancel := context.WithTimeout(context.Background(), timeout*time.Second)
	defer cancel()
	stopCh := make(chan struct{})
	var once sync.Once

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &batchV1.Job{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj any) {
				job := newObj.(*batchV1.Job)
				for _, condition := range job.Status.Conditions {
					if condition.Type == batchV1.JobComplete && condition.Status == coreV1.ConditionTrue {
						logger.InfoCtx(logCtx, "job succeeded")
						once.Do(func() { close(stopCh) })
						return
					} else if condition.Type == batchV1.JobFailed && condition.Status == coreV1.ConditionTrue {
						logger.ErrorCtx(logCtx, "job failed, reason: %v, message: %v", condition.Reason, condition.Message)
						err = ErrJobFailed

						deletePolicy := metaV1.DeletePropagationForeground

						ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
						defer cancel()
						err = Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Delete(ctx, jobName, metaV1.DeleteOptions{
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
		logger.ErrorCtx(logCtx, "job completion timeout")
		err = ErrJobTimeout
		once.Do(func() { close(stopCh) })
	}
	return
}

// GetJobDetail retrieves the execution detail of a job.
func (jobMsg JobMessage) GetJobDetail(jobName string) (jobStatus int, duration time.Duration, err error) {
	job, err := jobMsg.GetJob(jobName)
	if err != nil {
		return
	}

	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		err = fmt.Errorf("get job status failed, job has no start or completion time")
		return
	}

	if job.Status.Succeeded > 0 {
		jobStatus = StatusJobCompleted
	} else {
		jobStatus = StatusJobFailed
	}

	duration = job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
	return
}

// GetJobFailureDetail retrieves failure details for a job and its pods.
func (jobMsg JobMessage) GetJobFailureDetail(jobName string) (*JobFailureDetail, error) {
	result := &JobFailureDetail{}

	job, err := jobMsg.GetJob(jobName)
	if err != nil {
		return nil, err
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type == batchV1.JobFailed && cond.Status == coreV1.ConditionTrue {
			result.JobReason = cond.Reason
			result.JobMessage = cond.Message
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
	defer cancel()
	pods, err := Clientset.CoreV1().Pods(jobMsg.Job.Namespace).List(ctx, metaV1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for job %s: %w", jobName, err)
	}

	for _, pod := range pods.Items {
		for _, status := range pod.Status.ContainerStatuses {
			if status.State.Terminated != nil {
				term := status.State.Terminated
				result.PodFailures = append(result.PodFailures, PodFailure{
					PodName:  pod.Name,
					ExitCode: term.ExitCode,
					Reason:   term.Reason,
					Message:  term.Message,
				})
			}
			if status.State.Waiting != nil {
				wait := status.State.Waiting
				result.PodFailures = append(result.PodFailures, PodFailure{
					PodName:  pod.Name,
					ExitCode: 0,
					Reason:   wait.Reason,
					Message:  wait.Message,
				})
			}
		}
	}

	return result, nil
}

// hashStringSHA256 hashes a string using SHA256 and returns the first 8 characters.
func hashStringSHA256(s string) string {
	h := sha256.New()
	h.Write([]byte(s))
	hashed := fmt.Sprintf("%x", h.Sum(nil))
	return hashed[:8]
}
