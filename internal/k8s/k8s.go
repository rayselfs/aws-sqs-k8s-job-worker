package k8s

import (
	"aws-sqs-k8s-job-worker/config"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"aws-sqs-k8s-job-worker/internal/logger"

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

type CallbackStatus int

const pvcName = "persistent-storage"

const (
	StatusJobCreated CallbackStatus = iota
	StatusPodRunning
	StatusJobCompleted
	StatusJobFailed
	StatusException
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
	PrefixName              string        `json:"prefixName" validate:"required"`                              // Job name prefix
	Namespace               string        `json:"namespace" validate:"required"`                               // Namespace
	TTLSecondsAfterFinished int32         `json:"ttlSecondsAfterFinished" validate:"required,gte=60,lte=120"`  // TTL after job finished
	ActiveDeadlineSeconds   int64         `json:"activeDeadlineSeconds" validate:"required,gte=120,lte=86400"` // Max job duration
	BackoffLimit            int32         `json:"backoffLimit"`                                                // Retry limit
	Image                   string        `json:"image" validate:"required"`                                   // Container image
	Command                 []string      `json:"command" validate:"required"`                                 // Command to run
	Resources               *Resources    `json:"resources"`                                                   // Resource requests/limits
	ServiceAccount          *string       `json:"serviceAccount"`                                              // Service account
	Volume                  *Volume       `json:"volume"`                                                      // Volume mount
	NodeSelector            *NodeSelector `json:"nodeSelector"`                                                // Node selector
	Toleration              *Toleration   `json:"toleration"`                                                  // Toleration
	GpuEnable               bool          `json:"gpuEnable"`                                                   // Use GPU
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

// Webhook defines a callback URL for job status updates.
type Webhook struct {
	URL string `json:"url" validate:"required"`
}

// JobFailureDetail holds job and pod failure reasons/messages.
type JobFailureDetail struct {
	JobReason   string
	JobMessage  string
	PodFailures []PodFailure
}

// PodFailure holds details about a failed pod.
type PodFailure struct {
	PodName   string
	Container string
	ExitCode  int32
	Reason    string
	Message   string
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
func (jobMsg JobMessage) ApplyJob(ctx context.Context, jobName string) error {
	_, err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Create(ctx, jobMsg.getJobSpec(jobName), metaV1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create job %s: %w", jobName, err)
	}

	return nil
}

// GetJob retrieves a Job by name from the specified namespace.
func (jobMsg JobMessage) GetJob(ctx context.Context, jobName string) (*batchV1.Job, error) {
	return Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Get(ctx, jobName, metaV1.GetOptions{})
}

// getJobPods waits for the first pod belonging to a Job, with timeout.
func (jobMsg JobMessage) getJobPods(ctx context.Context, jobName string) (*coreV1.Pod, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for pod: %w", timeoutCtx.Err())
		default:
			pods, err := Clientset.CoreV1().Pods(jobMsg.Job.Namespace).List(timeoutCtx, metaV1.ListOptions{
				LabelSelector: fmt.Sprintf("job-name=%s", jobName),
			})
			if err != nil {
				return nil, fmt.Errorf("failed to list pods for job %s: %w", jobName, err)
			}

			if len(pods.Items) > 0 {
				return &pods.Items[0], nil
			}

			time.Sleep(2 * time.Second)
		}
	}
}

// JobExists checks if a job with the given name already exists in the specified namespace.
func (jobMsg JobMessage) JobExists(ctx context.Context, jobName string) (bool, error) {
	_, err := jobMsg.GetJob(ctx, jobName)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			return false, nil
		}

		logger.Error("Failed to get job: %s", err.Error())
		return false, fmt.Errorf("failed to get job %s: %w", jobName, err)
	}
	return true, nil
}

func (jobMsg JobMessage) JobCheck(ctx context.Context, jobName string) error {
	exist, err := jobMsg.JobExists(ctx, jobName)
	if err != nil {
		err = fmt.Errorf("failed to check job existence: %w", err)
		return err
	}
	if !exist {
		err = fmt.Errorf("job %s not exists", jobName)
		return err
	}
	return nil
}

// WatchPodRunning watches for the running phase of a specific pod.
func (jobMsg JobMessage) WatchPodRunning(ctx context.Context, jobName string) (*coreV1.Pod, error) {
	err := jobMsg.JobCheck(ctx, jobName)
	if err != nil {
		return nil, err
	}

	pod, err := jobMsg.getJobPods(ctx, jobName)
	if err != nil {
		return nil, err
	}

	lw := cache.NewListWatchFromClient(
		Clientset.CoreV1().RESTClient(),
		"pods",
		jobMsg.Job.Namespace,
		fields.OneTermEqualSelector("metadata.name", pod.Name),
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, config.Env.PodStartTimeoutDuration)
	defer cancel()

	errCh := make(chan error, 1)

	handle := func(p *coreV1.Pod) {
		if timeoutCtx.Err() != nil {
			return
		}
		if p.Status.Phase == coreV1.PodRunning {
			logger.InfoCtx(timeoutCtx, "pod running")
			select {
			case errCh <- nil:
			default:
			}
		}
	}

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &coreV1.Pod{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { handle(obj.(*coreV1.Pod)) },
			UpdateFunc: func(_, newObj any) { handle(newObj.(*coreV1.Pod)) },
		},
	})

	go controller.Run(timeoutCtx.Done())

	select {
	case err = <-errCh:
		cancel()
		return pod, nil
	case <-timeoutCtx.Done():
		logger.ErrorCtx(timeoutCtx, "pod start timeout")
		return nil, ErrPodStartTimeout
	}
}

// WatchJobCompletion watches for the completion of a Job and handles success/failure conditions.
func (jobMsg JobMessage) WatchJobCompletion(ctx context.Context, jobName string) error {
	err := jobMsg.JobCheck(ctx, jobName)
	if err != nil {
		return err
	}

	lw := cache.NewListWatchFromClient(
		Clientset.BatchV1().RESTClient(),
		"jobs",
		jobMsg.Job.Namespace,
		fields.OneTermEqualSelector("metadata.name", jobName),
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(jobMsg.Job.ActiveDeadlineSeconds)*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	var once sync.Once

	handle := func(job *batchV1.Job) {
		if timeoutCtx.Err() != nil {
			return
		}

		for _, cond := range job.Status.Conditions {
			switch {
			case cond.Type == batchV1.JobComplete && cond.Status == coreV1.ConditionTrue:
				logger.InfoCtx(timeoutCtx, "job succeeded")
				once.Do(func() {
					select {
					case errCh <- nil:
					case <-timeoutCtx.Done():
					}
					cancel()
				})

			case cond.Type == batchV1.JobFailed && cond.Status == coreV1.ConditionTrue:
				logger.ErrorCtx(timeoutCtx, "job failed: %s - %s", cond.Reason, cond.Message)
				go func() {
					deleteCtx, deleteCancel := context.WithTimeout(context.Background(), config.Env.KubernetesClientDuration)
					defer deleteCancel()

					err := Clientset.BatchV1().Jobs(jobMsg.Job.Namespace).Delete(deleteCtx, jobName, metaV1.DeleteOptions{})
					if err != nil {
						logger.ErrorCtx(deleteCtx, "failed to delete failed job: %v", err)
					}
				}()

				once.Do(func() {
					select {
					case errCh <- ErrJobFailed:
					case <-timeoutCtx.Done():
					}
					cancel()
				})
			}
		}
	}

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: lw,
		ObjectType:    &batchV1.Job{},
		ResyncPeriod:  0,
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj any) { handle(obj.(*batchV1.Job)) },
			UpdateFunc: func(_, newObj any) { handle(newObj.(*batchV1.Job)) },
		},
	})

	go controller.Run(timeoutCtx.Done())

	select {
	case err := <-errCh:
		return err
	case <-timeoutCtx.Done():
		logger.ErrorCtx(timeoutCtx, "job completion timeout")
		return ErrJobTimeout
	}
}

// GetJobDetail retrieves the execution detail of a job.
func (jobMsg JobMessage) GetJobDetail(ctx context.Context, jobName string) (jobStatus CallbackStatus, duration time.Duration, err error) {
	err = jobMsg.JobCheck(ctx, jobName)
	if err != nil {
		return
	}

	job, err := jobMsg.GetJob(ctx, jobName)
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
func (jobMsg JobMessage) GetJobFailureDetail(ctx context.Context, jobName string) (*JobFailureDetail, error) {
	err := jobMsg.JobCheck(ctx, jobName)
	if err != nil {
		return nil, err
	}

	result := &JobFailureDetail{}
	job, err := jobMsg.GetJob(ctx, jobName)
	if err != nil {
		return nil, fmt.Errorf("failed to get job %s: %w", jobName, err)
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type == batchV1.JobFailed && cond.Status == coreV1.ConditionTrue {
			result.JobReason = cond.Reason
			result.JobMessage = cond.Message
		}
	}

	pods, err := Clientset.CoreV1().Pods(jobMsg.Job.Namespace).List(ctx, metaV1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for job %s: %w", jobName, err)
	}

	for _, pod := range pods.Items {
		for _, status := range pod.Status.ContainerStatuses {
			switch {
			case status.State.Terminated != nil:
				term := status.State.Terminated
				result.PodFailures = append(result.PodFailures, PodFailure{
					PodName:   pod.Name,
					Container: status.Name,
					ExitCode:  term.ExitCode,
					Reason:    term.Reason,
					Message:   term.Message,
				})
			case status.State.Waiting != nil && status.State.Waiting.Reason != "ContainerCreating":
				wait := status.State.Waiting
				result.PodFailures = append(result.PodFailures, PodFailure{
					PodName:   pod.Name,
					Container: status.Name,
					ExitCode:  0,
					Reason:    wait.Reason,
					Message:   wait.Message,
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
	return fmt.Sprintf("%x", h.Sum(nil))[:8]
}
