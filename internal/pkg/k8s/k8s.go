package k8s

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"aws-sqs-k8s-job-worker/internal/app/callback"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"

	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

// Clientset is the global Kubernetes clientset instance.
type Client struct {
	Clientset *kubernetes.Clientset
	Config    *Config
}

type Config struct {
	PodStartTimeout time.Duration
	ClientTimeout   time.Duration
}

// Common errors for job and pod operations.
var (
	ErrPodStartTimeout = errors.New("pod start timeout")      // Pod did not start in time
	ErrJobTimeout      = errors.New("job completion timeout") // Job did not complete in time
	ErrJobFailed       = errors.New("job failed")             // Job failed
)

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
func New() (*kubernetes.Clientset, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	return kubernetes.NewForConfig(config)
}

// JobGet retrieves a Job by name from the specified namespace.
func (c *Client) JobGet(ctx context.Context, namespace string, name string) (*batchV1.Job, error) {
	return c.Clientset.BatchV1().Jobs(namespace).Get(ctx, name, metaV1.GetOptions{})
}

// JobCreate creates a Kubernetes Job in the specified namespace.
func (c *Client) JobCreate(ctx context.Context, namespace string, job *batchV1.Job) error {
	_, err := c.Clientset.BatchV1().Jobs(namespace).Create(ctx, job, metaV1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create job %s: %w", job.Name, err)
	}

	return nil
}

// JobExists checks if a job with the given name already exists in the specified namespace.
func (c *Client) JobExists(ctx context.Context, namespace string, name string) (bool, error) {
	_, err := c.JobGet(ctx, namespace, name)
	if err != nil {
		if k8sErrors.IsNotFound(err) {
			return false, nil
		}

		logger.Error("Failed to get job: %s", err.Error())
		return false, fmt.Errorf("failed to get job %s: %w", name, err)
	}
	return true, nil
}

// JobCheck checks if job not exist, return an error
func (c *Client) JobCheck(ctx context.Context, namespace string, name string) error {
	exist, err := c.JobExists(ctx, namespace, name)
	if err != nil {
		err = fmt.Errorf("failed to check job existence: %w", err)
		return err
	}
	if !exist {
		err = fmt.Errorf("job %s not exists", name)
		return err
	}
	return nil
}

// JobPodsGet waits for the first pod belonging to a Job, with timeout.
func (c *Client) JobPodsGet(ctx context.Context, namespace string, name string) (*coreV1.Pod, error) {
	logger.InfoCtx(ctx, "waiting for pod creation for job %s in namespace %s", name, namespace)

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	for {
		select {
		case <-timeoutCtx.Done():
			logger.ErrorCtx(ctx, "timed out waiting for pod for job %s", name)
			return nil, fmt.Errorf("timed out waiting for pod: %w", timeoutCtx.Err())
		default:
			pods, err := c.Clientset.CoreV1().Pods(namespace).List(timeoutCtx, metaV1.ListOptions{
				LabelSelector: fmt.Sprintf("job-name=%s", name),
			})
			if err != nil {
				logger.ErrorCtx(ctx, "failed to list pods for job %s: %v", name, err)
				return nil, fmt.Errorf("failed to list pods for job %s: %w", name, err)
			}

			logger.InfoCtx(ctx, "found %d pods for job %s", len(pods.Items), name)

			if len(pods.Items) > 0 {
				pod := &pods.Items[0]
				logger.InfoCtx(ctx, "using pod %s for job %s, phase: %s", pod.Name, name, pod.Status.Phase)
				return pod, nil
			}

			logger.InfoCtx(ctx, "no pods found for job %s, retrying in 2 seconds", name)
			time.Sleep(2 * time.Second)
		}
	}
}

// JobPodRunningWatch watches for the running phase of a specific pod.
func (c *Client) JobPodRunningWatch(ctx context.Context, namespace string, name string) (*coreV1.Pod, error) {
	logger.InfoCtx(ctx, "starting pod running watch for job %s in namespace %s", name, namespace)

	err := c.JobCheck(ctx, namespace, name)
	if err != nil {
		logger.ErrorCtx(ctx, "job check failed: %v", err)
		return nil, err
	}

	pod, err := c.JobPodsGet(ctx, namespace, name)
	if err != nil {
		logger.ErrorCtx(ctx, "failed to get pod for job %s: %v", name, err)
		return nil, err
	}

	logger.InfoCtx(ctx, "found pod %s for job %s, current phase: %s", pod.Name, name, pod.Status.Phase)

	// Check if pod is already running
	if pod.Status.Phase == coreV1.PodRunning {
		logger.InfoCtx(ctx, "pod %s is already running", pod.Name)
		return pod, nil
	}

	lw := cache.NewListWatchFromClient(
		c.Clientset.CoreV1().RESTClient(),
		"pods",
		namespace,
		fields.OneTermEqualSelector("metadata.name", pod.Name),
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, c.Config.PodStartTimeout)
	defer cancel()

	logger.InfoCtx(ctx, "watching pod %s for running state, timeout: %v", pod.Name, c.Config.PodStartTimeout)

	errCh := make(chan error, 1)
	podCh := make(chan *coreV1.Pod, 1)

	handle := func(p *coreV1.Pod) {
		if timeoutCtx.Err() != nil {
			return
		}
		logger.InfoCtx(timeoutCtx, "pod %s status update: phase=%s", p.Name, p.Status.Phase)
		switch p.Status.Phase {
		case coreV1.PodRunning:
			logger.InfoCtx(timeoutCtx, "pod %s is now running", p.Name)
			select {
			case podCh <- p:
			case errCh <- nil:
			default:
			}
		case coreV1.PodFailed:
			// 嘗試從多個來源獲取錯誤資訊
			reason := p.Status.Reason
			message := p.Status.Message

			// 如果 Reason 為空，嘗試從 ContainerStatuses 獲取錯誤資訊
			if reason == "" && len(p.Status.ContainerStatuses) > 0 {
				for _, containerStatus := range p.Status.ContainerStatuses {
					if containerStatus.State.Terminated != nil {
						if containerStatus.State.Terminated.Reason != "" {
							reason = containerStatus.State.Terminated.Reason
						}
						if containerStatus.State.Terminated.Message != "" {
							message = containerStatus.State.Terminated.Message
						}
						break
					}
				}
			}

			// 組合錯誤訊息
			errorMsg := "pod failed"
			if reason != "" {
				errorMsg += fmt.Sprintf(" (reason: %s)", reason)
			}
			if message != "" {
				errorMsg += fmt.Sprintf(" (message: %s)", message)
			}

			logger.ErrorCtx(timeoutCtx, "pod %s failed: %s", p.Name, errorMsg)
			select {
			case errCh <- fmt.Errorf("%s", errorMsg):
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
	case runningPod := <-podCh:
		logger.InfoCtx(ctx, "pod %s successfully reached running state", runningPod.Name)
		cancel()
		return runningPod, nil
	case err = <-errCh:
		cancel()
		if err != nil {
			return nil, err
		}
		return pod, nil
	case <-timeoutCtx.Done():
		logger.ErrorCtx(ctx, "pod start timeout for pod %s after %v", pod.Name, c.Config.PodStartTimeout)
		return nil, ErrPodStartTimeout
	}
}

// JobCompletionWatch watches for the completion of a Job and handles success/failure conditions.
func (c *Client) JobCompletionWatch(ctx context.Context, namespace string, name string, timeoutSeconds int64) error {
	err := c.JobCheck(ctx, namespace, name)
	if err != nil {
		return err
	}

	lw := cache.NewListWatchFromClient(
		c.Clientset.BatchV1().RESTClient(),
		"jobs",
		namespace,
		fields.OneTermEqualSelector("metadata.name", name),
	)

	timeoutCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
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
				// 組合詳細的錯誤訊息
				errorMsg := "job failed"
				if cond.Reason != "" {
					errorMsg += fmt.Sprintf(" (reason: %s)", cond.Reason)
				}
				if cond.Message != "" {
					errorMsg += fmt.Sprintf(" (message: %s)", cond.Message)
				}

				logger.ErrorCtx(timeoutCtx, "job failed: %s", errorMsg)
				go func() {
					deleteCtx, deleteCancel := context.WithTimeout(ctx, c.Config.ClientTimeout)
					defer deleteCancel()

					err := c.Clientset.BatchV1().Jobs(namespace).Delete(deleteCtx, name, metaV1.DeleteOptions{})
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

// JobDetailGet retrieves the execution detail of a job.
func (c *Client) JobDetailGet(ctx context.Context, namespace string, name string) (jobStatus callback.Status, duration time.Duration, err error) {
	err = c.JobCheck(ctx, namespace, name)
	if err != nil {
		return
	}

	job, err := c.JobGet(ctx, namespace, name)
	if err != nil {
		return
	}

	if job.Status.StartTime == nil || job.Status.CompletionTime == nil {
		err = fmt.Errorf("get job status failed, job has no start or completion time")
		return
	}

	if job.Status.Succeeded > 0 {
		jobStatus = callback.StatusJobCompleted
	} else {
		jobStatus = callback.StatusJobFailed
	}

	duration = job.Status.CompletionTime.Sub(job.Status.StartTime.Time)
	return
}

// GetJobFailureDetail retrieves failure details for a job and its pods.
func (c *Client) GetJobFailureDetail(ctx context.Context, namespace string, name string) (*JobFailureDetail, error) {
	err := c.JobCheck(ctx, namespace, name)
	if err != nil {
		return nil, err
	}

	result := &JobFailureDetail{}
	job, err := c.JobGet(ctx, namespace, name)
	if err != nil {
		return nil, fmt.Errorf("failed to get job %s: %w", name, err)
	}

	for _, cond := range job.Status.Conditions {
		if cond.Type == batchV1.JobFailed && cond.Status == coreV1.ConditionTrue {
			result.JobReason = cond.Reason
			result.JobMessage = cond.Message
		}
	}

	pods, err := c.Clientset.CoreV1().Pods(namespace).List(ctx, metaV1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", name),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods for job %s: %w", name, err)
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
