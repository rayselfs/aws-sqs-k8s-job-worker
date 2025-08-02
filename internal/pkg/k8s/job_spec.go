package k8s

import (
	"fmt"
	"strconv"

	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const pvcName = "persistent-storage"

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
	GpuNumber               *int          `json:"gpuNumber" validate:"gte=1"`                                  // Number of GPUs
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
func (jobMsg JobMessage) GetJobSpec(jobName string) *batchV1.Job {
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

	// 初始化資源限制
	resources := coreV1.ResourceRequirements{
		Limits:   coreV1.ResourceList{},
		Requests: coreV1.ResourceList{},
	}

	// 加入 CPU / Memory
	if jobMsg.Job.Resources != nil {
		if jobMsg.Job.Resources.Limits.CPU != "" {
			resources.Limits["cpu"] = resource.MustParse(jobMsg.Job.Resources.Limits.CPU)
		}
		if jobMsg.Job.Resources.Limits.Memory != "" {
			resources.Limits["memory"] = resource.MustParse(jobMsg.Job.Resources.Limits.Memory)
		}
		if jobMsg.Job.Resources.Requests.CPU != "" {
			resources.Requests["cpu"] = resource.MustParse(jobMsg.Job.Resources.Requests.CPU)
		}
		if jobMsg.Job.Resources.Requests.Memory != "" {
			resources.Requests["memory"] = resource.MustParse(jobMsg.Job.Resources.Requests.Memory)
		}
	}

	// 加入 GPU
	if jobMsg.Job.GpuEnable {
		gpuNumber := 1
		if n := jobMsg.Job.GpuNumber; n != nil {
			gpuNumber = *n
		}
		gpuQty := resource.MustParse(strconv.Itoa(gpuNumber))
		resources.Limits["nvidia.com/gpu"] = gpuQty
	}

	// 如果有任何資源需求才設定
	if len(resources.Limits) > 0 || len(resources.Requests) > 0 {
		mainSpec.Resources = resources
	}

	// 加入 VolumeMount
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
