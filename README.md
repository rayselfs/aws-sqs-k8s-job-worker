# AWS SQS K8s Job Worker

## Architecture Diagram

```
+--------+        +-------------+        +-----------+        +-------------------+
| Client +------->+ SQS/Redis   +------->+ Job Worker+------->+  Kubernetes Job   |
+--------+        +-------------+        +-----------+        +-------------------+
     ^                                                                 |
     |                                                                 |
     +-------------------------------------------------<---------------+
                (Webhook callback: job result/status)
```

## Main Workflow

1. The client sends a job request to SQS or Redis queue.
2. The job worker pulls messages from the queue.
3. The job worker validates and parses the message, then creates the corresponding Kubernetes Job.
4. The job worker monitors the Job status and sends the result/status back to the client via webhook callback.
5. All important events and errors are logged.

---

## Project Description

This project is a worker that integrates AWS SQS/Redis queue with Kubernetes Job, supporting leader election, health checks, Prometheus monitoring, and callback.

- Supports Redis and AWS SQS as queue backends
- Executes tasks as Kubernetes Jobs, supporting resource, node, toleration, volume, and other parameters
- Supports webhook callback for job status reporting
- Provides health checks and Prometheus metrics
- Uses leader election to ensure only one worker processes the queue at a time

## Directory Structure

```
cmd/            # Entry point main.go
config/         # Environment variable configuration
internal/       # Main logic and components
  app/service/  # Business logic
  pkg/          # Shared components (k8s, queue, rdb, logger, prometheus, request)
  ...
build/          # Dockerfile and build-related files
example/        # Example yaml/json
```

## Main Environment Variables

| Variable Name                  | Description                        | Default/Required |
|-------------------------------|------------------------------------|------------------|
| QUEUE_TYPE                    | queue type (redis/sqs)              | redis            |
| LEADER_ELECTION_LOCK_NAME     | leader lock name                    | aws-sqs-job-worker-lock |
| POD_NAME                      | pod name                            | required         |
| POD_NAMESPACE                 | pod namespace                       | required         |
| LEADER_LOCK_NAME              | leader lock name                    | job-worker       |
| POLLING_INTERVAL              | queue polling interval (seconds)    | 5                |
| REDIS_ENDPOINT                | Redis connection address            | required         |
| REDIS_DB                      | Redis DB index                      | required         |
| REDIS_JOB_KEY_PREFIX          | Redis job key prefix                | job-worker-      |
| AWS_SQS_REGION                | SQS region                          | required         |
| AWS_SQS_URL                   | SQS queue url                       | required         |
| ACTIVE_DEADLINE_SECONDS_MAX   | Max job execution seconds           | 86400            |

## How to Start

1. Set up environment variables
2. Build/Run main.go
3. Refer to yaml/json in the example/ directory

---

1. SQS message json body

- id: unique
- service: job app label value
- job.prefixName: job prefix name
- job.namespace: job namespace
- job.image: job image
- job.command: container command with arg, only use arg for parameter
- job.ttlSecondsAfterFinished: seconds, automatic cleanup for finished jobs, need larger than 60s
- job.backoffLimit: specify the number of retries, set to 0
- job.activeDeadlineSeconds: seconds, the Job runtime exceeded the specified
- job.gpuEnable: use gpu vm
- job.resources: cpu, memory setting
- job.nodeSelector: job affinity with node selector
- job.toleration: allow the scheduler to schedule pods with matching taints (node)
- job.volume: mount folder
- webhook: callback

- job.nodeSelector: optional
- job.toleration: optional
- job.volume: optional
- webhook: optional

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "service": "example",
  "job": {
    "prefixName": "bb",
    "namespace": "default",
    "image": "busybox",
    "command": ["/bin/sh", "-c", "echo Hello World && sleep 30 && exit 0"],
    "serviceAccount": "default",
    "ttlSecondsAfterFinished": 60,
    "backoffLimit": 0,
    "activeDeadlineSeconds": 600,
    "gpuEnable": false,
    "resources": {
      "limits": {
        "cpu": "1",
        "memory": "512Mi"
      },
      "requests": {
        "cpu": "500m",
        "memory": "256Mi"
      }
    },
    "nodeSelector": {
      "matchKey": "kubernetes.io/hostname",
      "matchValue": "k8s-node-1"
    },
    "toleration": {
      "key": "key",
      "value": "value"
    },
    "fargate": {
      "key": "WorkerType",
      "value": "fargate"
    },
    "volume": {
      "mountPath": "workdir",
      "pvc": "ps-pvc"
    }
  },
  "webhook": {
    "url": "http://webhook.com"
  }
}
```

1. Callback API body

- id: sqs message body id
- status
  - 0: start running job
  - 1: start running pod
  - 2: job complete
  - 3: job failed
  - 99: job exception
- detail: more information

2. status 0

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 0,
  "detail": {
    "jobId": "08c87282-97e8-4993-9da4-359f3e84fdfd",
    "jobName": "bb-fbe70e59"
  }
}
```

3. status 1

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 1,
  "detail": {
    "podId": "08c87282-97e8-4993-9da4-359f3e84fdfd",
    "podName": "bb-fbe70e59-ww7g2"
  }
}
```

4. status 2

- duration: seconds, job execution time

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 2,
  "detail": {
    "duration": 0
  }
}
```

5. status 3 or 99

- errorCode: error code
- errorMessage: error detail

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 3,
  "detail": {
    "error": {
      "errorCode": "ERROR_CODE",
      "message": "error message"
    }
  }
}
```
