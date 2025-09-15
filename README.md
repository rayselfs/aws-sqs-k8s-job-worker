# AWS SQS K8s Job Worker

## Architecture Diagram

```
+--------+        +-------------+        +-----------+        +-------------------+
| Client +------->+ SQS/Redis   +------->+ Job Worker+------->+  Kubernetes Job   |
+--------+        +-------------+        +-----------+        +-------------------+
     ^                                                               |
     |                                                               |
     +------------------------------ callback webhook <--------------+
```

## Main Workflow

1. The client sends a job request to SQS or Redis queue.
2. The job worker pulls messages from the queue.
3. The job worker validates and parses the message, then creates the corresponding Kubernetes Job.
4. The job worker monitors the Job status and **reports the result or error** back to the client via webhook callback.
5. All important events and errors are logged.

---

## Project Description

This project is a worker that integrates AWS SQS/Redis queue with Kubernetes Job, supporting leader election, health checks, Prometheus monitoring, and webhook callbacks.

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
build/          # Dockerfile and build-related files
example/        # Example yaml/json
```

## Main Environment Variables

| Variable Name               | Description                      | Default/Required        |
| --------------------------- | -------------------------------- | ----------------------- |
| QUEUE_TYPE                  | queue type (redis/sqs)           | redis                   |
| LEADER_ELECTION_LOCK_NAME   | leader election lock name        | aws-sqs-job-worker-lock |
| POD_NAME                    | pod name                         | required                |
| POD_NAMESPACE               | pod namespace                    | required                |
| POLLING_INTERVAL            | queue polling interval (seconds) | 5                       |
| REDIS_ENDPOINT              | Redis connection address         | required                |
| REDIS_DB                    | Redis DB index                   | required                |
| REDIS_JOB_KEY_PREFIX        | Redis job key prefix             | job-worker-             |
| AWS_SQS_REGION              | SQS region                       | required                |
| AWS_SQS_URL                 | SQS queue url                    | required                |
| ACTIVE_DEADLINE_SECONDS_MAX | Max job execution seconds        | 86400                   |

## How to Start

1. Set up environment variables
2. Build/Run main.go, e.g.
   ```
   go run ./cmd
   ```
3. Refer to yaml/json in the example/ directory

---

## SQS message JSON body

- id: unique
- service: job app label value
- job.prefixName: job prefix name
- job.namespace: job namespace
- job.image: job image
- job.command: container command (optional)
- job.args: container args (optional)
- job.serviceAccount: service account name (need create first, or use client sa, optional)
- job.ttlSecondsAfterFinished: seconds, automatic cleanup for finished jobs (must be > 30s)
- job.backoffLimit: specify the number of retries, recommended 0
- job.activeDeadlineSeconds: maximum runtime for the Job in seconds (60 ~ 86400)
- job.gpuEnable: whether to use GPU node (optional)
- job.gpuNumber: deployment resources nvidia.com/gpu number, which represents how many GPUs are required (optional)
- job.env: deployment env (optional)
- job.envFrom: deployment envFrom, type list: configmap, secret (optional)
- job.resources: cpu, memory setting (optional)
- job.nodeSelector: node affinity selector (optional)
- job.toleration: toleration for node taints (optional)
- job.volume: mount folder (optional)
- webhook: callback config (optional)

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "service": "example",
  "job": {
    "prefixName": "bb",
    "namespace": "default",
    "image": "busybox",
    "command": ["/bin/sh", "-c", "echo Hello World && sleep 30 && exit 0"],
    "args": [],
    "serviceAccount": "default",
    "ttlSecondsAfterFinished": 60,
    "backoffLimit": 0,
    "activeDeadlineSeconds": 600,
    "gpuEnable": false,
    "gpuNumber": 1,
    "env": [
      {
        "name": "Test1",
        "value": "Hello from a static variable!" 
      }
    ],
    "envRef": [
        {
          "name": "busybox-default",
          "type": "configmap"
        }
    ],
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

---

## Callback API body

- id: sqs message id
- status:
  - 0: job resource created
  - 1: pod started
  - 2: job complete
  - 3: job failed
  - 99: job exception
- detail: extra details depending on status

## Example callback

**status 0** (job created)

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

**status 1** (pod started)

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

**status 2** (job completed)

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 2,
  "detail": {
    "duration": 120
  }
}
```
> duration: seconds

**status 3 or 99** (job failed / exception)

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
