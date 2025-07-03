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
- job.toleration optional
- job.volume optional
- webhook optional

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
    "volume:": {
      "mountPath": "workdir",
      "pvc": "ps-pvc"
    }
  },
  "webhook": {
    "url": "http://webhook.com"
  }
}
```

1. Callback api body

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
