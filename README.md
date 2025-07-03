# AWS SQS K8s Job Worker

## 架構說明

本專案為一個將 AWS SQS/Redis queue 與 Kubernetes Job 整合的 worker，支援領導選舉、健康檢查、Prometheus 監控與 callback。

- 支援 Redis 及 AWS SQS 作為 queue backend
- 以 Kubernetes Job 執行任務，並支援資源、節點、toleration、volume 等參數
- 支援 webhook callback 回報任務狀態
- 具備健康檢查與 Prometheus metrics
- 以 leader election 機制確保同時僅一個 worker 處理 queue

## 目錄結構

```
cmd/            # 入口 main.go
config/         # 環境變數設定
internal/       # 主要邏輯與元件
  app/service/  # 業務邏輯
  pkg/          # 各種共用元件 (k8s, queue, rdb, logger, prometheus, request)
  ...
build/          # Dockerfile 等建置相關
example/        # 範例 yaml/json
```

## 主要環境變數

| 變數名稱                      | 說明                        | 預設值/必填 |
|------------------------------|-----------------------------|-------------|
| QUEUE_TYPE                   | queue 類型(redis/sqs)       | redis       |
| LEADER_ELECTION_LOCK_NAME    | leader lock 名稱            | aws-sqs-job-worker-lock |
| POD_NAME                     | pod 名稱                    | 必填        |
| POD_NAMESPACE                | pod namespace               | 必填        |
| LEADER_LOCK_NAME             | leader lock 名稱            | job-worker  |
| POLLING_INTERVAL             | queue 輪詢間隔(秒)          | 5           |
| REDIS_ENDPOINT               | Redis 連線位址              | 必填        |
| REDIS_DB                     | Redis DB index              | 必填        |
| REDIS_JOB_KEY_PREFIX         | Redis job key 前綴          | job-worker- |
| AWS_SQS_REGION               | SQS region                  | 必填        |
| AWS_SQS_URL                  | SQS queue url               | 必填        |
| ACTIVE_DEADLINE_SECONDS_MAX  | Job 最長執行秒數            | 86400       |

## 啟動方式

1. 設定好環境變數
2. 編譯/啟動 main.go
3. 參考 example/ 目錄下的 yaml/json

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
