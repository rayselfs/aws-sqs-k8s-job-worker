# Kubernetes Queue Job Worker

A worker node that consumes tasks from message queues and creates/manages Jobs in Kubernetes clusters. Supports multiple queue backends, leader election, health checks, Prometheus monitoring, and webhook callbacks.

## 📋 Table of Contents

- [Architecture Overview](#architecture-overview)
- [Features](#features)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Message Format](#message-format)
- [Callback API](#callback-api)
- [Project Structure](#project-structure)
- [Deployment Guide](#deployment-guide)
- [Monitoring & Observability](#monitoring--observability)

## 🏗️ Architecture Overview

```
┌─────────┐        ┌─────────────┐        ┌──────────────┐        ┌──────────────────┐
│ Client  │───────>│   Queue     │───────>│ Job Worker   │───────>│ Kubernetes Job   │
└─────────┘        └─────────────┘        └──────────────┘        └──────────────────┘
     ▲                                                                        │
     │                                                                        │
     └─────────────────────────────── Webhook Callback <─────────────────────┘
```

### Workflow

1. **Message Reception**: Client sends job requests to the message queue
2. **Message Consumption**: Job Worker pulls messages from the queue
3. **Job Creation**: Worker validates and parses messages, then creates corresponding Kubernetes Jobs
4. **Status Monitoring**: Worker monitors Job status and reports results or errors back to the client via webhook callbacks
5. **Logging**: All important events and errors are logged

## ✨ Features

- 🔄 **Multiple Queue Support**: Supports multiple message queue backends (extensible)
- 🎯 **Kubernetes Job Management**: Automatically creates and manages Kubernetes Jobs with full Job configuration options
- 🔐 **Leader Election**: Uses Kubernetes Leader Election to ensure only one Worker processes the queue at a time
- 📊 **Monitoring Metrics**: Provides Prometheus metrics for monitoring and alerting
- 🔔 **Webhook Callbacks**: Supports callback notifications on task status changes
- 🏥 **Health Checks**: Provides HTTP health check endpoints
- 🔄 **Job Recovery**: Supports recovery of incomplete jobs from cache
- ⚡ **Concurrent Processing**: Supports worker pool pattern for improved processing efficiency

## 🚀 Quick Start

### Prerequisites

- Kubernetes cluster (1.20+)
- Message queue service (depending on configured queue type)
- Redis (for caching and deduplication)
- Go 1.24+ (for local development)

### Local Run

1. **Clone the repository**

```bash
git clone <repository-url>
cd k8s-queue-job-worker
```

2. **Set environment variables**

```bash
export POD_NAME=local-worker
export POD_NAMESPACE=default
export CACHE_REDIS_ENDPOINT=localhost:6379
export CACHE_REDIS_DB=0
# ... other required environment variables
```

3. **Run the application**

```bash
go run ./cmd/main.go
```

### Docker Run

```bash
docker build -t k8s-queue-job-worker:latest -f build/Dockerfile .
docker run -e POD_NAME=worker-1 -e POD_NAMESPACE=default ... k8s-queue-job-worker:latest
```

## ⚙️ Configuration

### Environment Variables

| Variable Name | Description | Default | Required |
|--------------|-------------|---------|----------|
| **Basic Configuration** |
| `POD_NAME` | Pod name | - | ✅ |
| `POD_NAMESPACE` | Pod namespace | - | ✅ |
| `LEADER_ELECTION_LOCK_NAME` | Leader Election lock name | `job-worker-lock` | ❌ |
| `POLLING_INTERVAL` | Queue polling interval (seconds) | `5` | ❌ |
| **HTTP Server** |
| `HTTP_SERVER_PORT` | HTTP server port | `8080` | ❌ |
| `HTTP_SERVER_TIMEOUT_SECONDS` | HTTP server timeout (seconds) | `30` | ❌ |
| **Queue Configuration** |
| `QUEUE_TYPE` | Queue type | `redis` | ❌ |
| `QUEUE_WORKER_POOL_SIZE` | Worker pool size (1-10) | `10` | ❌ |
| **Redis Queue** |
| `REDIS_QUEUE_ENDPOINT` | Redis queue connection address | - | Conditional* |
| `REDIS_QUEUE_DB` | Redis queue database index | `0` | ❌ |
| `REDIS_QUEUE_KEY_PREFIX` | Redis queue key prefix | `queue-` | ❌ |
| **Cache Configuration** |
| `CACHE_REDIS_ENDPOINT` | Redis cache connection address | - | ✅ |
| `CACHE_REDIS_DB` | Redis cache database index | - | ✅ |
| `CACHE_JOB_KEY_PREFIX` | Cache Job key prefix | `job-worker-` | ❌ |
| **Kubernetes Configuration** |
| `KUBERNETES_CLIENT_TIMEOUT` | Kubernetes client timeout (seconds) | `30` | ❌ |
| `POD_START_TIMEOUT` | Pod start timeout (seconds) | `600` | ❌ |

\* Required when `QUEUE_TYPE=redis`

### Configuration Validation

The application automatically validates configuration on startup:
- `POD_START_TIMEOUT` must be greater than 0
- `QUEUE_TYPE` must be a supported type
- `QUEUE_WORKER_POOL_SIZE` must be between 1-10
- Required configuration items are validated based on `QUEUE_TYPE`

## 📨 Message Format

### Message Structure

Messages sent to the queue must be in JSON format with the following fields:

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "service": "example",
  "job": {
    "prefixName": "my-job",
    "namespace": "default",
    "image": "busybox:latest",
    "command": ["/bin/sh", "-c", "echo Hello World"],
    "args": [],
    "serviceAccount": "default",
    "ttlSecondsAfterFinished": 60,
    "backoffLimit": 0,
    "activeDeadlineSeconds": 600,
    "gpuEnable": false,
    "gpuNumber": 1,
    "env": [
      {
        "name": "ENV_VAR",
        "value": "value"
      }
    ],
    "envRef": [
      {
        "name": "configmap-name",
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
      "mountPath": "/workdir",
      "pvc": "persistent-volume-claim"
    }
  },
  "webhook": {
    "url": "http://webhook.example.com/callback"
  }
}
```

### Field Descriptions

#### Root-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | ✅ | Unique task identifier |
| `service` | string | ✅ | Service label for Job's app label |
| `job` | object | ✅ | Job configuration object |
| `webhook` | object | ❌ | Webhook callback configuration |

#### Job Configuration Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `prefixName` | string | ✅ | Job name prefix |
| `namespace` | string | ✅ | Job namespace |
| `image` | string | ✅ | Container image |
| `command` | array | ❌ | Container command |
| `args` | array | ❌ | Container arguments |
| `serviceAccount` | string | ❌ | ServiceAccount name |
| `ttlSecondsAfterFinished` | int | ❌ | Auto-cleanup time after Job completion (seconds, must be > 30) |
| `backoffLimit` | int | ❌ | Number of retries, recommended 0 |
| `activeDeadlineSeconds` | int | ❌ | Maximum Job runtime (seconds, 60-86400) |
| `gpuEnable` | bool | ❌ | Whether to enable GPU |
| `gpuNumber` | int | ❌ | Number of GPUs (1-4) |
| `env` | array | ❌ | Environment variable list |
| `envRef` | array | ❌ | Environment variables from ConfigMap/Secret |
| `resources` | object | ❌ | Resource limits and requests |
| `nodeSelector` | object | ❌ | Node selector |
| `toleration` | object | ❌ | Toleration configuration |
| `volume` | object | ❌ | Volume mount configuration |

## 🔔 Callback API

When task status changes, the Worker sends HTTP POST requests to the configured Webhook URL.

### Callback Request Format

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 0,
  "detail": {
    // Detail content varies based on status
  }
}
```

### Status Codes

| Status Code | Description | Detail Content |
|-------------|-------------|----------------|
| `0` | Job resource created | `jobId`, `jobName` |
| `1` | Pod started | `podId`, `podName` |
| `2` | Job completed | `duration` (seconds) |
| `3` | Job failed | `error` (contains `errorCode` and `message`) |
| `99` | Job exception | `error` (contains `errorCode` and `message`) |

### Callback Examples

#### Status 0 - Job Created

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 0,
  "detail": {
    "jobId": "08c87282-97e8-4993-9da4-359f3e84fdfd",
    "jobName": "my-job-fbe70e59"
  }
}
```

#### Status 1 - Pod Started

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 1,
  "detail": {
    "podId": "08c87282-97e8-4993-9da4-359f3e84fdfd",
    "podName": "my-job-fbe70e59-ww7g2"
  }
}
```

#### Status 2 - Job Completed

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 2,
  "detail": {
    "duration": 120
  }
}
```

#### Status 3/99 - Job Failed or Exception

```json
{
  "id": "31a89a91-43d8-4786-a195-9b25cec28a44",
  "status": 3,
  "detail": {
    "error": {
      "errorCode": "JOB_EXECUTION_FAILED",
      "message": "Job execution failed with exit code 1"
    }
  }
}
```

## 📁 Project Structure

```
k8s-queue-job-worker/
├── cmd/                    # Application entry point
│   └── main.go            # Main program
├── configs/               # Configuration management
│   └── configs.go        # Environment variable configuration parsing
├── internal/             # Internal packages
│   ├── app/              # Application layer logic
│   │   ├── callback/     # Webhook callback handling
│   │   ├── http/         # HTTP server
│   │   ├── job/          # Job processing logic
│   │   ├── utils/        # Utility functions
│   │   └── worker/       # Worker main logic
│   └── pkg/              # Shared components
│       ├── cache/        # Cache interface and implementation
│       ├── k8s/          # Kubernetes client
│       ├── logger/       # Logging utilities
│       ├── observability/# Observability (metrics, tracing)
│       └── queue/        # Queue interface and implementation
├── build/                 # Build related
│   └── Dockerfile        # Docker build file
├── example/              # Example files
│   ├── callback.yaml    # Callback service example
│   ├── deployment.yaml  # Deployment configuration example
│   └── message.json     # Message format example
├── go.mod                # Go module definition
└── README.md            # This document
```

## 🚢 Deployment Guide

### Kubernetes Deployment

1. **Create ServiceAccount and RBAC**

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: job-worker-sa
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: job-worker-role
rules:
  - apiGroups: ["batch"]
    resources: ["jobs"]
    verbs: ["get", "list", "watch", "create"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "list", "watch", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: job-worker-binding
  namespace: default
subjects:
  - kind: ServiceAccount
    name: job-worker-sa
    namespace: default
roleRef:
  kind: ClusterRole
  name: job-worker-role
  apiGroup: rbac.authorization.k8s.io
```

2. **Deploy Worker**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-worker
spec:
  replicas: 2  # Supports multiple replicas, elected via Leader Election
  selector:
    matchLabels:
      app: job-worker
  template:
    metadata:
      labels:
        app: job-worker
    spec:
      serviceAccountName: job-worker-sa
      containers:
        - name: job-worker
          image: job-worker:latest
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            - name: CACHE_REDIS_ENDPOINT
              value: "redis-service:6379"
            - name: CACHE_REDIS_DB
              value: "0"
            # ... other environment variables
          ports:
            - name: http
              containerPort: 8080
          livenessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /healthz
              port: http
            initialDelaySeconds: 10
            periodSeconds: 5
```

3. **Create Service (Optional)**

```yaml
apiVersion: v1
kind: Service
metadata:
  name: job-worker
spec:
  selector:
    app: job-worker
  ports:
    - port: 8080
      targetPort: http
      name: http
```

### High Availability Deployment

- Deploy multiple replicas (recommended 2-3)
- Use Leader Election to ensure only one Worker processes the queue
- Configure appropriate resource limits and requests
- Set up health checks and readiness probes

## 📊 Monitoring & Observability

### Prometheus Metrics

The Worker exposes the following Prometheus metrics (default at `/metrics` endpoint):

- `queue_length`: Current queue length
- `messages_processed_total`: Total number of processed messages
- `messages_failed_total`: Total number of failed messages

### Health Check

- **Endpoint**: `GET /healthz`
- **Response**: HTTP 200 indicates healthy

### Logging

- Uses structured logging for all important events
- Supports trace ID to correlate related logs
- Log levels: INFO, WARN, ERROR

## 🔧 Development

### Local Development

```bash
# Install dependencies
go mod download

# Run tests
go test ./...

# Build
go build -o bin/job-worker ./cmd/main.go
```

### Build Docker Image

```bash
docker build -t job-worker:latest -f build/Dockerfile .
```

## 📝 License

See [LICENSE](LICENSE) file for details.

## 🤝 Contributing

Issues and Pull Requests are welcome!
