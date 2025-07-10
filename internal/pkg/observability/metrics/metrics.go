package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	MessagesProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aws_sqs_messages_processed_total",
			Help: "Total messages processed from SQS",
		})

	MessagesFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aws_sqs_messages_failed_total",
			Help: "Total messages failed from SQS",
		})

	MessageProcessingTime = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aws_sqs_message_processing_seconds",
			Help:    "Histogram of message processing duration",
			Buckets: prometheus.DefBuckets,
		})

	QueueLength = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aws_sqs_queue_length",
			Help: "Current number of messages in the queue",
		},
	)

	JobSuccess = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aws_sqs_job_success_total",
			Help: "Total number of successful job executions",
		},
	)

	JobFailure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "aws_sqs_job_failure_total",
			Help: "Total number of failed job executions",
		},
	)
)

func Setup() {
	prometheus.MustRegister(MessagesProcessed)
	prometheus.MustRegister(MessagesFailed)
	prometheus.MustRegister(MessageProcessingTime)
	prometheus.MustRegister(QueueLength)
	prometheus.MustRegister(JobSuccess)
	prometheus.MustRegister(JobFailure)
}
