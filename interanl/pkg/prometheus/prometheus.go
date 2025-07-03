package prometheus

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
)

func Setup() {
	// 註冊 metrics
	prometheus.MustRegister(MessagesProcessed)
	prometheus.MustRegister(MessagesFailed)
	prometheus.MustRegister(MessageProcessingTime)
}
