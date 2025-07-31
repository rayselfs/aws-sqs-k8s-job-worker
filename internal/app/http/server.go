package http

import (
	"aws-sqs-k8s-job-worker/internal/app/http/handler"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func StartHTTPServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.Healthz)
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		logger.Info("Starting HTTP server on :8080")
		if err := srv.ListenAndServe(); err != nil {
			if err == http.ErrServerClosed {
				logger.Info("HTTP server closed")
			} else {
				logger.Error("HTTP server failed, error: %s", err.Error())
			}
		}
	}()

	go func() {
		<-ctx.Done()
		logger.Info("Shutting down HTTP server...")
		ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctxShutdown); err != nil {
			logger.Error("HTTP server shutdown failed, error: %s", err.Error())
		} else {
			logger.Info("HTTP server shut down gracefully")
		}
	}()
}
