package http

import (
	"aws-sqs-k8s-job-worker/internal/app/http/handler"
	"aws-sqs-k8s-job-worker/internal/pkg/logger"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HttpConfig holds the HTTP server configuration
type HttpConfig struct {
	Port    string
	Timeout time.Duration
}

// StartHTTPServer sets up and runs the HTTP server.
// It's a blocking function that will run until the context is canceled.
// It should NOT be executed in a goroutine since it handles graceful shutdown internally.
func (cfg HttpConfig) StartHTTPServer(ctx context.Context) {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", handler.Healthz)
	mux.Handle("/metrics", promhttp.Handler())

	addr := fmt.Sprintf(":%s", cfg.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  cfg.Timeout,
		WriteTimeout: cfg.Timeout,
		IdleTimeout:  cfg.Timeout,
	}

	// Start the server in a separate goroutine so it doesn't block the shutdown listener.
	go func() {
		logger.Info("Starting HTTP server on %s", addr)
		// ListenAndServe blocks until the server is closed.
		// We handle the http.ErrServerClosed error gracefully.
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed, error: %s", err.Error())
		}
	}()

	// Wait for the context to be canceled (e.g., by a shutdown signal).
	<-ctx.Done()

	// Once the context is canceled, begin a graceful shutdown.
	logger.Info("Shutting down HTTP server...")
	ctxShutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctxShutdown); err != nil {
		logger.Error("HTTP server shutdown failed, error: %s", err.Error())
	} else {
		logger.Info("HTTP server shut down gracefully")
	}
}
