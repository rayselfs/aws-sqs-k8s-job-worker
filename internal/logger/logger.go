package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

// logger is the global slog.Logger instance.
var logger *slog.Logger

const (
	TraceIDKey = "traceid" // Key for trace ID in logs
	SpanIDKey  = "spanid"  // Key for span ID in logs
)

type ctxKey string

const (
	ctxTraceID ctxKey = "traceid" // Context key for trace ID
	ctxSpanID  ctxKey = "spanid"  // Context key for span ID
)

// Setup initializes the global logger with JSON handler.
func Setup() error {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return nil
}

// WithTraceID returns a new context with the given trace ID.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, ctxTraceID, traceID)
}

// WithSpanID returns a new context with the given span ID.
func WithSpanID(ctx context.Context, spanID string) context.Context {
	return context.WithValue(ctx, ctxSpanID, spanID)
}

// TraceIDFromContext extracts the trace ID from context or OpenTelemetry span.
func TraceIDFromContext(ctx context.Context) string {
	if v := ctx.Value(ctxTraceID); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	if span := trace.SpanFromContext(ctx); span != nil {
		if sc := span.SpanContext(); sc.IsValid() {
			return sc.TraceID().String()
		}
	}
	return ""
}

// SpanIDFromContext extracts the span ID from context or OpenTelemetry span.
func SpanIDFromContext(ctx context.Context) string {
	if v := ctx.Value(ctxSpanID); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	if span := trace.SpanFromContext(ctx); span != nil {
		if sc := span.SpanContext(); sc.IsValid() {
			return sc.SpanID().String()
		}
	}
	return ""
}

// InfoCtx logs an info message with trace and span IDs from context.
func InfoCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Info(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

// WarnCtx logs a warning message with trace and span IDs from context.
func WarnCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Warn(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

// ErrorCtx logs an error message with trace and span IDs from context.
func ErrorCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Error(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

// Info logs an info message (without context).
func Info(msg string, attrs ...any) {
	logger.Info(fmt.Sprintf(msg, attrs...))
}

// Warn logs a warning message (without context).
func Warn(msg string, attrs ...any) {
	logger.Warn(fmt.Sprintf(msg, attrs...))
}

// Error logs an error message (without context).
func Error(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...))
}

// Fatal logs an error message and exits the process.
func Fatal(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...))
	os.Exit(1)
}
