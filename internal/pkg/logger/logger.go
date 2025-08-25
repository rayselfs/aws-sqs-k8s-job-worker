package logger

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"

	"github.com/go-logr/stdr"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/klog/v2"
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

type slogWriter struct {
	logger *slog.Logger
}

func (w slogWriter) Write(p []byte) (n int, err error) {
	w.logger.Info(string(p))
	return len(p), nil
}

// Setup initializes the global logger with JSON handler.
func Setup() error {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))

	log.SetOutput(slogWriter{logger: logger})
	klog.SetLogger(stdr.New(log.Default()))
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

// getCaller returns the file:line of the caller for tracing.
func getCaller() string {
	if _, file, line, ok := runtime.Caller(2); ok {
		return fmt.Sprintf("%s:%d", file, line)
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

// WarnCtx logs a warning message with trace and span IDs from context, and code trace.
func WarnCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Warn(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
		slog.String("caller", getCaller()),
	)
}

// ErrorCtx logs an error message with trace and span IDs from context, and code trace.
func ErrorCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Error(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
		slog.String("caller", getCaller()),
	)
}

// Info logs an info message (without context).
func Info(msg string, attrs ...any) {
	logger.Info(fmt.Sprintf(msg, attrs...))
}

// Warn logs a warning message (without context), with code trace.
func Warn(msg string, attrs ...any) {
	logger.Warn(fmt.Sprintf(msg, attrs...), slog.String("caller", getCaller()))
}

// Error logs an error message (without context), with code trace.
func Error(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...), slog.String("caller", getCaller()))
}

// Fatal logs an error message and exits the process, with code trace.
func Fatal(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...), slog.String("caller", getCaller()))
	os.Exit(1)
}
