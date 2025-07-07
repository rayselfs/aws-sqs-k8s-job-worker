package logger

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"go.opentelemetry.io/otel/trace"
)

var logger *slog.Logger

const (
	TraceIDKey = "traceid"
	SpanIDKey  = "spanid"
)

type ctxKey string

const (
	ctxTraceID ctxKey = "traceid"
	ctxSpanID  ctxKey = "spanid"
)

func Setup() error {
	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
	return nil
}

// Context helpers
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, ctxTraceID, traceID)
}

func WithSpanID(ctx context.Context, spanID string) context.Context {
	return context.WithValue(ctx, ctxSpanID, spanID)
}

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

// Context-aware logging
func InfoCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Info(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

// Context-aware logging
func WarnCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Warn(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

func ErrorCtx(ctx context.Context, msg string, attrs ...any) {
	msg = fmt.Sprintf(msg, attrs...)
	logger.Error(msg,
		slog.String(TraceIDKey, TraceIDFromContext(ctx)),
		slog.String(SpanIDKey, SpanIDFromContext(ctx)),
	)
}

// 原本的 logger 方法保留
func Info(msg string, attrs ...any) {
	logger.Info(fmt.Sprintf(msg, attrs...))
}

func Error(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...))
}

func Fatal(msg string, attrs ...any) {
	logger.Error(fmt.Sprintf(msg, attrs...))
	os.Exit(1)
}
