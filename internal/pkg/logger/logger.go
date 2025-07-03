package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func Setup() error {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.LevelKey = "severity"
	cfg.EncoderConfig.CallerKey = "caller"
	cfg.EncoderConfig.StacktraceKey = "stacktrace"
	var err error
	logger, err = cfg.Build()
	if err != nil {
		return err
	}
	return nil
}

func Info(msg string, fields ...zap.Field) {
	logger.Info(msg, fields...)
}

func Error(msg string, fields ...zap.Field) {
	logger.Error(msg, fields...)
}

func Fatal(msg string, fields ...zap.Field) {
	logger.Fatal(msg, fields...)
}
