/*
	Logger handles JSON logging of application events

All logs must have 3 core parameters:

	timestamp
	level (INFO, WARN, ERROR, DEBUG)
	message

Dynamic parameters include:

	target_url
	duration_ms
	status_code
	error
*/
package logger

import (
	"fmt"
	"log/slog"
	"os"
)

type Logger struct {
	jsonLogger  *slog.Logger
	logFilename *os.File
}

func NewLogger(logFilename string) (*Logger, error) {
	file, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("error initializing logger: %w", err)
	}
	return &Logger{
		jsonLogger: slog.New(slog.NewJSONHandler(file, nil)),
	}, nil
}

func (l *Logger) LogMessage(message string, statusCode int, latency int64, url string) {
	l.jsonLogger.Info(message,
		slog.Int("status_code", statusCode),
		slog.Int64("latency_ms", latency),
		slog.String("target_url", url))
}

func (l *Logger) LogNonErrorFailure(message string, statusCode int, latency int64, failures int, url string) {
	l.jsonLogger.Error(message,
		slog.Int("status_code", statusCode),
		slog.Int64("latency_ms", latency),
		slog.String("target_url", url),
		slog.Int("failure_count", failures))
}

func (l *Logger) LogErrorFailure(message string, latency int64, failures int, url string, err string) {
	l.jsonLogger.Error(message,
		slog.Int64("latency_ms", latency),
		slog.String("target_url", url),
		slog.Int("failure_count", failures),
		slog.String("error_message", err))
}
