package logger

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type LogLevel string

const (
	LevelInfo  LogLevel = "INFO"
	LevelWarn  LogLevel = "WARN"
	LevelError LogLevel = "ERROR"
	LevelAudit LogLevel = "AUDIT"
)

type contextKey string

const CorrelationIDKey contextKey = "correlation_id"

type LogEntry struct {
	Timestamp     string                 `json:"timestamp"`
	Level         LogLevel               `json:"level"`
	Service       string                 `json:"service"`
	ServerID      string                 `json:"server_id,omitempty"`
	CorrelationID string                 `json:"correlation_id,omitempty"`
	Message       string                 `json:"message"`
	Fields        map[string]interface{} `json:"fields,omitempty"`
}

type StructuredLogger struct {
	service  string
	serverID string
	mu       sync.Mutex
}

var defaultLogger *StructuredLogger
var once sync.Once

func InitLogger(service, serverID string) *StructuredLogger {
	once.Do(func() {
		defaultLogger = &StructuredLogger{
			service:  service,
			serverID: serverID,
		}
	})
	return defaultLogger
}

func GetLogger() *StructuredLogger {
	if defaultLogger == nil {
		return InitLogger("unknown-service", "node-1")
	}
	return defaultLogger
}

func (l *StructuredLogger) log(ctx context.Context, level LogLevel, msg string, fields map[string]interface{}) {
	corrID := ""
	if ctx != nil {
		if val, ok := ctx.Value(CorrelationIDKey).(string); ok {
			corrID = val
		}
	}

	entry := LogEntry{
		Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
		Level:         level,
		Service:       l.service,
		ServerID:      l.serverID,
		CorrelationID: corrID,
		Message:       msg,
		Fields:        fields,
	}

	data, err := json.Marshal(entry)
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger json error: %v\n", err)
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Println(string(data))
}

func (l *StructuredLogger) Info(ctx context.Context, msg string, fields map[string]interface{}) {
	l.log(ctx, LevelInfo, msg, fields)
}

func (l *StructuredLogger) Warn(ctx context.Context, msg string, fields map[string]interface{}) {
	l.log(ctx, LevelWarn, msg, fields)
}

func (l *StructuredLogger) Error(ctx context.Context, msg string, fields map[string]interface{}) {
	l.log(ctx, LevelError, msg, fields)
}

func (l *StructuredLogger) Audit(ctx context.Context, msg string, fields map[string]interface{}) {
	l.log(ctx, LevelAudit, msg, fields)
}
