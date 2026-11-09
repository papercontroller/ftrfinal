package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
)

func generateCorrelationID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return time.Now().Format("20060102150405.000000")
	}
	return hex.EncodeToString(b)
}

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

func CorrelationAndTelemetryMiddleware(serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			corrID := r.Header.Get("X-Correlation-ID")
			if corrID == "" {
				corrID = generateCorrelationID()
			}

			w.Header().Set("X-Correlation-ID", corrID)
			ctx := context.WithValue(r.Context(), logger.CorrelationIDKey, corrID)
			r = r.WithContext(ctx)

			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(rec, r)

			duration := time.Since(start)
			metrics.RecordMetrics(serviceName, r.URL.Path, r.Method, rec.statusCode, duration)

			logr := logger.GetLogger()
			logr.Info(ctx, "HTTP request processed", map[string]interface{}{
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      rec.statusCode,
				"duration_ms": duration.Milliseconds(),
				"remote_addr": r.RemoteAddr,
				"user_agent":  r.UserAgent(),
			})
		})
	}
}
