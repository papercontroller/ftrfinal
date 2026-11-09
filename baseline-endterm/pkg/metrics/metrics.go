package metrics

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	HttpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed by service, status, method and endpoint.",
		},
		[]string{"service", "endpoint", "method", "status"},
	)

	HttpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request latency distributions across endpoints.",
			Buckets: []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.3, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"service", "endpoint"},
	)

	CircuitBreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "circuit_breaker_state",
			Help: "Current state of circuit breaker: 0=Closed, 1=Half-Open, 2=Open.",
		},
		[]string{"service", "breaker_name"},
	)

	RateLimitDropsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rate_limit_drops_total",
			Help: "Total number of requests dropped by token bucket rate limiter.",
		},
		[]string{"service", "client_ip"},
	)

	UpstreamReplicaHealthy = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "upstream_replica_healthy",
			Help: "Gateway view of a backend replica: 1=healthy, 0=removed from rotation.",
		},
		[]string{"service", "replica"},
	)

	DegradedResponsesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "degraded_responses_total",
			Help: "Responses served from the last-known-good cache instead of the database.",
		},
		[]string{"service"},
	)

	AuditSecurityEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "audit_security_events_total",
			Help: "Total count of security audit events recorded.",
		},
		[]string{"service", "event_type", "severity"},
	)
)

func init() {
	prometheus.MustRegister(
		HttpRequestsTotal,
		HttpRequestDuration,
		CircuitBreakerState,
		RateLimitDropsTotal,
		AuditSecurityEvents,
		UpstreamReplicaHealthy,
		DegradedResponsesTotal,
	)
}

func MetricsHandler() http.Handler {
	return promhttp.Handler()
}

// normalizeEndpoint keeps the label set bounded, otherwise each random URL an
// attacker requests would create a new time series in memory.
func normalizeEndpoint(path string) string {
	switch path {
	case "/healthz", "/readyz", "/metrics", "/auth/login":
		return path
	}
	parts := strings.SplitN(path, "/", 4)
	if len(parts) >= 3 && parts[1] == "api" {
		switch parts[2] {
		case "students", "courses", "payments", "transcripts":
			return "/api/" + parts[2]
		}
	}
	return "other"
}

func RecordMetrics(service, path, method string, statusCode int, duration time.Duration) {
	statusStr := strconv.Itoa(statusCode)
	endpoint := normalizeEndpoint(path)
	HttpRequestsTotal.WithLabelValues(service, endpoint, method, statusStr).Inc()
	HttpRequestDuration.WithLabelValues(service, endpoint).Observe(duration.Seconds())
}
