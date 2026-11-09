package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
)

type TokenBucket struct {
	capacity   float64
	tokens     float64
	refillRate float64 // tokens per second
	lastRefill time.Time
	mu         sync.Mutex
}

func NewTokenBucket(capacity, refillRate float64) *TokenBucket {
	return &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.lastRefill = now

	tb.tokens += elapsed * tb.refillRate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}

	if tb.tokens >= 1.0 {
		tb.tokens -= 1.0
		return true
	}
	return false
}

type IPRateLimiter struct {
	buckets    map[string]*TokenBucket
	mu         sync.Mutex
	capacity   float64
	refillRate float64
}

func NewIPRateLimiter(capacity, refillRate float64) *IPRateLimiter {
	limiter := &IPRateLimiter{
		buckets:    make(map[string]*TokenBucket),
		capacity:   capacity,
		refillRate: refillRate,
	}

	// periodic cleanup of stale client buckets every 5 minutes
	go func() {
		for {
			time.Sleep(5 * time.Minute)
			limiter.mu.Lock()
			now := time.Now()
			for ip, b := range limiter.buckets {
				b.mu.Lock()
				if now.Sub(b.lastRefill) > 10*time.Minute {
					delete(limiter.buckets, ip)
				}
				b.mu.Unlock()
			}
			limiter.mu.Unlock()
		}
	}()

	return limiter
}

func (limiter *IPRateLimiter) getBucket(ip string) *TokenBucket {
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	b, exists := limiter.buckets[ip]
	if !exists {
		b = NewTokenBucket(limiter.capacity, limiter.refillRate)
		limiter.buckets[ip] = b
	}
	return b
}

// X-Forwarded-For is client-controlled and is deliberately ignored: trusting it
// would let an attacker get a fresh token bucket per request.
func getClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func RateLimitMiddleware(limiter *IPRateLimiter, serviceName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip rate limit on health checks and metrics
			if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
				next.ServeHTTP(w, r)
				return
			}

			clientIP := getClientIP(r)
			bucket := limiter.getBucket(clientIP)

			if !bucket.Allow() {
				metrics.RateLimitDropsTotal.WithLabelValues(serviceName, clientIP).Inc()
				metrics.AuditSecurityEvents.WithLabelValues(serviceName, "rate_limit_exceeded", "WARNING").Inc()

				logr := logger.GetLogger()
				logr.Warn(r.Context(), "Rate limit exceeded for client IP", map[string]interface{}{
					"client_ip": clientIP,
					"endpoint":  r.URL.Path,
				})

				w.Header().Set("Retry-After", "1")
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "Too Many Requests",
					"message":     "Request quota exceeded. Please slow down.",
					"status_code": 429,
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}
