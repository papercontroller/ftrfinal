package main

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
	"endterm-university-system/pkg/middleware"
)

const (
	maxBodyBytes  = 1 << 20
	maxAttempts   = 3
	probeInterval = time.Second
	probeTimeout  = 500 * time.Millisecond
	headerTimeout = 3 * time.Second // must exceed the 1.5s DB timeout of the services
)

type replica struct {
	url     *url.URL
	healthy atomic.Bool
}

// pool is a set of interchangeable instances of one backend service.
type pool struct {
	service  string
	replicas []*replica
	counter  atomic.Uint64
}

func newPool(service, rawURLs string) *pool {
	p := &pool{service: service}
	for _, raw := range strings.Split(rawURLs, ",") {
		if raw = strings.TrimSpace(raw); raw != "" {
			r := &replica{url: parseURL(raw)}
			r.healthy.Store(true)
			p.replicas = append(p.replicas, r)
		}
	}
	if len(p.replicas) == 0 {
		log.Fatalf("no backend URLs configured for %s", service)
	}
	return p
}

// next returns the next healthy replica round-robin. If all are marked down it
// still returns one, because a stale health flag must not turn into an outage.
func (p *pool) next() *replica {
	n := uint64(len(p.replicas))
	start := p.counter.Add(1)
	for i := uint64(0); i < n; i++ {
		if r := p.replicas[(start+i)%n]; r.healthy.Load() {
			return r
		}
	}
	return p.replicas[start%n]
}

func (p *pool) anyHealthy() bool {
	for _, r := range p.replicas {
		if r.healthy.Load() {
			return true
		}
	}
	return false
}

func (p *pool) setHealth(r *replica, healthy bool, reason string) {
	value := 0.0
	if healthy {
		value = 1.0
	}
	metrics.UpstreamReplicaHealthy.WithLabelValues(p.service, r.url.Host).Set(value)
	if r.healthy.Swap(healthy) != healthy {
		logger.GetLogger().Warn(context.Background(), "Upstream replica health changed", map[string]interface{}{
			"service": p.service, "replica": r.url.Host, "healthy": healthy, "reason": reason,
		})
	}
}

// watch actively probes every replica so a recovered instance rejoins the pool.
func (p *pool) watch(client *http.Client) {
	for _, r := range p.replicas {
		p.setHealth(r, true, "startup")
	}
	for range time.Tick(probeInterval) {
		for _, r := range p.replicas {
			resp, err := client.Get(r.url.String() + "/healthz")
			ok := err == nil && resp.StatusCode == http.StatusOK
			if err == nil {
				resp.Body.Close()
			}
			p.setHealth(r, ok, "active_probe")
		}
	}
}

// failoverTransport sends each request to a replica of the pool. GETs are retried
// on any failure. A POST is retried only if the connection was never established:
// after that it may already have taken effect, so repeating it is left to the
// client, which owns the idempotency key.
type failoverTransport struct {
	pool *pool
	base http.RoundTripper
}

func (t *failoverTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	attempts := maxAttempts
	backoff := 50 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		r := t.pool.next()
		out := req.Clone(req.Context()) // the body is still unread: nothing was sent yet or it is a GET
		out.URL.Scheme, out.URL.Host, out.Host = r.url.Scheme, r.url.Host, r.url.Host

		resp, err := t.base.RoundTrip(out)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		t.pool.setHealth(r, false, "request_failed")
		if attempt < attempts && (req.Method == http.MethodGet || neverConnected(err)) {
			logger.GetLogger().Warn(req.Context(), "Upstream call failed, retrying on next replica", map[string]interface{}{
				"attempt": attempt, "max_attempts": attempts, "replica": r.url.Host, "error": err.Error(),
			})
			select {
			case <-req.Context().Done():
				return nil, req.Context().Err()
			case <-time.After(backoff + time.Duration(rand.Int63n(int64(backoff)))):
			}
			backoff *= 2
			continue
		}
		break
	}
	return nil, lastErr
}

func neverConnected(err error) bool {
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

type GatewayServer struct {
	pools       []*pool
	rateLimiter *middleware.IPRateLimiter
	demoPass    string
}

// limitBody answers 413 up front. MaxBytesHandler alone would only cut the stream
// halfway, and the client would see a broken connection instead of a clear refusal.
func limitBody(next http.Handler) http.Handler {
	limited := http.MaxBytesHandler(next, maxBodyBytes)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > maxBodyBytes {
			w.Header().Set("Connection", "close")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusRequestEntityTooLarge)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       "Payload Too Large",
				"message":     "Request body exceeds 1 MB",
				"status_code": http.StatusRequestEntityTooLarge,
			})
			return
		}
		limited.ServeHTTP(w, r)
	})
}

func parseURL(raw string) *url.URL {
	u, err := url.Parse(raw)
	if err != nil {
		log.Fatalf("invalid service URL %s: %v", raw, err)
	}
	return u
}

func envFloat(name string, fallback float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(name), 64); err == nil && v > 0 {
		return v
	}
	return fallback
}

func main() {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "api-gateway-1"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	logr := logger.InitLogger("api-gateway", serverID)

	studentURLStr := os.Getenv("STUDENT_SERVICE_URL")
	if studentURLStr == "" {
		studentURLStr = "http://localhost:8081"
	}

	paymentURLStr := os.Getenv("PAYMENT_SERVICE_URL")
	if paymentURLStr == "" {
		paymentURLStr = "http://localhost:8082"
	}

	academicURLsStr := os.Getenv("ACADEMIC_SERVICE_URLS")
	if academicURLsStr == "" {
		academicURLsStr = "http://localhost:8083"
	}

	jwtSecret, demoPass := os.Getenv("JWT_SECRET"), os.Getenv("DEMO_PASSWORD")
	if len(jwtSecret) < 32 || demoPass == "" {
		log.Fatal("JWT_SECRET (min 32 chars) and DEMO_PASSWORD must be set; refusing to start with default credentials")
	}

	gw := &GatewayServer{
		rateLimiter: middleware.NewIPRateLimiter(envFloat("RATE_LIMIT_BURST", 50), envFloat("RATE_LIMIT_RPS", 20)),
		demoPass:    demoPass,
	}

	studentPool := newPool("student-service", studentURLStr)
	paymentPool := newPool("payment-service", paymentURLStr)
	academicPool := newPool("academic-service", academicURLsStr)
	gw.pools = []*pool{studentPool, paymentPool, academicPool}

	probeClient := &http.Client{Timeout: probeTimeout}
	for _, p := range gw.pools {
		go p.watch(probeClient)
	}

	auth := middleware.RequireAuth

	mux := http.NewServeMux()

	// Public and health endpoints
	mux.HandleFunc("/healthz", gw.handleHealth)
	mux.HandleFunc("/readyz", gw.handleReady)
	mux.Handle("/metrics", metrics.MetricsHandler())
	mux.HandleFunc("/auth/login", gw.handleLogin)

	// Protected Microservices Routes with RBAC
	studentProxy := gw.createProxy(studentPool)
	paymentProxy := gw.createProxy(paymentPool)
	academicProxy := gw.createProxy(academicPool)

	// The full student list exposes other people's emails, so it is staff-only.
	mux.Handle("/api/students", auth("Registrar", "Admin")(studentProxy))
	mux.Handle("/api/students/", auth("Student", "Registrar", "Admin")(studentProxy))
	mux.Handle("/api/courses", auth("Student", "Registrar", "Admin")(studentProxy))
	mux.Handle("/api/courses/", auth("Student", "Registrar", "Admin")(studentProxy))

	// Payments (Allowed: Student, Admin)
	mux.Handle("/api/payments", auth("Student", "Admin")(paymentProxy))
	mux.Handle("/api/payments/", auth("Student", "Admin")(paymentProxy))

	// Academic & Transcripts (Allowed: Student, Registrar, Admin)
	mux.Handle("/api/transcripts", auth("Student", "Registrar", "Admin")(academicProxy))
	mux.Handle("/api/transcripts/", auth("Student", "Registrar", "Admin")(academicProxy))

	// Attach Middlewares: Rate Limiter -> Correlation & Telemetry
	handler := middleware.CorrelationAndTelemetryMiddleware("api-gateway")(
		middleware.RateLimitMiddleware(gw.rateLimiter, "api-gateway")(limitBody(mux)),
	)

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logr.Info(context.Background(), fmt.Sprintf("API Gateway online on :%s", port), map[string]interface{}{
		"student_backend":   studentURLStr,
		"payment_backend":   paymentURLStr,
		"academic_backends": academicURLsStr,
	})

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("API Gateway stopped: %v", err)
	}
}

func (gw *GatewayServer) createProxy(p *pool) http.Handler {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.MaxIdleConnsPerHost = 100
	base.ResponseHeaderTimeout = headerTimeout
	base.DialContext = (&net.Dialer{Timeout: time.Second}).DialContext

	return &httputil.ReverseProxy{
		Transport: &failoverTransport{pool: p, base: base},
		Rewrite: func(pr *httputil.ProxyRequest) {
			// Claims always overwrite these, so a client cannot forge its identity.
			pr.Out.Header.Del("X-User-ID")
			pr.Out.Header.Del("X-User-Role")
			if corrID, ok := pr.In.Context().Value(logger.CorrelationIDKey).(string); ok {
				pr.Out.Header.Set("X-Correlation-ID", corrID)
			}
			if claims, ok := pr.In.Context().Value(middleware.UserClaimsKey).(*middleware.CustomClaims); ok {
				pr.Out.Header.Set("X-User-ID", claims.UserID)
				pr.Out.Header.Set("X-User-Role", claims.Role)
			}
		},
		ErrorHandler: func(rw http.ResponseWriter, req *http.Request, err error) {
			logger.GetLogger().Error(req.Context(), "Reverse proxy upstream error", map[string]interface{}{
				"service":  p.service,
				"error":    err.Error(),
				"endpoint": req.URL.Path,
			})
			status, title := http.StatusBadGateway, "Bad Gateway"
			var netErr net.Error
			if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &netErr) && netErr.Timeout()) {
				status, title = http.StatusGatewayTimeout, "Gateway Timeout"
			}
			rw.Header().Set("Content-Type", "application/json")
			rw.WriteHeader(status)
			json.NewEncoder(rw).Encode(map[string]interface{}{
				"error":       title,
				"message":     "Upstream microservice instance unreachable or timeout",
				"service":     p.service,
				"status_code": status,
			})
		},
	}
}

func (gw *GatewayServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "healthy",
		"service": "api-gateway",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (gw *GatewayServer) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	for _, p := range gw.pools {
		if !p.anyHealthy() {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"readiness": "not_ready", "service": p.service})
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"readiness": "ready"})
}

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type demoUser struct{ ID, Role, Name string }

// shortcut: fixed synthetic accounts sharing one password, replace with a real identity provider before any non-lab use.
// The role comes from this table, never from the client.
var demoUsers = map[string]demoUser{
	"student1":   {"1", "Student", "Synthetic Student One"},
	"student2":   {"2", "Student", "Synthetic Student Two"},
	"registrar1": {"900", "Registrar", "Synthetic Registrar"},
	"admin1":     {"999", "Admin", "Synthetic Admin"},
}

func (gw *GatewayServer) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	var req LoginRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	user, known := demoUsers[req.Username]
	if !known || subtle.ConstantTimeCompare([]byte(req.Password), []byte(gw.demoPass)) != 1 {
		metrics.AuditSecurityEvents.WithLabelValues("api-gateway", "failed_login", "WARNING").Inc()
		logger.GetLogger().Audit(r.Context(), "Failed login attempt", map[string]interface{}{
			"username": req.Username, "remote_addr": r.RemoteAddr,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Unauthorized",
			"message": "Invalid credentials",
		})
		return
	}

	token, err := middleware.GenerateToken(user.ID, user.Role, user.Name, 2*time.Hour)
	if err != nil {
		http.Error(w, "failed to sign token", http.StatusInternalServerError)
		return
	}

	metrics.AuditSecurityEvents.WithLabelValues("api-gateway", "successful_login", "INFO").Inc()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "authenticated",
		"token":      token,
		"user_id":    user.ID,
		"role":       user.Role,
		"expires_in": 7200,
	})
}
