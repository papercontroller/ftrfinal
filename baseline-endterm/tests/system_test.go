package tests

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"endterm-university-system/pkg/metrics"
	"endterm-university-system/pkg/middleware"
	"endterm-university-system/pkg/resilience"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker"
)

func TestMain(m *testing.M) {
	os.Setenv("JWT_SECRET", "unit-test-secret-of-at-least-32-characters")
	os.Exit(m.Run())
}

func TestJWTGenerationAndValidation(t *testing.T) {
	token, err := middleware.GenerateToken("1", "Student", "Synthetic Student One", 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to generate JWT token: %v", err)
	}

	claims, err := middleware.ValidateToken(token)
	if err != nil {
		t.Fatalf("failed to validate valid JWT token: %v", err)
	}

	if claims.UserID != "1" || claims.Role != "Student" {
		t.Errorf("unexpected claims: %+v", claims)
	}

	// Test invalid token
	_, err = middleware.ValidateToken(token + "tampered")
	if err == nil {
		t.Error("expected error for tampered token, got nil")
	}
}

func TestRBACMiddleware(t *testing.T) {
	// Create student token and admin token
	studentToken, _ := middleware.GenerateToken("1", "Student", "Alice", 1*time.Hour)
	adminToken, _ := middleware.GenerateToken("99", "Admin", "Admin User", 1*time.Hour)

	adminOnlyHandler := middleware.RequireAuth("Admin")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("admin_secret_accessed"))
	}))

	// 1. Missing header -> 401
	req1 := httptest.NewRequest(http.MethodGet, "/admin", nil)
	rr1 := httptest.NewRecorder()
	adminOnlyHandler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 for missing token, got %d", rr1.Code)
	}

	// 2. Student token accessing Admin endpoint -> 403 Forbidden
	req2 := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req2.Header.Set("Authorization", "Bearer "+studentToken)
	rr2 := httptest.NewRecorder()
	adminOnlyHandler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusForbidden {
		t.Errorf("expected 403 for unauthorized role, got %d", rr2.Code)
	}

	// 3. Admin token accessing Admin endpoint -> 200 OK
	req3 := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req3.Header.Set("Authorization", "Bearer "+adminToken)
	rr3 := httptest.NewRecorder()
	adminOnlyHandler.ServeHTTP(rr3, req3)
	if rr3.Code != http.StatusOK {
		t.Errorf("expected 200 for admin role, got %d", rr3.Code)
	}
}

func TestTokenBucketRateLimiting(t *testing.T) {
	bucket := middleware.NewTokenBucket(3.0, 1.0) // capacity 3, refill 1/s

	// First 3 requests must pass
	for i := 0; i < 3; i++ {
		if !bucket.Allow() {
			t.Fatalf("request %d should have been allowed", i+1)
		}
	}

	// 4th request must be rejected (exhausted)
	if bucket.Allow() {
		t.Fatal("4th request should have been rejected by rate limiter")
	}

	// Wait 1.1s for refill
	time.Sleep(1100 * time.Millisecond)
	if !bucket.Allow() {
		t.Fatal("request after refill interval should have been allowed")
	}
}

func TestCircuitBreakerStateTransitions(t *testing.T) {
	cb := resilience.NewCircuitBreaker("test-service", "test_breaker")

	if cb.State() != gobreaker.StateClosed {
		t.Fatalf("expected initial state Closed, got %v", cb.State())
	}

	// Inject 3 consecutive failures to trip circuit breaker (threshold >= 3 and fail ratio >= 0.5)
	for i := 0; i < 3; i++ {
		_, _ = cb.Execute(func() (interface{}, error) {
			return nil, errors.New("simulated backend timeout")
		})
	}

	// State should now be Open
	if cb.State() != gobreaker.StateOpen {
		t.Fatalf("expected state Open after 3 failures, got %v", cb.State())
	}

	// Subsequent calls must fail fast with gobreaker.ErrOpenState without executing function
	executed := false
	_, err := cb.Execute(func() (interface{}, error) {
		executed = true
		return "ok", nil
	})

	if executed {
		t.Error("circuit breaker executed request while in Open state!")
	}
	if !strings.Contains(err.Error(), "circuit breaker is open") {
		t.Errorf("expected circuit breaker open error, got %v", err)
	}
}

func TestRejectsUnsignedAlgNoneToken(t *testing.T) {
	enc := base64.RawURLEncoding.EncodeToString
	forged := enc([]byte(`{"alg":"none","typ":"JWT"}`)) + "." +
		enc([]byte(`{"user_id":"1","role":"Admin","exp":4102444800}`)) + "."
	if _, err := middleware.ValidateToken(forged); err == nil {
		t.Fatal("alg=none token must be rejected")
	}
}

func TestRequireOwner(t *testing.T) {
	cases := []struct {
		name, role, user string
		target           int
		allowed          bool
	}{
		{"own record", "Student", "1", 1, true},
		{"other student", "Student", "1", 2, false},
		{"registrar", "Registrar", "900", 2, true},
		{"admin", "Admin", "999", 2, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/api/transcripts/2", nil)
			r.Header.Set("X-User-Role", c.role)
			r.Header.Set("X-User-ID", c.user)
			rr := httptest.NewRecorder()
			if got := middleware.RequireOwner(rr, r, "test", c.target); got != c.allowed {
				t.Fatalf("allowed=%v, want %v", got, c.allowed)
			}
			if !c.allowed && rr.Code != http.StatusForbidden {
				t.Fatalf("expected 403, got %d", rr.Code)
			}
		})
	}
}

func TestSpoofedForwardedForDoesNotBypassRateLimit(t *testing.T) {
	limited := middleware.RateLimitMiddleware(middleware.NewIPRateLimiter(2, 0.001), "test")(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))

	last := 0
	for i := 0; i < 3; i++ {
		r := httptest.NewRequest(http.MethodGet, "/api/courses", nil)
		r.RemoteAddr = "10.0.0.1:1234"
		r.Header.Set("X-Forwarded-For", "1.2.3."+string(rune('0'+i)))
		rr := httptest.NewRecorder()
		limited.ServeHTTP(rr, r)
		last = rr.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("expected 429 on the third request, got %d", last)
	}
}

// Regression test: a frozen database (accepts TCP, never answers) once hung callers
// forever, because lib/pq ignores context cancellation while blocked on the socket.
func TestFrozenDatabaseCannotHangCaller(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer c.Close() // hold the connection open and never reply
		}
	}()

	addr := ln.Addr().(*net.TCPAddr)
	start := time.Now()
	db, err := resilience.NewDB(fmt.Sprintf("postgres://u:p@127.0.0.1:%d/x?sslmode=disable", addr.Port))
	if err != nil {
		t.Fatal(err)
	}
	if resilience.DBUp(context.Background(), db) {
		t.Fatal("frozen database reported as up")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Fatalf("caller blocked for %v", took)
	}
}

// Regression test: the open state used to stick without traffic and caused false alerts.
func TestBreakerMetricLeavesOpenWithoutTraffic(t *testing.T) {
	cb := resilience.NewCircuitBreaker("idle-test", "idle_breaker")
	for i := 0; i < 3; i++ {
		cb.Execute(func() (interface{}, error) { return nil, errors.New("down") })
	}
	gauge := metrics.CircuitBreakerState.WithLabelValues("idle-test", "idle_breaker")
	if testutil.ToFloat64(gauge) != 2 {
		t.Fatal("breaker should be open")
	}
	time.Sleep(6500 * time.Millisecond) // breaker timeout is 5s, the refresh runs every second
	if got := testutil.ToFloat64(gauge); got == 2 {
		t.Fatalf("metric still reports open: %v", got)
	}
}
