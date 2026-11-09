package main

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

type failingThenOK struct {
	failHost string
	calls    atomic.Int32
}

func (f *failingThenOK) RoundTrip(r *http.Request) (*http.Response, error) {
	f.calls.Add(1)
	if r.URL.Host == f.failHost {
		return nil, errors.New("connection refused")
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
}

func TestGetFailsOverToHealthyReplicaAndMarksDeadOneDown(t *testing.T) {
	p := newPool("academic", "http://dead:1,http://alive:1")
	tr := &failingThenOK{failHost: "dead:1"}
	transport := &failoverTransport{pool: p, base: tr}

	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodGet, "http://gw/api/transcripts/1", nil)
		if _, err := transport.RoundTrip(req); err != nil {
			t.Fatalf("request %d not failed over: %v", i, err)
		}
	}
	if p.replicas[0].healthy.Load() {
		t.Fatal("dead replica must be removed from rotation")
	}
	if got := tr.calls.Load(); got != 5 {
		t.Fatalf("expected one failed call and four successes, got %d calls", got)
	}
}

func TestPostIsNeverRetried(t *testing.T) {
	p := newPool("payment", "http://dead:1,http://also-dead:1")
	tr := &failingThenOK{failHost: "dead:1"}
	transport := &failoverTransport{pool: p, base: tr}
	p.counter.Store(1) // next() starts at index 0, the failing replica

	// A retried POST could charge twice, so the failure must reach the client.
	req := httptest.NewRequest(http.MethodPost, "http://gw/api/payments", strings.NewReader("{}"))
	req.GetBody = nil
	p.replicas[1].healthy.Store(false)
	if _, err := transport.RoundTrip(req); err == nil {
		t.Fatal("expected the POST failure to be returned")
	}
	if got := tr.calls.Load(); got != 1 {
		t.Fatalf("POST was sent %d times", got)
	}
}

func TestAllReplicasDownStillTriesOne(t *testing.T) {
	p := newPool("academic", "http://a:1,http://b:1")
	for _, r := range p.replicas {
		r.healthy.Store(false)
	}
	if p.next() == nil {
		t.Fatal("next must return a replica even when all are marked down")
	}
}

func TestClientCannotChooseItsRole(t *testing.T) {
	t.Setenv("JWT_SECRET", "unit-test-secret-of-at-least-32-characters")
	gw := &GatewayServer{demoPass: "pw"}
	body := `{"username":"student1","password":"pw","role":"Admin"}`
	r := httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(body))
	rr := httptest.NewRecorder()
	gw.handleLogin(rr, r)
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"role":"Student"`) {
		t.Fatalf("got %d %s", rr.Code, rr.Body.String())
	}
}

func TestLoginRejectsWrongPasswordAndUnknownUser(t *testing.T) {
	gw := &GatewayServer{demoPass: "pw"}
	for _, body := range []string{`{"username":"student1","password":"no"}`, `{"username":"ghost","password":"pw"}`} {
		rr := httptest.NewRecorder()
		gw.handleLogin(rr, httptest.NewRequest(http.MethodPost, "/auth/login", strings.NewReader(body)))
		if rr.Code != http.StatusUnauthorized {
			t.Fatalf("%s: got %d", body, rr.Code)
		}
	}
}

type dialFailsOnce struct{ calls atomic.Int32 }

func (d *dialFailsOnce) RoundTrip(r *http.Request) (*http.Response, error) {
	if d.calls.Add(1) == 1 {
		return nil, &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	}
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: r}, nil
}

func TestPostFailsOverWhenConnectionWasNeverEstablished(t *testing.T) {
	p := newPool("payment", "http://a:1,http://b:1")
	tr := &dialFailsOnce{}
	req := httptest.NewRequest(http.MethodPost, "http://gw/api/payments", strings.NewReader("{}"))
	if _, err := (&failoverTransport{pool: p, base: tr}).RoundTrip(req); err != nil {
		t.Fatal(err)
	}
	if got := tr.calls.Load(); got != 2 {
		t.Fatalf("expected a second attempt after the dial error, got %d calls", got)
	}
}

func TestOversizedBodyGetsClear413(t *testing.T) {
	called := false
	h := limitBody(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { called = true }))
	r := httptest.NewRequest(http.MethodPost, "/api/payments", strings.NewReader(strings.Repeat("a", maxBodyBytes+1)))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	if rr.Code != http.StatusRequestEntityTooLarge || called {
		t.Fatalf("got %d, handler called=%v", rr.Code, called)
	}
}
