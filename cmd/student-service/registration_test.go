package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"final-university-system/pkg/resilience"
)

func sagaWith(urls ...string) *registrationSaga {
	return &registrationSaga{
		payments: urls,
		client:   http.DefaultClient,
		breaker:  resilience.NewCircuitBreaker("student-service", "test_breaker"),
	}
}

func TestChargeMovesToNextReplicaOn5xx(t *testing.T) {
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer broken.Close()
	var key, role string
	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]interface{}
		json.NewDecoder(r.Body).Decode(&body)
		key, _ = body["idempotency_key"].(string)
		role = r.Header.Get("X-User-Role")
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"payment_id":7}`))
	}))
	defer good.Close()

	id, declined, err := sagaWith(broken.URL, good.URL).charge(context.Background(), &registration{Key: "k", StudentID: 1, Amount: 5})
	if err != nil || declined != "" || id != 7 {
		t.Fatalf("got id=%d declined=%q err=%v", id, declined, err)
	}
	if key != "reg-k" || role != "Service" {
		t.Fatalf("charge must reuse a key derived from the saga and act as a service, got key=%q role=%q", key, role)
	}
}

func TestDeclineIsFinalAndOutageIsNot(t *testing.T) {
	decline := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
	}))
	defer decline.Close()
	calls := 0
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer failing.Close()

	_, declined, err := sagaWith(decline.URL).charge(context.Background(), &registration{Key: "k"})
	if err != nil || declined == "" {
		t.Fatalf("a 4xx must be a decline, not an unknown outcome: declined=%q err=%v", declined, err)
	}
	_, declined, err = sagaWith(failing.URL, failing.URL).charge(context.Background(), &registration{Key: "k"})
	if err == nil || declined != "" {
		t.Fatalf("a 5xx must leave the outcome unknown: declined=%q err=%v", declined, err)
	}
	if calls != 2 {
		t.Fatalf("expected one attempt per replica, got %d", calls)
	}
}

func TestVoidFailureKeepsTheSagaOpen(t *testing.T) {
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer failing.Close()
	if err := sagaWith(failing.URL).void(context.Background(), "k"); err == nil {
		t.Fatal("a refused void must not count as voided, or the seat would be released while a charge stands")
	}
}

func TestFinalStatus(t *testing.T) {
	cases := map[string]bool{
		statusStarted: false, statusReserved: false, statusCancelling: false,
		statusConfirmed: true, statusRejected: true, statusCompensated: true,
	}
	for status, want := range cases {
		if finalStatus(status) != want {
			t.Errorf("finalStatus(%s) = %v", status, !want)
		}
	}
}

func TestRegistrationStatusCodes(t *testing.T) {
	cases := map[string]int{
		statusConfirmed: http.StatusCreated, statusRejected: http.StatusConflict, statusCompensated: http.StatusConflict,
		statusStarted: http.StatusAccepted, statusReserved: http.StatusAccepted, statusCancelling: http.StatusAccepted,
	}
	for status, want := range cases {
		rec := httptest.NewRecorder()
		writeRegistration(rec, &registration{Status: status})
		if rec.Code != want {
			t.Errorf("%s answered %d, want %d", status, rec.Code, want)
		}
	}
}

func TestRegisterRejectsBadInputBeforeTouchingTheDatabase(t *testing.T) {
	svc := &StudentService{} // nil db: reaching it would panic
	long := strings.Repeat("k", 201)
	cases := []struct {
		body string
		want int
	}{
		{`{"amount":`, http.StatusBadRequest},
		{`{"student_id":1,"course_id":"CSE2501","amount":1}`, http.StatusBadRequest},
		{`{"idempotency_key":"` + long + `","student_id":1,"course_id":"CSE2501","amount":1}`, http.StatusBadRequest},
		{`{"idempotency_key":"k","course_id":"CSE2501","amount":1}`, http.StatusBadRequest},
		{`{"idempotency_key":"k","student_id":1,"course_id":"x' OR '1'='1","amount":1}`, http.StatusUnprocessableEntity},
		{`{"idempotency_key":"k","student_id":1,"course_id":"CSE2501","amount":-1}`, http.StatusUnprocessableEntity},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		svc.handleRegister(rec, httptest.NewRequest(http.MethodPost, "/api/registrations", strings.NewReader(c.body)))
		if rec.Code != c.want {
			t.Errorf("%.40s answered %d, want %d", c.body, rec.Code, c.want)
		}
	}
}
