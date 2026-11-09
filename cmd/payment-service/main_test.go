package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVoidIsRefusedForEveryClientRole(t *testing.T) {
	svc := &PaymentService{} // nil db: reaching it would panic
	for _, role := range []string{"", "Student", "Registrar", "Admin"} {
		req := httptest.NewRequest(http.MethodPost, "/api/payments/void", strings.NewReader(`{"idempotency_key":"reg-k"}`))
		req.Header.Set("X-User-Role", role)
		rec := httptest.NewRecorder()
		svc.handleVoid(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("role %q answered %d", role, rec.Code)
		}
	}
}

func TestVoidRequiresAKey(t *testing.T) {
	svc := &PaymentService{}
	req := httptest.NewRequest(http.MethodPost, "/api/payments/void", strings.NewReader(`{}`))
	req.Header.Set("X-User-Role", "Service")
	rec := httptest.NewRecorder()
	svc.handleVoid(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("answered %d", rec.Code)
	}
}
