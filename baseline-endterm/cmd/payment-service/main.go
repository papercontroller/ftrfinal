package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
	"endterm-university-system/pkg/middleware"
	"endterm-university-system/pkg/resilience"

	"github.com/lib/pq"
)

var currencyRegex = regexp.MustCompile(`^[A-Z]{3}$`)

type PaymentService struct {
	db       *sql.DB
	serverID string
}

type PaymentRequest struct {
	IdempotencyKey string  `json:"idempotency_key"`
	StudentID      int     `json:"student_id"`
	Amount         float64 `json:"amount"`
	Currency       string  `json:"currency"`
}

type PaymentResponse struct {
	PaymentID        int       `json:"payment_id"`
	StudentID        int       `json:"student_id"`
	Amount           float64   `json:"amount"`
	Currency         string    `json:"currency"`
	Status           string    `json:"status"`
	ProcessedAt      time.Time `json:"processed_at"`
	IdempotentReplay bool      `json:"idempotent_replay"`
}

func main() {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "payment-service-1"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}

	logr := logger.InitLogger("payment-service", serverID)

	dbConn := os.Getenv("DB_CONN")
	if dbConn == "" {
		log.Fatal("DB_CONN must be set")
	}

	db, err := resilience.OpenDB(dbConn)
	if err != nil {
		log.Fatalf("invalid database configuration: %v", err)
	}

	svc := &PaymentService{
		db:       db,
		serverID: serverID,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", svc.handleHealth)
	mux.HandleFunc("/readyz", svc.handleReady)
	mux.Handle("/metrics", metrics.MetricsHandler())

	mux.HandleFunc("/api/payments", svc.handlePayments)
	mux.HandleFunc("/api/payments/", svc.handlePayments)

	handler := middleware.CorrelationAndTelemetryMiddleware("payment-service")(mux)

	logr.Info(context.Background(), fmt.Sprintf("Payment Service listening on :%s", port), nil)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Payment Service crashed: %v", err)
	}
}

func (s *PaymentService) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "healthy",
		"service":  "payment-service",
		"instance": s.serverID,
	})
}

func (s *PaymentService) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if resilience.DBUp(r.Context(), s.db) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"readiness": "ready"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]string{"readiness": "not_ready"})
}

func (s *PaymentService) handlePayments(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logr := logger.GetLogger()

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid json payload"})
		return
	}

	// 1. Strict Input Validation
	if req.IdempotencyKey == "" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "idempotency_key is required"})
		return
	}
	if req.StudentID <= 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid student_id"})
		return
	}
	if req.Amount <= 0.0 || req.Amount > 10000000.0 {
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(map[string]string{"error": "amount must be greater than 0 and realistic"})
		return
	}
	if len(req.IdempotencyKey) > 255 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "idempotency_key is too long"})
		return
	}
	if req.Currency == "" {
		req.Currency = "KZT"
	}
	if !currencyRegex.MatchString(req.Currency) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(map[string]string{"error": "currency must be a 3-letter code"})
		return
	}
	if !middleware.RequireOwner(w, r, "payment-service", req.StudentID) {
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if !resilience.DBUp(ctx, s.db) {
		logr.Error(ctx, "Payment rejected: database unavailable", nil)
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Service Unavailable",
			"message": "Payment database is temporarily unreachable. Please retry safely.",
		})
		return
	}

	// 2. Idempotency Check
	if s.replayIfProcessed(ctx, w, req) {
		return
	}

	// 3. Atomic Database Transaction
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		http.Error(w, "transaction initiation failed", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	var paymentID int
	err = tx.QueryRowContext(ctx,
		"INSERT INTO payments (student_id, amount, currency, status) VALUES ($1, $2, $3, 'COMPLETED') RETURNING id",
		req.StudentID, req.Amount, req.Currency,
	).Scan(&paymentID)
	if err != nil {
		if pgCode(err) == "23503" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			json.NewEncoder(w).Encode(map[string]string{"error": "student does not exist"})
			return
		}
		logr.Error(ctx, "Failed to insert payment record", map[string]interface{}{"error": err.Error()})
		http.Error(w, "payment record error", http.StatusInternalServerError)
		return
	}

	resp := PaymentResponse{
		PaymentID:        paymentID,
		StudentID:        req.StudentID,
		Amount:           req.Amount,
		Currency:         req.Currency,
		Status:           "COMPLETED",
		ProcessedAt:      time.Now().UTC(),
		IdempotentReplay: false,
	}

	respJSON, _ := json.Marshal(resp)

	_, err = tx.ExecContext(ctx,
		"INSERT INTO idempotency_keys (key, result, status_code) VALUES ($1, $2, 200)",
		req.IdempotencyKey, string(respJSON),
	)
	if pgCode(err) == "23505" {
		// A concurrent request with this key won the race. Our insert rolls back,
		// so the payment still exists exactly once, and we replay the winner's result.
		tx.Rollback()
		if s.replayIfProcessed(ctx, w, req) {
			return
		}
	}
	if err != nil {
		logr.Error(ctx, "Failed to store idempotency key", map[string]interface{}{"error": err.Error()})
		http.Error(w, "idempotency persistence error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		logr.Error(ctx, "Transaction commit failed", map[string]interface{}{"error": err.Error()})
		http.Error(w, "commit error", http.StatusInternalServerError)
		return
	}

	// 4. Financial Audit Logging
	logr.Audit(ctx, "Financial payment processed", map[string]interface{}{
		"payment_id":      paymentID,
		"student_id":      req.StudentID,
		"amount":          req.Amount,
		"currency":        req.Currency,
		"idempotency_key": req.IdempotencyKey,
	})
	metrics.AuditSecurityEvents.WithLabelValues("payment-service", "payment_settled", "INFO").Inc()

	w.WriteHeader(http.StatusCreated)
	w.Write(respJSON)
}

// replayIfProcessed answers from a stored result and reports whether it did. The
// same key with different details is a client bug, so it is refused, not replayed.
func (s *PaymentService) replayIfProcessed(ctx context.Context, w http.ResponseWriter, req PaymentRequest) bool {
	var stored string
	err := s.db.QueryRowContext(ctx, "SELECT result FROM idempotency_keys WHERE key = $1", req.IdempotencyKey).Scan(&stored)
	if err != nil {
		return false
	}

	var resp PaymentResponse
	if err := json.Unmarshal([]byte(stored), &resp); err != nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(stored))
		return true
	}
	if resp.StudentID != req.StudentID || resp.Amount != req.Amount {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "idempotency_key was already used for a different payment"})
		return true
	}

	logger.GetLogger().Info(ctx, "Idempotent payment replayed", map[string]interface{}{
		"idempotency_key": req.IdempotencyKey,
		"student_id":      req.StudentID,
	})
	resp.IdempotentReplay = true
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
	return true
}

func pgCode(err error) string {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code)
	}
	return ""
}
