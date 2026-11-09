package handlers

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"university-system/internal/resilience"

	"github.com/sony/gobreaker"
)

type SystemHandler struct {
	DB          *sql.DB
	ServerID    string
	Breaker     *gobreaker.CircuitBreaker
	RetryClient *resilience.RetryClient
}

type PaymentRequest struct {
	IdempotencyKey string  `json:"idempotency_key"`
	Amount         float64 `json:"amount"`
	StudentID      int     `json:"student_id"`
}

func NewSystemHandler(db *sql.DB, serverID string) *SystemHandler {
	return &SystemHandler{
		DB:          db,
		ServerID:    serverID,
		Breaker:     resilience.NewCircuitBreaker("DBBreaker"),
		RetryClient: resilience.NewRetryClient(),
	}
}

func (h *SystemHandler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Second)
	defer cancel()

	w.Header().Set("Content-Type", "application/json")

	if h.DB == nil || h.DB.PingContext(ctx) != nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "degraded",
			"server_id": h.ServerID,
			"database":  "disconnected",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":    "healthy",
		"server_id": h.ServerID,
	})
}

func (h *SystemHandler) GetTranscript(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	result, err := h.Breaker.Execute(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		var transcript string
		err := h.DB.QueryRowContext(ctx, "SELECT data FROM transcripts WHERE student_id = 1").Scan(&transcript)
		if err != nil {
			return nil, err
		}
		return transcript, nil
	})

	if err != nil {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":    "degraded_mode",
			"server_id": h.ServerID,
			"data":      "Cached transcript preview (Database temporarily unavailable)",
		})
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(result.(string)))
}

func (h *SystemHandler) ProcessPayment(w http.ResponseWriter, r *http.Request) {
	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON payload", http.StatusBadRequest)
		return
	}

	if req.IdempotencyKey == "" || req.StudentID <= 0 || req.Amount <= 0 {
		http.Error(w, "missing required fields", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()

	if h.DB == nil || h.DB.PingContext(ctx) != nil {
		http.Error(w, "database unavailable", http.StatusServiceUnavailable)
		return
	}

	var existingResult string
	err := h.DB.QueryRowContext(ctx, "SELECT result FROM idempotency_keys WHERE key = $1", req.IdempotencyKey).Scan(&existingResult)
	if err == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(existingResult))
		return
	}

	tx, err := h.DB.BeginTx(ctx, nil)
	if err != nil {
		http.Error(w, "database transaction error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	resultJSON := fmt.Sprintf(`{"status":"SUCCESS","amount":%.2f,"student_id":%d}`, req.Amount, req.StudentID)

	_, err = tx.ExecContext(ctx, "INSERT INTO payments(student_id, amount) VALUES($1, $2)", req.StudentID, req.Amount)
	if err != nil {
		http.Error(w, "failed to record payment", http.StatusInternalServerError)
		return
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO idempotency_keys(key, result) VALUES($1, $2)", req.IdempotencyKey, resultJSON)
	if err != nil {
		http.Error(w, "failed to record idempotency key", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(resultJSON))
}

func (h *SystemHandler) ExternalCall(w http.ResponseWriter, r *http.Request) {
	targetURL := r.URL.Query().Get("url")
	if targetURL == "" {
		http.Error(w, "url parameter is required", http.StatusBadRequest)
		return
	}

	data, err := h.RetryClient.GetWithRetry(r.Context(), targetURL, 3, 200*time.Millisecond)
	if err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}
