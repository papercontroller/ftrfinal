package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"final-university-system/pkg/logger"
	"final-university-system/pkg/metrics"
	"final-university-system/pkg/middleware"
	"final-university-system/pkg/resilience"
)

// Paid course registration crosses two services, so it cannot be one transaction.
// It is a saga: registrations.status is the checkpoint and every step is safe to repeat.
//
//	STARTED -> SEAT_RESERVED -> CONFIRMED
//	   |             |
//	   v             v
//	REJECTED     CANCELLING -> COMPENSATED
const (
	statusStarted     = "STARTED"
	statusReserved    = "SEAT_RESERVED"
	statusConfirmed   = "CONFIRMED"
	statusRejected    = "REJECTED"
	statusCancelling  = "CANCELLING"
	statusCompensated = "COMPENSATED"

	sagaLease       = 5 * time.Second // a replica owns a registration this long before others may take over
	sagaMaxAttempts = 6               // about 30 s of retries through recovery, then compensate
	sagaBudget      = 2500 * time.Millisecond
	paymentTimeout  = 1200 * time.Millisecond
	stuckAfter      = "20 seconds"
)

type registration struct {
	Key       string  `json:"idempotency_key"`
	StudentID int     `json:"student_id"`
	CourseID  string  `json:"course_id"`
	Amount    float64 `json:"amount"`
	Status    string  `json:"status"`
	Reason    string  `json:"failure_reason,omitempty"`
	attempts  int
}

type registrationSaga struct {
	db             *sql.DB
	payments       []string
	client         *http.Client
	breaker        *resilience.CircuitBreakerWrapper
	faultInjection bool
}

func newRegistrationSaga(db *sql.DB) *registrationSaga {
	return &registrationSaga{
		db:             db,
		payments:       strings.Split(os.Getenv("PAYMENT_SERVICE_URLS"), ","),
		client:         &http.Client{Timeout: paymentTimeout},
		breaker:        resilience.NewCircuitBreaker("student-service", "payment_breaker"),
		faultInjection: os.Getenv("FAULT_INJECTION") == "on",
	}
}

func finalStatus(s string) bool {
	return s == statusConfirmed || s == statusRejected || s == statusCompensated
}

// crashPoint ends the process between two steps, for experiment E9 only. It needs
// FAULT_INJECTION=on and a key named for this point. Recovery never crashes, because
// otherwise the replica that resumes the work would die at the same step.
func (g *registrationSaga) crashPoint(point, key string, recovering bool) {
	if g.faultInjection && !recovering && strings.HasPrefix(key, "fault-"+point+"-") {
		logger.GetLogger().Warn(context.Background(), "Injected crash", map[string]interface{}{"point": point, "key": key})
		os.Exit(1)
	}
}

func (g *registrationSaga) load(ctx context.Context, key string) (*registration, error) {
	reg := &registration{Key: key}
	var reason sql.NullString
	err := g.db.QueryRowContext(ctx,
		"SELECT student_id, course_id, amount, status, failure_reason, attempts FROM registrations WHERE idempotency_key = $1", key,
	).Scan(&reg.StudentID, &reg.CourseID, &reg.Amount, &reg.Status, &reason, &reg.attempts)
	reg.Reason = reason.String
	return reg, err
}

// advance runs every step that is still open. An error means the outcome is not known
// yet: the status stays where it is and recovery continues from there.
func (g *registrationSaga) advance(ctx context.Context, key string, recovering bool) (*registration, error) {
	g.crashPoint("after_start", key, recovering)
	reg, err := g.load(ctx, key)
	if err != nil {
		return nil, err
	}

	if reg.Status == statusStarted {
		if err := g.reserveSeat(ctx, reg); err != nil {
			return reg, err
		}
		g.crashPoint("after_seat", key, recovering)
	}

	if reg.Status == statusReserved {
		paymentID, declined, err := g.charge(ctx, reg)
		switch {
		case declined != "":
			if err := g.setStatus(ctx, reg, statusReserved, statusCancelling, declined, nil); err != nil {
				return reg, err
			}
		case err != nil && reg.attempts >= sagaMaxAttempts:
			if err := g.setStatus(ctx, reg, statusReserved, statusCancelling, "payment unavailable", nil); err != nil {
				return reg, err
			}
		case err != nil:
			return reg, err
		default:
			g.crashPoint("after_charge", key, recovering)
			if err := g.setStatus(ctx, reg, statusReserved, statusConfirmed, "", &paymentID); err != nil {
				return reg, err
			}
		}
	}

	if reg.Status == statusCancelling {
		if err := g.void(ctx, key); err != nil {
			return reg, err
		}
		g.crashPoint("after_void", key, recovering)
		if err := g.releaseSeat(ctx, reg); err != nil {
			return reg, err
		}
	}

	return reg, nil
}

// setStatus moves only from the expected state, so two replicas racing on one
// registration cannot apply a step twice. The loser takes the state the winner wrote.
func (g *registrationSaga) setStatus(ctx context.Context, reg *registration, from, to, reason string, paymentID *int) error {
	res, err := g.db.ExecContext(ctx, `UPDATE registrations
		SET status = $1, failure_reason = NULLIF($2, ''), payment_id = COALESCE($3, payment_id), updated_at = now()
		WHERE idempotency_key = $4 AND status = $5`, to, reason, paymentID, reg.Key, from)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		latest, err := g.load(ctx, reg.Key)
		if err != nil {
			return err
		}
		*reg = *latest
		return nil
	}
	reg.Status, reg.Reason = to, reason
	return nil
}

// lockStatus takes the row lock that makes one seat change atomic with its status change.
func lockStatus(ctx context.Context, tx *sql.Tx, key string) (string, error) {
	var status string
	err := tx.QueryRowContext(ctx, "SELECT status FROM registrations WHERE idempotency_key = $1 FOR UPDATE", key).Scan(&status)
	return status, err
}

func writeStatus(ctx context.Context, tx *sql.Tx, key, to, reason string) error {
	_, err := tx.ExecContext(ctx, `UPDATE registrations SET status = $1, failure_reason = NULLIF($2, ''), updated_at = now()
		WHERE idempotency_key = $3`, to, reason, key)
	return err
}

func (g *registrationSaga) reserveSeat(ctx context.Context, reg *registration) error {
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	status, err := lockStatus(ctx, tx, reg.Key)
	if err != nil {
		return err
	}
	if status != statusStarted { // another replica did this step first
		reg.Status = status
		return nil
	}

	var enrolled, capacity int
	err = tx.QueryRowContext(ctx, "SELECT enrolled, max_capacity FROM courses WHERE id = $1 FOR UPDATE", reg.CourseID).Scan(&enrolled, &capacity)
	if err != nil {
		return err
	}

	to, reason := statusReserved, ""
	if enrolled >= capacity {
		to, reason = statusRejected, "course is at full capacity"
	} else {
		res, err := tx.ExecContext(ctx, "INSERT INTO enrollments (student_id, course_id) VALUES ($1, $2) ON CONFLICT DO NOTHING", reg.StudentID, reg.CourseID)
		if err != nil {
			return err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			to, reason = statusRejected, "student already enrolled"
		} else if _, err = tx.ExecContext(ctx, "UPDATE courses SET enrolled = enrolled + 1 WHERE id = $1", reg.CourseID); err != nil {
			return err
		}
	}

	if err := writeStatus(ctx, tx, reg.Key, to, reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	reg.Status, reg.Reason = to, reason
	return nil
}

func (g *registrationSaga) releaseSeat(ctx context.Context, reg *registration) error {
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	status, err := lockStatus(ctx, tx, reg.Key)
	if err != nil {
		return err
	}
	if status != statusCancelling {
		reg.Status = status
		return nil
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM enrollments WHERE student_id = $1 AND course_id = $2", reg.StudentID, reg.CourseID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "UPDATE courses SET enrolled = enrolled - 1 WHERE id = $1", reg.CourseID); err != nil {
		return err
	}
	if err := writeStatus(ctx, tx, reg.Key, statusCompensated, reg.Reason); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	reg.Status = statusCompensated
	return nil
}

func paymentKey(key string) string { return "reg-" + key }

type paymentAnswer struct {
	status int
	body   []byte
}

// post tries the payment replicas in turn. The key makes a repeat harmless, so unlike
// the gateway it may retry after any failure. A 4xx is an answer, not a failure.
func (g *registrationSaga) post(ctx context.Context, path string, body interface{}) (paymentAnswer, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return paymentAnswer{}, err
	}
	result, err := g.breaker.Execute(func() (interface{}, error) {
		var lastErr error
		for _, base := range g.payments {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(payload))
			if err != nil {
				return nil, err
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-User-Role", middleware.RoleService)
			if id, ok := ctx.Value(logger.CorrelationIDKey).(string); ok {
				req.Header.Set("X-Correlation-ID", id)
			}
			resp, err := g.client.Do(req)
			if err != nil {
				lastErr = err
				continue
			}
			var buf bytes.Buffer
			buf.ReadFrom(resp.Body)
			resp.Body.Close()
			if resp.StatusCode >= 500 {
				lastErr = fmt.Errorf("payment service answered %d", resp.StatusCode)
				continue
			}
			return paymentAnswer{resp.StatusCode, buf.Bytes()}, nil
		}
		return nil, lastErr
	})
	if err != nil {
		return paymentAnswer{}, err
	}
	return result.(paymentAnswer), nil
}

// charge returns the payment id, or a decline reason if the payment was refused for
// good, or an error if the outcome is unknown and the same key must be tried again.
func (g *registrationSaga) charge(ctx context.Context, reg *registration) (int, string, error) {
	answer, err := g.post(ctx, "/api/payments", map[string]interface{}{
		"idempotency_key": paymentKey(reg.Key),
		"student_id":      reg.StudentID,
		"amount":          reg.Amount,
		"currency":        "KZT",
	})
	if err != nil {
		return 0, "", err
	}
	if answer.status >= 300 {
		return 0, fmt.Sprintf("payment declined with status %d", answer.status), nil
	}
	var paid struct {
		PaymentID int `json:"payment_id"`
	}
	if err := json.Unmarshal(answer.body, &paid); err != nil {
		return 0, "", err
	}
	return paid.PaymentID, "", nil
}

func (g *registrationSaga) void(ctx context.Context, key string) error {
	answer, err := g.post(ctx, "/api/payments/void", map[string]string{"idempotency_key": paymentKey(key)})
	if err != nil {
		return err
	}
	if answer.status >= 300 {
		return fmt.Errorf("void answered %d", answer.status)
	}
	return nil
}

// watch resumes open registrations and publishes the numbers behind the alerts.
// With recovery off (baseline) it only publishes the numbers.
func (g *registrationSaga) watch(recovery bool) {
	ctx := context.Background()
	for range time.Tick(time.Second) {
		up := resilience.DBUp(ctx, g.db)
		value := 0.0
		if up {
			value = 1.0
		}
		metrics.DBReachable.WithLabelValues("student-service").Set(value)
		if !up {
			continue
		}
		g.publishCounts(ctx)
		if !recovery {
			continue
		}
		keys, err := g.claim(ctx)
		if err != nil {
			logger.GetLogger().Warn(ctx, "Saga recovery could not claim work", map[string]interface{}{"error": err.Error()})
			continue
		}
		for _, key := range keys {
			runCtx, cancel := context.WithTimeout(ctx, sagaBudget)
			reg, err := g.advance(runCtx, key, true)
			cancel()
			if err != nil {
				logger.GetLogger().Warn(ctx, "Saga recovery attempt failed", map[string]interface{}{"key": key, "error": err.Error()})
				continue
			}
			if finalStatus(reg.Status) {
				metrics.SagaFinishedTotal.WithLabelValues(reg.Status, "recovery").Inc()
				logger.GetLogger().Info(ctx, "Saga recovered", map[string]interface{}{"key": key, "status": reg.Status})
			}
		}
	}
}

// claim leases expired registrations. SKIP LOCKED keeps two replicas off the same row.
func (g *registrationSaga) claim(ctx context.Context) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	rows, err := g.db.QueryContext(ctx, fmt.Sprintf(`UPDATE registrations
		SET lease_until = now() + interval '%d seconds', attempts = attempts + 1
		WHERE idempotency_key IN (
			SELECT idempotency_key FROM registrations
			WHERE status IN ('STARTED', 'SEAT_RESERVED', 'CANCELLING') AND lease_until < now()
			ORDER BY lease_until LIMIT 5 FOR UPDATE SKIP LOCKED)
		RETURNING idempotency_key`, int(sagaLease.Seconds())))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, rows.Err()
}

func (g *registrationSaga) publishCounts(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	var open, stuck int
	err := g.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE created_at < now() - interval '`+stuckAfter+`')
		FROM registrations WHERE status IN ('STARTED', 'SEAT_RESERVED', 'CANCELLING')`).Scan(&open, &stuck)
	if err == nil {
		metrics.RegistrationsOpen.Set(float64(open))
		metrics.RegistrationsStuck.Set(float64(stuck))
	}
}

func (s *StudentService) handleRegister(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req registration
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid json payload")
		return
	}
	if req.Key == "" || len(req.Key) > 200 {
		writeError(w, http.StatusBadRequest, "idempotency_key is required, at most 200 characters")
		return
	}
	if req.StudentID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid student_id")
		return
	}
	if !safeIDRegex.MatchString(req.CourseID) {
		writeError(w, http.StatusUnprocessableEntity, "course_id must be 3-20 alphanumeric characters")
		return
	}
	if req.Amount <= 0 {
		writeError(w, http.StatusUnprocessableEntity, "amount must be greater than 0")
		return
	}
	if !middleware.RequireOwner(w, r, "student-service", req.StudentID) {
		return
	}
	if !resilience.DBUp(r.Context(), s.db) {
		writeDBUnavailable(w)
		return
	}

	// The saga outlives the client: its deadline is its own, not the request's.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), sagaBudget)
	defer cancel()

	res, err := s.db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO registrations (idempotency_key, student_id, course_id, amount, lease_until)
		VALUES ($1, $2, $3, $4, now() + interval '%d seconds') ON CONFLICT (idempotency_key) DO NOTHING`, int(sagaLease.Seconds())),
		req.Key, req.StudentID, req.CourseID, req.Amount)
	if pgCode(err) == "23503" {
		writeError(w, http.StatusUnprocessableEntity, "student or course does not exist")
		return
	}
	if err != nil {
		logger.GetLogger().Error(ctx, "Registration not stored", map[string]interface{}{"error": err.Error()})
		writeDBUnavailable(w)
		return
	}

	if n, _ := res.RowsAffected(); n == 0 { // the same key again: report, do not repeat
		reg, err := s.saga.load(ctx, req.Key)
		if err != nil {
			writeDBUnavailable(w)
			return
		}
		if reg.StudentID != req.StudentID || reg.CourseID != req.CourseID || reg.Amount != req.Amount {
			writeError(w, http.StatusConflict, "idempotency_key was already used for a different registration")
			return
		}
		writeRegistration(w, reg)
		return
	}

	reg, err := s.saga.advance(ctx, req.Key, false)
	if err != nil {
		logger.GetLogger().Warn(ctx, "Registration left for recovery", map[string]interface{}{"key": req.Key, "error": err.Error()})
	}
	if reg == nil {
		reg = &req
		reg.Status = statusStarted
	}
	if finalStatus(reg.Status) {
		metrics.SagaFinishedTotal.WithLabelValues(reg.Status, "request").Inc()
	}
	writeRegistration(w, reg)
}

func (s *StudentService) handleRegistrationStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	key := strings.TrimPrefix(r.URL.Path, "/api/registrations/")
	reg, err := s.saga.load(r.Context(), key)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "registration not found")
		return
	}
	if err != nil {
		writeDBUnavailable(w)
		return
	}
	if !middleware.RequireOwner(w, r, "student-service", reg.StudentID) {
		return
	}
	writeRegistration(w, reg)
}

// writeRegistration: 202 tells the client the work continues without it.
func writeRegistration(w http.ResponseWriter, reg *registration) {
	switch reg.Status {
	case statusConfirmed:
		w.WriteHeader(http.StatusCreated)
	case statusRejected, statusCompensated:
		w.WriteHeader(http.StatusConflict)
	default:
		w.WriteHeader(http.StatusAccepted)
	}
	json.NewEncoder(w).Encode(reg)
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
