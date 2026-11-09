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
	"strconv"
	"strings"
	"sync"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
	"endterm-university-system/pkg/middleware"
	"endterm-university-system/pkg/resilience"

	_ "github.com/lib/pq"
)

type AcademicService struct {
	db             *sql.DB
	serverID       string
	circuitBreaker *resilience.CircuitBreakerWrapper
	cache          *transcriptCache
}

// transcriptCache keeps the last record read from the database, so a degraded
// response is always a real, older record and never an invented one.
// shortcut: per-replica and in memory, so it is empty after a restart; use a shared cache if cold replicas become a problem.
type transcriptCache struct {
	mu      sync.RWMutex
	records map[int]cachedTranscript
}

type cachedTranscript struct {
	record   TranscriptRecord
	storedAt time.Time
}

func (c *transcriptCache) put(r TranscriptRecord) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.records[r.StudentID] = cachedTranscript{record: r, storedAt: time.Now().UTC()}
}

func (c *transcriptCache) degraded(studentID int, serverID string) (TranscriptRecord, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	entry, ok := c.records[studentID]
	if !ok {
		return TranscriptRecord{}, false
	}
	r := entry.record
	r.Status = "DEGRADED_CACHED_MODE"
	r.ServedBy = serverID
	r.DegradedNotice = "Database unavailable; read-only copy from " + entry.storedAt.Format(time.RFC3339)
	return r, true
}

type TranscriptRecord struct {
	StudentID      int             `json:"student_id"`
	GPA            float64         `json:"gpa"`
	Credits        int             `json:"credits"`
	Details        json.RawMessage `json:"details"`
	Status         string          `json:"status"` // OFFICIAL or DEGRADED_CACHED_MODE
	ServedBy       string          `json:"served_by"`
	DegradedNotice string          `json:"degraded_notice,omitempty"`
}

func main() {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "academic-service-1"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8083"
	}

	logr := logger.InitLogger("academic-service", serverID)

	dbConn := os.Getenv("DB_CONN")
	if dbConn == "" {
		log.Fatal("DB_CONN must be set")
	}

	db, err := resilience.OpenDB(dbConn)
	if err != nil {
		log.Fatalf("invalid database configuration: %v", err)
	}

	cb := resilience.NewCircuitBreaker("academic-service", "academic_db_breaker")

	svc := &AcademicService{
		db:             db,
		serverID:       serverID,
		circuitBreaker: cb,
		cache:          &transcriptCache{records: make(map[int]cachedTranscript)},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", svc.handleHealth)
	mux.HandleFunc("/readyz", svc.handleReady)
	mux.Handle("/metrics", metrics.MetricsHandler())

	mux.HandleFunc("/api/transcripts", svc.handleTranscripts)
	mux.HandleFunc("/api/transcripts/", svc.handleTranscripts)

	handler := middleware.CorrelationAndTelemetryMiddleware("academic-service")(mux)

	logr.Info(context.Background(), fmt.Sprintf("Academic Service listening on :%s (Server: %s)", port, serverID), nil)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Academic Service crashed: %v", err)
	}
}

func (s *AcademicService) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "healthy",
		"service":  "academic-service",
		"instance": s.serverID,
	})
}

// handleReady stays OK while the database is down: the replica can still serve
// cached transcripts, so it must not be removed from rotation.
func (s *AcademicService) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"readiness": "ready"})
}

func (s *AcademicService) handleTranscripts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logr := logger.GetLogger()

	studentID, err := transcriptStudentID(r)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid student id"})
		return
	}
	if !middleware.RequireOwner(w, r, "academic-service", studentID) {
		return
	}

	// A missing student is a normal answer, not a dependency failure: it returns
	// (nil, nil) so that requests for random ids cannot trip the breaker.
	result, err := s.circuitBreaker.Execute(func() (interface{}, error) {
		ctx, cancel := context.WithTimeout(r.Context(), 1500*time.Millisecond)
		defer cancel()

		var gpa float64
		var credits int
		var detailsStr string
		queryErr := s.db.QueryRowContext(ctx,
			"SELECT gpa, credits, details::text FROM transcripts WHERE student_id = $1",
			studentID,
		).Scan(&gpa, &credits, &detailsStr)

		if errors.Is(queryErr, sql.ErrNoRows) {
			return nil, nil
		}
		if queryErr != nil {
			return nil, queryErr
		}

		return &TranscriptRecord{
			StudentID: studentID,
			GPA:       gpa,
			Credits:   credits,
			Details:   json.RawMessage(detailsStr),
			Status:    "OFFICIAL",
			ServedBy:  s.serverID,
		}, nil
	})

	if err != nil {
		logr.Warn(r.Context(), "Primary transcript lookup failed; trying last-known-good cache", map[string]interface{}{
			"student_id":      studentID,
			"circuit_breaker": s.circuitBreaker.State().String(),
			"error":           err.Error(),
		})

		fallback, ok := s.cache.degraded(studentID, s.serverID)
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error":   "Service Unavailable",
				"message": "Database unavailable and no cached copy of this transcript exists",
			})
			return
		}
		metrics.DegradedResponsesTotal.WithLabelValues("academic-service").Inc()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(fallback)
		return
	}

	record, _ := result.(*TranscriptRecord)
	if record == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "transcript not found"})
		return
	}
	s.cache.put(*record)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(record)
}

// transcriptStudentID reads /api/transcripts/{id}; without an id it is the caller.
func transcriptStudentID(r *http.Request) (int, error) {
	raw := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/transcripts"), "/")
	if raw == "" {
		raw = r.Header.Get("X-User-ID")
	}
	return strconv.Atoi(raw)
}
