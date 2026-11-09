package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"
	"endterm-university-system/pkg/middleware"
	"endterm-university-system/pkg/resilience"

	_ "github.com/lib/pq"
)

type StudentService struct {
	db       *sql.DB
	serverID string
}

type Student struct {
	ID        int       `json:"id"`
	Name      string    `json:"name"`
	Email     string    `json:"email"`
	Major     string    `json:"major"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type Course struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Credits     int    `json:"credits"`
	MaxCapacity int    `json:"max_capacity"`
	Enrolled    int    `json:"enrolled"`
}

type CourseRegistrationRequest struct {
	CourseID string `json:"course_id"`
}

var safeIDRegex = regexp.MustCompile(`^[A-Za-z0-9_-]{3,20}$`)

func main() {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "student-service-1"
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}

	logr := logger.InitLogger("student-service", serverID)

	dbConn := os.Getenv("DB_CONN")
	if dbConn == "" {
		log.Fatal("DB_CONN must be set")
	}

	db, err := resilience.OpenDB(dbConn)
	if err != nil {
		log.Fatalf("invalid database configuration: %v", err)
	}

	svc := &StudentService{
		db:       db,
		serverID: serverID,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", svc.handleHealth)
	mux.HandleFunc("/readyz", svc.handleReady)
	mux.Handle("/metrics", metrics.MetricsHandler())

	mux.HandleFunc("/api/students", svc.handleStudents)
	mux.HandleFunc("/api/students/", svc.handleStudentByID)
	mux.HandleFunc("/api/courses", svc.handleCourses)

	handler := middleware.CorrelationAndTelemetryMiddleware("student-service")(requestDeadline(mux))

	logr.Info(context.Background(), fmt.Sprintf("Student Service listening on :%s", port), nil)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("Student Service crashed: %v", err)
	}
}

// requestDeadline bounds every query, so a stalled database cannot pin a handler.
func requestDeadline(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func writeDBUnavailable(w http.ResponseWriter) {
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]string{"error": "Service Unavailable", "message": "Database temporarily unavailable"})
}

func (s *StudentService) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "healthy",
		"service":  "student-service",
		"instance": s.serverID,
	})
}

func (s *StudentService) handleReady(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if resilience.DBUp(r.Context(), s.db) {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"readiness": "ready"})
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]string{"readiness": "not_ready"})
}

func (s *StudentService) handleStudents(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if !resilience.DBUp(r.Context(), s.db) {
		writeDBUnavailable(w)
		return
	}

	rows, err := s.db.QueryContext(r.Context(), "SELECT id, name, email, major, status, created_at FROM students LIMIT 20")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var students []Student
	for rows.Next() {
		var st Student
		if err := rows.Scan(&st.ID, &st.Name, &st.Email, &st.Major, &st.Status, &st.CreatedAt); err == nil {
			students = append(students, st)
		}
	}
	json.NewEncoder(w).Encode(students)
}

func (s *StudentService) handleStudentByID(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logr := logger.GetLogger()

	// Parse /api/students/{id} or /api/students/{id}/courses
	path := strings.TrimPrefix(r.URL.Path, "/api/students/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "missing student id", http.StatusBadRequest)
		return
	}

	studentID, err := strconv.Atoi(parts[0])
	if err != nil {
		http.Error(w, "invalid student id format", http.StatusBadRequest)
		return
	}

	if !middleware.RequireOwner(w, r, "student-service", studentID) {
		return
	}

	if len(parts) >= 2 && parts[1] == "courses" && r.Method == http.MethodPost {
		s.handleCourseRegistration(w, r, studentID)
		return
	}

	// Fetch single student
	if !resilience.DBUp(r.Context(), s.db) {
		writeDBUnavailable(w)
		return
	}

	var st Student
	err = s.db.QueryRowContext(r.Context(), "SELECT id, name, email, major, status, created_at FROM students WHERE id = $1", studentID).
		Scan(&st.ID, &st.Name, &st.Email, &st.Major, &st.Status, &st.CreatedAt)
	if err != nil {
		logr.Warn(r.Context(), "Student not found", map[string]interface{}{"student_id": studentID})
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "student not found"})
		return
	}

	json.NewEncoder(w).Encode(st)
}

func (s *StudentService) handleCourseRegistration(w http.ResponseWriter, r *http.Request, studentID int) {
	logr := logger.GetLogger()
	var req CourseRegistrationRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Input Validation: Check against SQL Injection & Malformed input
	if !safeIDRegex.MatchString(req.CourseID) {
		metrics.AuditSecurityEvents.WithLabelValues("student-service", "malformed_input", "WARNING").Inc()
		logr.Audit(r.Context(), "Input validation failed: suspicious course id pattern", map[string]interface{}{
			"student_id": studentID,
			"course_id":  req.CourseID,
		})
		w.WriteHeader(http.StatusUnprocessableEntity)
		json.NewEncoder(w).Encode(map[string]string{
			"error":   "Unprocessable Entity",
			"message": "Course ID must be 3-20 alphanumeric characters",
		})
		return
	}

	if !resilience.DBUp(r.Context(), s.db) {
		writeDBUnavailable(w)
		return
	}

	tx, err := s.db.BeginTx(r.Context(), nil)
	if err != nil {
		http.Error(w, "internal transaction error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback()

	// Verify course exists
	var enrolled, capacity int
	err = tx.QueryRowContext(r.Context(), "SELECT enrolled, max_capacity FROM courses WHERE id = $1 FOR UPDATE", req.CourseID).
		Scan(&enrolled, &capacity)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "course not found"})
		return
	}

	if enrolled >= capacity {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "course is at full capacity"})
		return
	}

	_, err = tx.ExecContext(r.Context(), "INSERT INTO enrollments (student_id, course_id) VALUES ($1, $2)", studentID, req.CourseID)
	if err != nil {
		w.WriteHeader(http.StatusConflict)
		json.NewEncoder(w).Encode(map[string]string{"error": "student already enrolled or conflict"})
		return
	}

	if _, err = tx.ExecContext(r.Context(), "UPDATE courses SET enrolled = enrolled + 1 WHERE id = $1", req.CourseID); err != nil {
		http.Error(w, "enrollment update failed", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(); err != nil {
		http.Error(w, "commit failure", http.StatusInternalServerError)
		return
	}

	logr.Info(r.Context(), "Course enrolled successfully", map[string]interface{}{
		"student_id": studentID,
		"course_id":  req.CourseID,
	})

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ENROLLED",
		"student_id": studentID,
		"course_id":  req.CourseID,
	})
}

func (s *StudentService) handleCourses(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// The catalog is non-critical and rarely changes, so a static copy is acceptable.
	// Personal records above return 503 instead: a wrong record is worse than none.
	if !resilience.DBUp(r.Context(), s.db) {
		w.Header().Set("X-Degraded", "static-catalog")
		json.NewEncoder(w).Encode([]Course{
			{ID: "CSE2501", Name: "Fault Tolerance & Dependable Computing", Credits: 5},
			{ID: "CSE2502", Name: "Cloud Microservices & Resilience", Credits: 5},
			{ID: "CSE2503", Name: "Software Verification & Security", Credits: 5},
		})
		return
	}

	rows, err := s.db.QueryContext(r.Context(), "SELECT id, name, credits, max_capacity, enrolled FROM courses")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var courses []Course
	for rows.Next() {
		var c Course
		if err := rows.Scan(&c.ID, &c.Name, &c.Credits, &c.MaxCapacity, &c.Enrolled); err == nil {
			courses = append(courses, c)
		}
	}
	json.NewEncoder(w).Encode(courses)
}
