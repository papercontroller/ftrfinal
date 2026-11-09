package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"time"

	"university-system/internal/handlers"
	"university-system/internal/middleware"

	_ "github.com/lib/pq"
)

func main() {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		serverID = "standalone-node"
	}

	dbConn := os.Getenv("DB_CONN")
	if dbConn == "" {
		dbConn = "postgres://user:pass@db:5432/uis?sslmode=disable"
	}

	var db *sql.DB
	var err error

	for i := 0; i < 5; i++ {
		db, err = sql.Open("postgres", dbConn)
		if err == nil && db.Ping() == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}

	handler := handlers.NewSystemHandler(db, serverID)
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", handler.HealthCheck)
	mux.HandleFunc("/transcript", handler.GetTranscript)
	mux.HandleFunc("/pay", handler.ProcessPayment)
	mux.HandleFunc("/external", handler.ExternalCall)

	timeoutHandler := middleware.TimeoutMiddleware(5 * time.Second)(mux)

	log.Printf("[%s] University System listening on :8080...", serverID)
	if err := http.ListenAndServe(":8080", timeoutHandler); err != nil {
		log.Fatalf("[%s] Server crash: %v", serverID, err)
	}
}