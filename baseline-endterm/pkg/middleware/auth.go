package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"endterm-university-system/pkg/logger"
	"endterm-university-system/pkg/metrics"

	"github.com/golang-jwt/jwt/v5"
)

type userContextKey string

const UserClaimsKey userContextKey = "user_claims"

type CustomClaims struct {
	UserID string `json:"user_id"`
	Role   string `json:"role"` // "Student", "Registrar", "Admin"
	Name   string `json:"name"`
	jwt.RegisteredClaims
}

// The gateway refuses to start without JWT_SECRET, so there is no fallback here.
func getJWTSecret() []byte {
	return []byte(os.Getenv("JWT_SECRET"))
}

func GenerateToken(userID, role, name string, duration time.Duration) (string, error) {
	claims := CustomClaims{
		UserID: userID,
		Role:   role,
		Name:   name,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(duration)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			Issuer:    "AITU-University-Information-System",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString(getJWTSecret())
}

func ValidateToken(tokenStr string) (*CustomClaims, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &CustomClaims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return getJWTSecret(), nil
	})

	if err != nil {
		return nil, err
	}

	if claims, ok := token.Claims.(*CustomClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, errors.New("invalid token")
}

// RequireOwner stops a Student from touching another student's records. It trusts
// X-User-*, which only the gateway can set: backends are not published to the host.
func RequireOwner(w http.ResponseWriter, r *http.Request, service string, studentID int) bool {
	if r.Header.Get("X-User-Role") != "Student" || r.Header.Get("X-User-ID") == strconv.Itoa(studentID) {
		return true
	}
	metrics.AuditSecurityEvents.WithLabelValues(service, "forbidden_owner", "CRITICAL").Inc()
	logger.GetLogger().Audit(r.Context(), "Privilege violation: access to another student's record", map[string]interface{}{
		"user_id":           r.Header.Get("X-User-ID"),
		"target_student_id": studentID,
		"path":              r.URL.Path,
	})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"error":       "Forbidden",
		"message":     "Students may only access their own records",
		"status_code": 403,
	})
	return false
}

func RequireAuth(allowedRoles ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logr := logger.GetLogger()
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				metrics.AuditSecurityEvents.WithLabelValues("auth", "missing_token", "WARNING").Inc()
				logr.Audit(r.Context(), "Unauthorized access attempt: missing Bearer token", map[string]interface{}{
					"path":        r.URL.Path,
					"remote_addr": r.RemoteAddr,
				})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "Unauthorized",
					"message":     "Missing or malformed Authorization header",
					"status_code": 401,
				})
				return
			}

			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			claims, err := ValidateToken(tokenStr)
			if err != nil {
				metrics.AuditSecurityEvents.WithLabelValues("auth", "invalid_token", "CRITICAL").Inc()
				logr.Audit(r.Context(), "Security violation: invalid/expired JWT token", map[string]interface{}{
					"path":  r.URL.Path,
					"error": err.Error(),
				})
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnauthorized)
				json.NewEncoder(w).Encode(map[string]interface{}{
					"error":       "Unauthorized",
					"message":     "Invalid or expired token",
					"status_code": 401,
				})
				return
			}

			// Check role authorization if specified
			if len(allowedRoles) > 0 {
				authorized := false
				for _, role := range allowedRoles {
					if strings.EqualFold(role, claims.Role) {
						authorized = true
						break
					}
				}

				if !authorized {
					metrics.AuditSecurityEvents.WithLabelValues("auth", "forbidden_role", "CRITICAL").Inc()
					logr.Audit(r.Context(), "Privilege violation: insufficient user role", map[string]interface{}{
						"user_id":        claims.UserID,
						"user_role":      claims.Role,
						"required_roles": allowedRoles,
						"path":           r.URL.Path,
					})
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusForbidden)
					json.NewEncoder(w).Encode(map[string]interface{}{
						"error":       "Forbidden",
						"message":     "You do not possess the required privilege level for this operation",
						"status_code": 403,
					})
					return
				}
			}

			ctx := context.WithValue(r.Context(), UserClaimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
