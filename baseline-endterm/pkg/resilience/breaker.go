package resilience

import (
	"time"

	"endterm-university-system/pkg/metrics"

	"github.com/sony/gobreaker"
)

type CircuitBreakerWrapper struct {
	cb          *gobreaker.CircuitBreaker
	serviceName string
	breakerName string
}

func NewCircuitBreaker(serviceName, breakerName string) *CircuitBreakerWrapper {
	wrapper := &CircuitBreakerWrapper{
		serviceName: serviceName,
		breakerName: breakerName,
	}

	settings := gobreaker.Settings{
		Name:        breakerName,
		MaxRequests: 3,
		Interval:    10 * time.Second,
		Timeout:     5 * time.Second, // Open to Half-Open after 5 seconds
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			// Consecutive failures react at once when a healthy service suddenly
			// stops; the ratio catches a dependency that fails only some of the time.
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.ConsecutiveFailures >= 3 || (counts.Requests >= 10 && failureRatio >= 0.5)
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			var stateVal float64
			switch to {
			case gobreaker.StateClosed:
				stateVal = 0.0
			case gobreaker.StateHalfOpen:
				stateVal = 1.0
			case gobreaker.StateOpen:
				stateVal = 2.0
			}
			metrics.CircuitBreakerState.WithLabelValues(serviceName, breakerName).Set(stateVal)
		},
	}

	wrapper.cb = gobreaker.NewCircuitBreaker(settings)
	// gobreaker changes state only when called. Without traffic the metric would stay
	// at "open" forever and keep the alert firing long after the incident ended.
	go func() {
		for range time.Tick(time.Second) {
			wrapper.cb.State()
		}
	}()
	// Initialize metric to 0 (Closed)
	metrics.CircuitBreakerState.WithLabelValues(serviceName, breakerName).Set(0.0)

	return wrapper
}

func (w *CircuitBreakerWrapper) Execute(req func() (interface{}, error)) (interface{}, error) {
	return w.cb.Execute(req)
}

func (w *CircuitBreakerWrapper) State() gobreaker.State {
	return w.cb.State()
}
