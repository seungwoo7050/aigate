package gate

import (
	"errors"
	"sync"
	"time"

	"github.com/sony/gobreaker/v2"
)

// State represents the operational state of a circuit breaker.
type State int

const (
	// StateClosed indicates normal operation; requests are passed to upstream.
	StateClosed State = iota
	// StateHalfOpen indicates recovery probe mode; limited requests are admitted to test upstream health.
	StateHalfOpen
	// StateOpen indicates tripped state; all requests fail fast to prevent cascading failure.
	StateOpen
)

// String returns a human-readable representation of the breaker state.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateHalfOpen:
		return "HalfOpen"
	case StateOpen:
		return "Open"
	default:
		return "Unknown"
	}
}

// CircuitBreaker wraps gobreaker.TwoStepCircuitBreaker with probe concurrency gating and wall-clock cooldown support.
type CircuitBreaker struct {
	mu       sync.Mutex
	cfg      BreakerConfig
	cb       *gobreaker.TwoStepCircuitBreaker[any]
	openedAt time.Time
	lastNow  time.Time
}

// NewCircuitBreaker creates a new CircuitBreaker with the given configuration.
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 3
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Second
	}
	if cfg.MaxHalfOpenProbes <= 0 {
		cfg.MaxHalfOpenProbes = 1
	}
	cb := &CircuitBreaker{cfg: cfg}
	cb.init()
	return cb
}

func (cb *CircuitBreaker) init() {
	st := gobreaker.Settings{
		MaxRequests: uint32(cb.cfg.MaxHalfOpenProbes),
		Timeout:     time.Nanosecond, // Cooldown gating is handled via wall-clock now parameter
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(cb.cfg.FailureThreshold)
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			if to == gobreaker.StateOpen {
				cb.openedAt = cb.lastNow
			} else if to == gobreaker.StateClosed {
				cb.openedAt = time.Time{}
			}
		},
	}
	cb.cb = gobreaker.NewTwoStepCircuitBreaker[any](st)
	cb.openedAt = time.Time{}
}

// Allow evaluates whether a request may proceed based on the breaker's current state and wall-clock time.
// If allowed, it returns (true, doneFn) where doneFn must be invoked with true (success) or false (failure).
// If rejected, it returns (false, nil).
func (cb *CircuitBreaker) Allow(now time.Time) (bool, func(success bool)) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastNow = now

	// 1. If Open and still within Cooldown period, fast-fail
	if !cb.openedAt.IsZero() && now.Sub(cb.openedAt) < cb.cfg.Cooldown {
		return false, nil
	}

	// 2. Delegate to underlying gobreaker engine
	done, err := cb.cb.Allow()
	if err != nil {
		return false, nil
	}

	var once sync.Once
	return true, func(success bool) {
		once.Do(func() {
			cb.mu.Lock()
			defer cb.mu.Unlock()
			cb.lastNow = time.Now()
			if success {
				done(nil)
			} else {
				done(errors.New("request failed"))
			}
		})
	}
}

// State returns the current State, transitioning from Open to HalfOpen if Cooldown has elapsed.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if !cb.openedAt.IsZero() {
		if time.Since(cb.openedAt) < cb.cfg.Cooldown {
			return StateOpen
		}
		return StateHalfOpen
	}
	return State(cb.cb.State())
}

// Reset restores the breaker to StateClosed and clears failure counters.
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.init()
}

// Trip forces the breaker into StateOpen immediately (useful for manual intervention or testing).
func (cb *CircuitBreaker) Trip(now time.Time) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.lastNow = now
	cb.openedAt = now
	for i := 0; i < cb.cfg.FailureThreshold; i++ {
		done, err := cb.cb.Allow()
		if err == nil {
			done(errors.New("trip"))
		}
	}
}
