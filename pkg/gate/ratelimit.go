package gate

import (
	"math"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type tenantBucket struct {
	limiter    *rate.Limiter
	lastAccess time.Time
}

// TokenBucketLimiter implements a thread-safe token bucket rate limiter keyed by tenant identifier.
type TokenBucketLimiter struct {
	mu      sync.Mutex
	cfg     RateLimitConfig
	buckets map[string]*tenantBucket
}

// NewTokenBucketLimiter creates a TokenBucketLimiter with the specified configuration.
func NewTokenBucketLimiter(cfg RateLimitConfig) *TokenBucketLimiter {
	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = 10.0
	}
	if cfg.Burst <= 0 {
		cfg.Burst = cfg.RequestsPerSecond
	}
	if cfg.IdleTTL <= 0 {
		cfg.IdleTTL = 10 * time.Minute
	}
	return &TokenBucketLimiter{
		cfg:     cfg,
		buckets: make(map[string]*tenantBucket),
	}
}

// Take attempts to consume one token for the given tenantKey at the specified time.
// It returns allowed (true if token was consumed), retryAfter (wait duration before next token is available if rejected),
// and remaining (tokens remaining in bucket after this operation).
func (l *TokenBucketLimiter) Take(tenantKey string, now time.Time) (bool, time.Duration, float64) {
	if tenantKey == "" {
		tenantKey = "default"
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	b, exists := l.buckets[tenantKey]
	if !exists {
		b = &tenantBucket{
			limiter:    rate.NewLimiter(rate.Limit(l.cfg.RequestsPerSecond), int(math.Ceil(l.cfg.Burst))),
			lastAccess: now,
		}
		l.buckets[tenantKey] = b
	}
	b.lastAccess = now

	r := b.limiter.ReserveN(now, 1)
	if !r.OK() {
		return false, time.Hour, b.limiter.TokensAt(now)
	}

	delay := r.DelayFrom(now)
	if delay == 0 {
		return true, 0, b.limiter.TokensAt(now)
	}

	r.CancelAt(now)
	return false, delay, b.limiter.TokensAt(now)
}

// SweepIdle evicts tenant buckets that have been inactive longer than IdleTTL.
// Returns the number of evicted buckets.
func (l *TokenBucketLimiter) SweepIdle(now time.Time) int {
	l.mu.Lock()
	defer l.mu.Unlock()

	evicted := 0
	for key, b := range l.buckets {
		if now.Sub(b.lastAccess) > l.cfg.IdleTTL {
			delete(l.buckets, key)
			evicted++
		}
	}
	return evicted
}

// BucketCount returns the current number of tracked tenant buckets (used for diagnostics and tests).
func (l *TokenBucketLimiter) BucketCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}
