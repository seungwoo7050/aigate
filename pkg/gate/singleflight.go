package gate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"golang.org/x/sync/singleflight"
)

// Result holds the return value of a coalesced call.
type Result struct {
	Val    any
	Err    error
	Shared bool
}

type callState struct {
	cancel context.CancelFunc
	done   chan struct{}
	res    singleflight.Result
	mu     sync.Mutex
	subs   int
}

// Group represents a single-flight group that isolates caller context cancellations.
type Group struct {
	sfg   singleflight.Group
	mu    sync.Mutex
	calls map[string]*callState
}

// NewGroup creates an initialized single-flight group.
func NewGroup() *Group {
	return &Group{
		calls: make(map[string]*callState),
	}
}

// RequestKey computes a deduplication hash key from the HTTP method, path, and request body.
func RequestKey(method, path string, body []byte) string {
	h := sha256.New()
	h.Write(body)
	return method + " " + path + "#" + hex.EncodeToString(h.Sum(nil))
}

// Do executes and returns the results of the given function, making sure that only one execution
// is in-flight for a given key at a time.
func (g *Group) Do(ctx context.Context, key string, fn func(execCtx context.Context) (any, error)) (any, error, bool) {
	if ctx.Err() != nil {
		return nil, ctx.Err(), false
	}

	g.mu.Lock()
	st, ok := g.calls[key]
	if ok {
		st.mu.Lock()
		if st.subs > 0 {
			st.subs++
			st.mu.Unlock()
			g.mu.Unlock()
			return g.wait(ctx, key, st, true)
		}
		st.mu.Unlock()
		delete(g.calls, key)
		g.sfg.Forget(key)
	}

	execCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	st = &callState{
		cancel: cancel,
		done:   make(chan struct{}),
		subs:   1,
	}
	g.calls[key] = st
	g.mu.Unlock()

	g.sfg.DoChan(key, func() (any, error) {
		var val any
		var err error
		defer func() {
			st.res = singleflight.Result{Val: val, Err: err}
			close(st.done)
			g.mu.Lock()
			delete(g.calls, key)
			g.mu.Unlock()
		}()
		val, err = fn(execCtx)
		return val, err
	})

	return g.wait(ctx, key, st, false)
}

func (g *Group) wait(ctx context.Context, key string, st *callState, isFollower bool) (any, error, bool) {
	select {
	case <-st.done:
		return st.res.Val, st.res.Err, isFollower
	case <-ctx.Done():
		st.mu.Lock()
		st.subs--
		shouldCancel := (st.subs <= 0)
		st.mu.Unlock()
		if shouldCancel {
			st.cancel()
			g.mu.Lock()
			delete(g.calls, key)
			g.mu.Unlock()
			g.sfg.Forget(key)
		}
		return nil, ctx.Err(), isFollower
	}
}

// Forget removes the key from the single-flight group.
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	g.sfg.Forget(key)
}
