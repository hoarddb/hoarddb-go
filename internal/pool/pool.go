// Copyright 2026 Boeger IT Sh.p.k
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package pool is a bounded pool of connections with lazy dialling, health
// checks by liveness, and reconnection backoff.
//
// The pool is written for a connection that can carry several requests at once,
// even though the driver starts with one: each connection has an in-flight
// limit (default 1), checkout returns the least-loaded connection that is below
// it, and raising the limit is the whole change needed to pipeline (driver-go-
// scope.md section 4.6). The pool starts no goroutines. Liveness comes from the
// connection itself, whose reader goroutine closes its Done channel the moment
// the peer goes away, so a dead connection is dropped before a request is sent
// into it. Idle connections are trimmed at checkout rather than by a timer.
package pool

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Conn is what the pool needs from a connection.
type Conn interface {
	// Done is closed when the connection is no longer usable.
	Done() <-chan struct{}
	// Close releases the connection. It must be safe to call twice.
	Close() error
}

// Defaults, from handbook/api-go.md section 8.
const (
	DefaultMaxConns    = 4
	DefaultIdleTimeout = 120 * time.Second
	DefaultBackoffBase = 100 * time.Millisecond
	DefaultBackoffMax  = 5 * time.Second
)

// ErrClosed is an operation on a closed pool.
var ErrClosed = errors.New("connection pool is closed")

// BackoffError is a checkout refused because the last dial failed a moment ago
// and no connection exists to wait for. It wraps the dial error.
type BackoffError struct {
	// Until is when the pool will dial again.
	Until time.Time
	// Err is the failure being backed off from.
	Err error
}

func (err *BackoffError) Error() string {
	return fmt.Sprintf("not dialling again until %s after: %v", err.Until.Format(time.RFC3339Nano), err.Err)
}

// Unwrap returns the dial error.
func (err *BackoffError) Unwrap() error { return err.Err }

// Config creates a [Pool]. Dial is required.
type Config[C Conn] struct {
	// Dial opens and authenticates one connection.
	Dial func(ctx context.Context) (C, error)
	// MaxConns caps open connections, including those being dialled.
	MaxConns int
	// MaxInFlight is how many leases one connection may serve at once.
	MaxInFlight int
	// IdleTimeout closes a connection unused for this long, checked at checkout.
	IdleTimeout time.Duration
	// BackoffBase and BackoffMax bound the delay after consecutive dial
	// failures: base, doubling, to max, with full jitter.
	BackoffBase time.Duration
	BackoffMax  time.Duration

	// Now and Jitter exist so tests can control time; nil means the real ones.
	// Jitter returns a duration in [0, limit).
	Now    func() time.Time
	Jitter func(limit time.Duration) time.Duration
}

// Stats are aggregate counters. They are never per connection or per bucket.
type Stats struct {
	// Open is connections held, idle or leased.
	Open int
	// InUse is connections with at least one lease.
	InUse int
	// Waiting is checkouts blocked for a free connection.
	Waiting int
	// Dials counts successful dials, DialErrors failed ones, and Discarded the
	// connections dropped as unhealthy or idle.
	Dials, DialErrors, Discarded uint64
}

type entry[C Conn] struct {
	conn     C
	inFlight int
	lastUsed time.Time
}

// Pool is a bounded set of connections. It is safe for concurrent use.
type Pool[C Conn] struct {
	config Config[C]

	mu            sync.Mutex
	entries       []*entry[C]
	dialing       int
	waiting       int
	closed        bool
	changed       chan struct{} // closed and replaced whenever capacity may have appeared
	failures      int
	nextDial      time.Time
	lastDialError error
	stats         Stats
}

// New returns an empty pool; nothing is dialled until the first [Pool.Acquire].
func New[C Conn](config Config[C]) *Pool[C] {
	if config.MaxConns < 1 {
		config.MaxConns = DefaultMaxConns
	}
	if config.MaxInFlight < 1 {
		config.MaxInFlight = 1
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = DefaultIdleTimeout
	}
	if config.BackoffBase <= 0 {
		config.BackoffBase = DefaultBackoffBase
	}
	if config.BackoffMax < config.BackoffBase {
		config.BackoffMax = max(DefaultBackoffMax, config.BackoffBase)
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.Jitter == nil {
		config.Jitter = func(limit time.Duration) time.Duration { return rand.N(limit) }
	}
	return &Pool[C]{config: config, changed: make(chan struct{})}
}

// Lease is one checkout. Exactly one of Release or Discard should be called;
// further calls do nothing.
type Lease[C Conn] struct {
	pool  *Pool[C]
	entry *entry[C]
	once  sync.Once
}

// Conn is the leased connection.
func (lease *Lease[C]) Conn() C { return lease.entry.conn }

// Release returns a healthy connection to the pool.
func (lease *Lease[C]) Release() {
	lease.once.Do(func() { lease.pool.release(lease.entry, false) })
}

// Discard closes the connection and removes it from the pool, for a connection
// that failed, was abandoned by a cancelled request, or must not be reused.
func (lease *Lease[C]) Discard() {
	lease.once.Do(func() { lease.pool.release(lease.entry, true) })
}

// Adopt puts an already authenticated connection into the pool, idle. It
// reports false, and closes the connection, if the pool is closed or full.
func (pool *Pool[C]) Adopt(conn C) bool {
	pool.mu.Lock()
	if pool.closed || len(pool.entries)+pool.dialing >= pool.config.MaxConns {
		pool.mu.Unlock()
		// Never close under the lock: Close may wait on the connection's reader.
		_ = conn.Close()
		return false
	}
	pool.entries = append(pool.entries, &entry[C]{conn: conn, lastUsed: pool.config.Now()})
	pool.stats.Dials++
	pool.broadcast()
	pool.mu.Unlock()
	return true
}

// Acquire returns the least-loaded healthy connection that has room, dialling a
// new one when none has and the pool is below MaxConns. When the pool is full it
// blocks until a lease is released or ctx ends; there is no unbounded queue.
func (pool *Pool[C]) Acquire(ctx context.Context) (*Lease[C], error) {
	counted := false
	defer func() {
		if counted {
			pool.mu.Lock()
			pool.waiting--
			pool.mu.Unlock()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		pool.mu.Lock()
		if pool.closed {
			pool.mu.Unlock()
			return nil, ErrClosed
		}
		now := pool.config.Now()
		dropped := pool.pruneLocked(now)
		if chosen := pool.leastLoadedLocked(); chosen != nil {
			chosen.inFlight++
			chosen.lastUsed = now
			pool.mu.Unlock()
			closeAll(dropped)
			return &Lease[C]{pool: pool, entry: chosen}, nil
		}
		if len(pool.entries)+pool.dialing < pool.config.MaxConns {
			if now.Before(pool.nextDial) && len(pool.entries)+pool.dialing == 0 {
				until, cause := pool.nextDial, pool.lastDialError
				pool.mu.Unlock()
				closeAll(dropped)
				return nil, &BackoffError{Until: until, Err: cause}
			}
			if !now.Before(pool.nextDial) {
				pool.dialing++
				pool.mu.Unlock()
				closeAll(dropped)
				return pool.dial(ctx)
			}
		}
		wake := pool.changed
		if !counted {
			pool.waiting++
			counted = true
		}
		pool.mu.Unlock()
		closeAll(dropped)
		select {
		case <-wake:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// dial opens one connection outside the lock. The caller has already counted it
// in pool.dialing.
func (pool *Pool[C]) dial(ctx context.Context) (*Lease[C], error) {
	conn, err := pool.config.Dial(ctx)
	pool.mu.Lock()
	pool.dialing--
	if err != nil && ctx.Err() != nil {
		// The caller gave up; that says nothing about the server, so it earns no
		// backoff for the next caller.
		pool.broadcast()
		pool.mu.Unlock()
		return nil, err
	}
	if err != nil {
		pool.stats.DialErrors++
		pool.failures++
		pool.lastDialError = err
		pool.nextDial = pool.config.Now().Add(pool.config.Jitter(pool.backoffLimit()))
		pool.broadcast()
		pool.mu.Unlock()
		return nil, err
	}
	pool.failures, pool.lastDialError, pool.nextDial = 0, nil, time.Time{}
	if pool.closed {
		pool.mu.Unlock()
		_ = conn.Close() // the pool closed while this dial was in flight
		return nil, ErrClosed
	}
	pool.stats.Dials++
	added := &entry[C]{conn: conn, inFlight: 1, lastUsed: pool.config.Now()}
	pool.entries = append(pool.entries, added)
	pool.broadcast()
	pool.mu.Unlock()
	return &Lease[C]{pool: pool, entry: added}, nil
}

// backoffLimit is the ceiling of the jitter window after pool.failures
// consecutive failures: base, doubling, capped at max.
func (pool *Pool[C]) backoffLimit() time.Duration {
	limit := pool.config.BackoffBase
	for step := 1; step < pool.failures && limit < pool.config.BackoffMax; step++ {
		limit *= 2
	}
	return min(limit, pool.config.BackoffMax)
}

func (pool *Pool[C]) release(released *entry[C], discard bool) {
	pool.mu.Lock()
	released.inFlight--
	released.lastUsed = pool.config.Now()
	unhealthy := discard || isDone(released.conn)
	var dropped []C
	if unhealthy {
		if pool.removeLocked(released) {
			pool.stats.Discarded++
			dropped = append(dropped, released.conn)
		}
	}
	pool.broadcast()
	pool.mu.Unlock()
	closeAll(dropped)
}

// Close closes every connection and wakes every blocked checkout with
// [ErrClosed]. It is safe to call more than once.
func (pool *Pool[C]) Close() error {
	pool.mu.Lock()
	if pool.closed {
		pool.mu.Unlock()
		return nil
	}
	pool.closed = true
	entries := pool.entries
	pool.entries = nil
	pool.broadcast()
	pool.mu.Unlock()
	for _, held := range entries {
		_ = held.conn.Close()
	}
	return nil
}

// Stats returns the current counters.
func (pool *Pool[C]) Stats() Stats {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	snapshot := pool.stats
	snapshot.Open = len(pool.entries)
	snapshot.Waiting = pool.waiting
	for _, held := range pool.entries {
		if held.inFlight > 0 {
			snapshot.InUse++
		}
	}
	return snapshot
}

// pruneLocked removes connections that are dead or idle past the timeout and
// returns them to be closed once the lock is released.
func (pool *Pool[C]) pruneLocked(now time.Time) []C {
	var dropped []C
	kept := pool.entries[:0]
	for _, held := range pool.entries {
		dead := isDone(held.conn)
		stale := held.inFlight == 0 && now.Sub(held.lastUsed) > pool.config.IdleTimeout
		if dead || stale {
			dropped = append(dropped, held.conn)
			pool.stats.Discarded++
			continue
		}
		kept = append(kept, held)
	}
	for index := len(kept); index < len(pool.entries); index++ {
		pool.entries[index] = nil
	}
	pool.entries = kept
	return dropped
}

func (pool *Pool[C]) leastLoadedLocked() *entry[C] {
	var best *entry[C]
	for _, held := range pool.entries {
		if held.inFlight >= pool.config.MaxInFlight {
			continue
		}
		if best == nil || held.inFlight < best.inFlight {
			best = held
		}
	}
	return best
}

func (pool *Pool[C]) removeLocked(target *entry[C]) bool {
	for index, held := range pool.entries {
		if held == target {
			pool.entries = append(pool.entries[:index], pool.entries[index+1:]...)
			return true
		}
	}
	return false
}

// broadcast wakes every blocked checkout. Caller holds mu.
func (pool *Pool[C]) broadcast() {
	close(pool.changed)
	pool.changed = make(chan struct{})
}

func isDone[C Conn](conn C) bool {
	select {
	case <-conn.Done():
		return true
	default:
		return false
	}
}

func closeAll[C Conn](conns []C) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}
