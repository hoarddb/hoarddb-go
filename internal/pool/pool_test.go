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

package pool

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeConn struct {
	id     int
	done   chan struct{}
	once   sync.Once
	closed atomic.Bool
}

func newFakeConn(id int) *fakeConn { return &fakeConn{id: id, done: make(chan struct{})} }

func (conn *fakeConn) Done() <-chan struct{} { return conn.done }
func (conn *fakeConn) Close() error {
	conn.closed.Store(true)
	conn.once.Do(func() { close(conn.done) })
	return nil
}

// die simulates the peer going away: Done closes without Close being called.
func (conn *fakeConn) die() { conn.once.Do(func() { close(conn.done) }) }

type dialer struct {
	mu    sync.Mutex
	count int
	conns []*fakeConn
	fail  error
	delay time.Duration
}

func (d *dialer) dial(ctx context.Context) (*fakeConn, error) {
	d.mu.Lock()
	d.count++
	id := d.count
	fail := d.fail
	delay := d.delay
	d.mu.Unlock()
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if fail != nil {
		return nil, fail
	}
	conn := newFakeConn(id)
	d.mu.Lock()
	d.conns = append(d.conns, conn)
	d.mu.Unlock()
	return conn, nil
}

func (d *dialer) dials() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.count
}

func (d *dialer) setFail(err error) {
	d.mu.Lock()
	d.fail = err
	d.mu.Unlock()
}

type clock struct{ now atomic.Int64 }

func newClock() *clock {
	c := &clock{}
	c.now.Store(time.Unix(1_800_000_000, 0).UnixNano())
	return c
}
func (c *clock) Now() time.Time          { return time.Unix(0, c.now.Load()) }
func (c *clock) Advance(d time.Duration) { c.now.Add(int64(d)) }

func newPool(d *dialer, mutate func(*Config[*fakeConn])) *Pool[*fakeConn] {
	config := Config[*fakeConn]{Dial: d.dial}
	if mutate != nil {
		mutate(&config)
	}
	return New(config)
}

func TestNothingIsDialledUntilFirstUse(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	if d.dials() != 0 || pool.Stats().Open != 0 {
		t.Fatal("a new pool must hold no connections")
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if d.dials() != 1 {
		t.Fatalf("%d dials after one use, want 1", d.dials())
	}
}

func TestReleasedConnectionIsReused(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	for range 10 {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		lease.Release()
	}
	if d.dials() != 1 {
		t.Fatalf("%d dials for 10 sequential uses, want 1", d.dials())
	}
}

func TestDefaultsAreFourConnectionsOneInFlight(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	var leases []*Lease[*fakeConn]
	for range 4 {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		leases = append(leases, lease)
	}
	if d.dials() != 4 {
		t.Fatalf("%d dials for 4 concurrent leases, want 4 (one in flight per connection)", d.dials())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("fifth lease: err = %v, want to block until the deadline", err)
	}
	if d.dials() != 4 {
		t.Fatalf("the pool exceeded MaxConns: %d dials", d.dials())
	}
	for _, lease := range leases {
		lease.Release()
	}
}

func TestLeastLoadedConnectionWinsWhenPipelining(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns, c.MaxInFlight = 2, 3 })
	defer func() { _ = pool.Close() }()
	first, _ := pool.Acquire(context.Background())  // conn 1, load 1
	second, _ := pool.Acquire(context.Background()) // conn 1 still has room: reused, load 2
	if first.Conn() != second.Conn() {
		t.Fatal("room on an open connection is used before dialling another")
	}
	// Fill conn 1, then conn 2 appears, and it is then the least loaded.
	third, _ := pool.Acquire(context.Background())
	fourth, _ := pool.Acquire(context.Background())
	if third.Conn() != first.Conn() || fourth.Conn() == first.Conn() {
		t.Fatalf("expected the third lease on conn 1 (now full) and the fourth on a new conn; got %d and %d", third.Conn().id, fourth.Conn().id)
	}
	fifth, _ := pool.Acquire(context.Background())
	if fifth.Conn() != fourth.Conn() {
		t.Fatal("the least-loaded connection must be chosen")
	}
	for _, lease := range []*Lease[*fakeConn]{first, second, third, fourth, fifth} {
		lease.Release()
	}
}

func TestContentionNeverExceedsTheBoundsAndEveryoneFinishes(t *testing.T) {
	const (
		maxConns = 3
		workers  = 50
		rounds   = 20
	)
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = maxConns })
	defer func() { _ = pool.Close() }()
	var (
		inUse, peak atomic.Int32
		completed   atomic.Int32
		group       sync.WaitGroup
	)
	perConn := sync.Map{}
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for range rounds {
				lease, err := pool.Acquire(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				now := inUse.Add(1)
				for {
					seen := peak.Load()
					if now <= seen || peak.CompareAndSwap(seen, now) {
						break
					}
				}
				counter, _ := perConn.LoadOrStore(lease.Conn(), new(atomic.Int32))
				if counter.(*atomic.Int32).Add(1) != 1 {
					t.Error("two leases on one connection at MaxInFlight 1")
				}
				time.Sleep(50 * time.Microsecond)
				counter.(*atomic.Int32).Add(-1)
				inUse.Add(-1)
				lease.Release()
				completed.Add(1)
			}
		}()
	}
	group.Wait()
	if got := completed.Load(); got != workers*rounds {
		t.Fatalf("%d of %d checkouts completed", got, workers*rounds)
	}
	if peak.Load() > maxConns {
		t.Fatalf("%d leases at once with MaxConns %d", peak.Load(), maxConns)
	}
	if d.dials() > maxConns {
		t.Fatalf("%d dials; healthy connections must be reused, not redialled", d.dials())
	}
	stats := pool.Stats()
	if stats.InUse != 0 || stats.Waiting != 0 || stats.Open > maxConns {
		t.Fatalf("stats after the storm: %+v", stats)
	}
}

func TestBlockedCheckoutHonoursContextAndLeavesNoWaiter(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = 1 })
	defer func() { _ = pool.Close() }()
	held, _ := pool.Acquire(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(ctx)
		result <- err
	}()
	waitFor(t, func() bool { return pool.Stats().Waiting == 1 })
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if pool.Stats().Waiting != 0 {
		t.Fatalf("waiting = %d after cancellation", pool.Stats().Waiting)
	}
	held.Release()
}

func TestReleaseWakesAWaiter(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = 1 })
	defer func() { _ = pool.Close() }()
	held, _ := pool.Acquire(context.Background())
	got := make(chan *fakeConn, 1)
	go func() {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Error(err)
			got <- nil
			return
		}
		got <- lease.Conn()
		lease.Release()
	}()
	waitFor(t, func() bool { return pool.Stats().Waiting == 1 })
	heldConn := held.Conn()
	held.Release()
	if conn := <-got; conn != heldConn {
		t.Fatalf("waiter got %v, want the released connection", conn)
	}
}

func TestDiscardClosesAndTheNextCheckoutDials(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	lease, _ := pool.Acquire(context.Background())
	bad := lease.Conn()
	lease.Discard()
	lease.Discard() // idempotent
	lease.Release() // and once discarded, a Release is a no-op
	if !bad.closed.Load() {
		t.Fatal("a discarded connection must be closed")
	}
	if pool.Stats().Open != 0 || pool.Stats().Discarded != 1 {
		t.Fatalf("stats = %+v", pool.Stats())
	}
	next, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next.Conn() == bad || d.dials() != 2 {
		t.Fatalf("expected a fresh dial; dials = %d", d.dials())
	}
	next.Release()
}

func TestDeadConnectionIsNeverHandedOut(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	lease, _ := pool.Acquire(context.Background())
	victim := lease.Conn()
	lease.Release()
	victim.die() // the server closed an idle connection
	next, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer next.Release()
	if next.Conn() == victim {
		t.Fatal("a connection whose reader saw the peer go away was handed out")
	}
	if !victim.closed.Load() && d.dials() != 2 {
		t.Fatalf("dials = %d", d.dials())
	}
}

func TestConnectionThatDiesWhileLeasedIsDroppedOnRelease(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	lease, _ := pool.Acquire(context.Background())
	lease.Conn().die()
	lease.Release()
	if pool.Stats().Open != 0 {
		t.Fatalf("open = %d; a dead connection must not return to the pool", pool.Stats().Open)
	}
}

func TestIdleConnectionsAreTrimmedAtCheckout(t *testing.T) {
	d := &dialer{}
	clk := newClock()
	pool := newPool(d, func(c *Config[*fakeConn]) { c.Now = clk.Now; c.IdleTimeout = 120 * time.Second })
	defer func() { _ = pool.Close() }()
	lease, _ := pool.Acquire(context.Background())
	idle := lease.Conn()
	lease.Release()
	clk.Advance(119 * time.Second)
	again, _ := pool.Acquire(context.Background())
	if again.Conn() != idle {
		t.Fatal("a connection inside the idle timeout must be reused")
	}
	again.Release()
	clk.Advance(121 * time.Second)
	fresh, _ := pool.Acquire(context.Background())
	defer fresh.Release()
	if fresh.Conn() == idle || !idle.closed.Load() {
		t.Fatal("a connection idle past the timeout must be closed and replaced")
	}
}

func TestDialBackoffDoublesToTheCapWithFullJitter(t *testing.T) {
	boom := errors.New("connection refused")
	d := &dialer{fail: boom}
	clk := newClock()
	var limits []time.Duration
	pool := newPool(d, func(c *Config[*fakeConn]) {
		c.Now = clk.Now
		c.BackoffBase, c.BackoffMax = 100*time.Millisecond, 5*time.Second
		c.Jitter = func(limit time.Duration) time.Duration {
			limits = append(limits, limit)
			return limit // the worst case, so the window is exactly the limit
		}
	})
	defer func() { _ = pool.Close() }()

	for attempt := 0; attempt < 8; attempt++ {
		if _, err := pool.Acquire(context.Background()); !errors.Is(err, boom) {
			t.Fatalf("attempt %d: err = %v", attempt, err)
		}
		// Inside the window, the pool refuses without dialling.
		dials := d.dials()
		var backoff *BackoffError
		if _, err := pool.Acquire(context.Background()); !errors.As(err, &backoff) || !errors.Is(err, boom) {
			t.Fatalf("attempt %d inside the window: err = %v, want a *BackoffError wrapping the dial error", attempt, err)
		}
		if d.dials() != dials {
			t.Fatalf("attempt %d: dialled inside the backoff window", attempt)
		}
		clk.Advance(5 * time.Second)
	}
	want := []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000}
	for index, limit := range limits {
		if limit != want[index]*time.Millisecond {
			t.Fatalf("jitter limits = %v; want 100ms doubling to a 5s cap", limits)
		}
	}
	if pool.Stats().DialErrors != 8 {
		t.Fatalf("dial errors = %d", pool.Stats().DialErrors)
	}
}

func TestBackoffEndsOnSuccess(t *testing.T) {
	d := &dialer{fail: errors.New("down")}
	clk := newClock()
	pool := newPool(d, func(c *Config[*fakeConn]) {
		c.Now = clk.Now
		c.Jitter = func(limit time.Duration) time.Duration { return limit }
	})
	defer func() { _ = pool.Close() }()
	_, _ = pool.Acquire(context.Background())
	d.setFail(nil)
	clk.Advance(time.Minute)
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	// The failure streak is forgotten: the next failure starts from the base.
	var limits []time.Duration
	pool.config.Jitter = func(limit time.Duration) time.Duration { limits = append(limits, limit); return limit }
	lease, _ = pool.Acquire(context.Background())
	lease.Discard()
	d.setFail(errors.New("down again"))
	_, _ = pool.Acquire(context.Background())
	if len(limits) != 1 || limits[0] != DefaultBackoffBase {
		t.Fatalf("limits = %v; a success must reset the streak to the base %v", limits, DefaultBackoffBase)
	}
}

func TestDialFailureDoesNotStarveWaitersForExistingConnections(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = 2 })
	defer func() { _ = pool.Close() }()
	held, _ := pool.Acquire(context.Background())
	d.setFail(errors.New("second dial refused"))
	if _, err := pool.Acquire(context.Background()); err == nil {
		t.Fatal("the failing dial should surface")
	}
	// Now backing off with one connection busy: a checkout waits for it rather
	// than failing, because there is something to wait for.
	got := make(chan error, 1)
	go func() {
		lease, err := pool.Acquire(context.Background())
		if err == nil {
			lease.Release()
		}
		got <- err
	}()
	waitFor(t, func() bool { return pool.Stats().Waiting == 1 })
	held.Release()
	if err := <-got; err != nil {
		t.Fatalf("waiter: %v", err)
	}
}

func TestDialHonoursContext(t *testing.T) {
	d := &dialer{delay: time.Minute}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := pool.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if pool.Stats().Open != 0 {
		t.Fatal("a dial that never finished left a connection behind")
	}
}

func TestCloseWakesWaitersClosesConnectionsAndIsIdempotent(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = 1 })
	held, _ := pool.Acquire(context.Background())
	conn := held.Conn()
	result := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(context.Background())
		result <- err
	}()
	waitFor(t, func() bool { return pool.Stats().Waiting == 1 })
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("waiter err = %v, want ErrClosed", err)
	}
	if !conn.closed.Load() {
		t.Fatal("Close must close connections, leased ones included")
	}
	held.Release() // must not panic or dial
	if _, err := pool.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("after Close: err = %v", err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseDuringDialClosesTheNewConnection(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var dialed *fakeConn
	pool := New(Config[*fakeConn]{Dial: func(context.Context) (*fakeConn, error) {
		close(started)
		<-release
		dialed = newFakeConn(1)
		return dialed, nil
	}})
	result := make(chan error, 1)
	go func() {
		_, err := pool.Acquire(context.Background())
		result <- err
	}()
	<-started
	_ = pool.Close()
	close(release)
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
	if !dialed.closed.Load() {
		t.Fatal("a connection that finished dialling into a closed pool leaked")
	}
}

func TestAdoptCountsAgainstTheBoundAndIsUsedWithoutDialling(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, func(c *Config[*fakeConn]) { c.MaxConns = 1 })
	defer func() { _ = pool.Close() }()
	adopted := newFakeConn(99)
	if !pool.Adopt(adopted) {
		t.Fatal("Adopt into an empty pool failed")
	}
	extra := newFakeConn(100)
	if pool.Adopt(extra) || !extra.closed.Load() {
		t.Fatal("Adopt beyond MaxConns must refuse and close the connection")
	}
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Conn() != adopted || d.dials() != 0 {
		t.Fatalf("the adopted connection should serve the first checkout without a dial (dials = %d)", d.dials())
	}
}

// TestPoolStartsNoGoroutines pins the "every goroutine has an owner" rule: the
// pool is passive, so closing it leaves nothing behind.
func TestPoolStartsNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()
	d := &dialer{}
	pool := newPool(d, nil)
	for range 20 {
		lease, err := pool.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		lease.Release()
	}
	_ = pool.Close()
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d -> %d", before, after)
	}
}

func TestCancelledCallerNeverDialsAndEarnsNoBackoff(t *testing.T) {
	d := &dialer{}
	pool := newPool(d, nil)
	defer func() { _ = pool.Close() }()
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := pool.Acquire(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if d.dials() != 0 {
		t.Fatalf("a checkout whose context is already done dialled %d times", d.dials())
	}

	// A dial the caller abandons halfway is not the server's fault either.
	d.mu.Lock()
	d.delay = time.Minute
	d.mu.Unlock()
	short, release := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer release()
	if _, err := pool.Acquire(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	d.mu.Lock()
	d.delay = 0
	d.mu.Unlock()
	lease, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("the next caller was backed off because the previous one gave up: %v", err)
	}
	lease.Release()
	if pool.Stats().DialErrors != 0 {
		t.Fatalf("DialErrors = %d; a caller's cancellation is not a dial error", pool.Stats().DialErrors)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
