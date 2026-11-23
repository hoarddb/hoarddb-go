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

package hoarddb_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Component tests of Consume against the fake server: the defaults it sends,
// how it waits, which errors end it and which it survives
// (handbook/api-go.md section 5.2).

// consumeFake is a fake server for Consume. claim answers each QUEUE_CLAIM
// (count is 1-based), ack each QUEUE_ACK; release always succeeds. Requests are
// recorded.
type consumeFake struct {
	server  *fakeServer
	record  *requestRecorder
	claims  atomic.Int32
	acks    atomic.Int32
	release atomic.Int32
}

func newConsumeFake(t *testing.T, claim func(c *fakeConn, request hoarddbwire.Frame, count int), ack func(c *fakeConn, request hoarddbwire.Frame, count int) bool) *consumeFake {
	t.Helper()
	fake := &consumeFake{record: &requestRecorder{}}
	fake.server = startFake(t, serveFramesWith([]string{"write_concern"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		fake.record.add(t, request)
		switch request.Op {
		case hoarddbwire.OpQueueClaim:
			claim(c, request, int(fake.claims.Add(1)))
		case hoarddbwire.OpQueueAck:
			if ack != nil {
				return ack(c, request, int(fake.acks.Add(1)))
			}
			fake.acks.Add(1)
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "acked", Acked: 1})
		case hoarddbwire.OpQueueRelease:
			fake.release.Add(1)
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "released"})
		}
		return true
	}))
	return fake
}

func (fake *consumeFake) bucket(t *testing.T) *hoarddb.Bucket {
	return mustConnect(t, fake.server.uri()).Database("app").Bucket("q")
}

func emptyClaim(c *fakeConn, request hoarddbwire.Frame, _ int) {
	c.send(request, request.Op, hoarddbwire.ClaimResponse{Message: "queue is empty"})
}

func oneEntryClaim(t *testing.T, leaseMS int64) func(c *fakeConn, request hoarddbwire.Frame, count int) {
	return func(c *fakeConn, request hoarddbwire.Frame, _ int) {
		c.send(request, request.Op, hoarddbwire.ClaimResponse{Found: true, Items: []hoarddbwire.Document{adaDocument(t)},
			ItemIDs: []string{"0000000000000000"}, LeaseID: aLeaseID, LeaseMS: leaseMS})
	}
}

// runUntil runs Consume with a context that ends when stop reports true, and
// returns what Consume returned.
func runUntil(t *testing.T, bucket *hoarddb.Bucket, stop func() bool, handle func(context.Context, hoarddb.Delivery) error, opts ...hoarddb.ConsumeOption) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(testContext(t), 10*time.Second)
	defer cancel()
	go func() {
		for !stop() && ctx.Err() == nil {
			time.Sleep(5 * time.Millisecond)
		}
		cancel()
	}()
	return bucket.Consume(ctx, handle, opts...)
}

func noHandler(context.Context, hoarddb.Delivery) error { return nil }

// Without options a claim asks for 10 entries and a 30 second lease.
func TestConsumeClaimsTenEntriesFor30SecondsByDefault(t *testing.T) {
	fake := newConsumeFake(t, emptyClaim, nil)
	_ = runUntil(t, fake.bucket(t), func() bool { return fake.claims.Load() >= 1 }, noHandler)
	members := fake.record.requests()[0].members
	if fmt.Sprint(members["limit"]) != "10" {
		t.Errorf("the default claim sent limit %v; want 10", members["limit"])
	}
	if fmt.Sprint(members["lease_ms"]) != "30000" {
		t.Errorf("the default claim sent lease_ms %v; want 30000", members["lease_ms"])
	}
}

// A claim that returned expired leases and took nothing is claimed again at
// once: Consume neither returns nor waits.
func TestConsumeClaimsAgainAtOnceWhenExpiredLeasesWereReturned(t *testing.T) {
	fake := newConsumeFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ int) {
		c.send(request, request.Op, hoarddbwire.ClaimResponse{Message: "queue is empty", Returned: 2})
	}, nil)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- fake.bucket(t).Consume(ctx, noHandler) }()
	waitUntil(t, func() bool { return fake.claims.Load() >= 40 }) // a backoff would take seconds
	select {
	case err := <-done:
		t.Fatalf("Consume returned %v after %d claims; it must go on while leases keep coming back", err, fake.claims.Load())
	default:
	}
	cancel()
	<-done
}

// An empty queue is waited on: 100 ms doubling with jitter, not a spin.
func TestConsumeBacksOffOnAnEmptyQueue(t *testing.T) {
	fake := newConsumeFake(t, emptyClaim, nil)
	start := time.Now()
	_ = runUntil(t, fake.bucket(t), func() bool { return time.Since(start) > 600*time.Millisecond }, noHandler)
	if claims := fake.claims.Load(); claims < 1 || claims > 15 {
		t.Fatalf("%d claims in 600 ms of an empty queue; want a backoff of 100 ms doubling, a handful at most", claims)
	}
}

// The handler's context ends before the lease does, by a margin: 250 ms or a
// tenth of the lease, whichever is more.
func TestConsumeHandlerDeadlineLeavesAMarginBeforeTheLease(t *testing.T) {
	fake := newConsumeFake(t, oneEntryClaim(t, 2000), nil)
	var cut atomic.Int64
	handled := make(chan struct{}, 1)
	start := time.Now()
	_ = runUntil(t, fake.bucket(t), func() bool { return len(handled) > 0 }, func(ctx context.Context, _ hoarddb.Delivery) error {
		<-ctx.Done()
		cut.CompareAndSwap(0, int64(time.Since(start)))
		select {
		case handled <- struct{}{}:
		default: // a later claim's handler
		}
		return ctx.Err()
	}, hoarddb.WithLimit(1), hoarddb.WithLease(2*time.Second))
	elapsed := time.Duration(cut.Load())
	if elapsed < 1500*time.Millisecond || elapsed > 1900*time.Millisecond {
		t.Fatalf("the handler was cut off after %s of a 2 s lease; want about 1.75 s (the lease less a 250 ms margin)", elapsed)
	}
}

// Consume does not return while a handler is running, even when the caller
// cancels: a handler that is slow to notice is waited for.
func TestConsumeWaitsForHandlersBeforeReturningOnCancel(t *testing.T) {
	fake := newConsumeFake(t, oneEntryClaim(t, 30000), nil)
	var finished atomic.Bool
	started := make(chan struct{}, 1)
	err := runUntil(t, fake.bucket(t), func() bool { return len(started) > 0 }, func(ctx context.Context, _ hoarddb.Delivery) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		time.Sleep(300 * time.Millisecond) // slow to wind down
		finished.Store(true)
		return ctx.Err()
	}, hoarddb.WithLimit(1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v; want the context's cancellation", err)
	}
	if !finished.Load() {
		t.Fatal("Consume returned while a handler was still running")
	}
	if fake.release.Load() != 1 {
		t.Errorf("%d releases; want exactly one, after the handlers returned", fake.release.Load())
	}
}

// A transient claim error is waited out and the loop goes on; a terminal one
// is returned.
func TestConsumeSurvivesTransientClaimErrorsAndEndsOnTerminalOnes(t *testing.T) {
	for _, tc := range []struct {
		name     string
		code     int32
		terminal *hoarddb.Error
	}{
		{"unavailable", 6, nil}, {"timeout", 7, nil},
		{"permission denied", 3, hoarddb.ErrPermissionDenied}, {"invalid argument", 4, hoarddb.ErrInvalidArgument},
		{"not found", 1, hoarddb.ErrNotFound}, {"internal", 5, hoarddb.ErrInternal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newConsumeFake(t, func(c *fakeConn, request hoarddbwire.Frame, count int) {
				if count == 1 {
					c.errorWith(request, map[string]any{"code": tc.code, "message": "refused: " + tc.name})
					return
				}
				emptyClaim(c, request, count)
			}, nil)
			if tc.terminal != nil {
				err := runUntil(t, fake.bucket(t), func() bool { return false }, noHandler)
				if !errors.Is(err, tc.terminal) || !strings.Contains(err.Error(), "refused: "+tc.name) || fake.claims.Load() != 1 {
					t.Fatalf("Consume = %v after %d claims; want %v at once", err, fake.claims.Load(), tc.terminal.Code)
				}
				return
			}
			err := runUntil(t, fake.bucket(t), func() bool { return fake.claims.Load() >= 2 }, noHandler)
			if !errors.Is(err, context.Canceled) || fake.claims.Load() < 2 {
				t.Fatalf("Consume = %v after %d claims; want it to back off and claim again", err, fake.claims.Load())
			}
		})
	}
}

// A claim whose connection is lost is transient too.
func TestConsumeSurvivesALostClaimConnection(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if frames.Add(1) == 1 {
			return false // close without answering the first claim
		}
		emptyClaim(c, request, 0)
		return true
	}))
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	err := runUntil(t, bucket, func() bool { return frames.Load() >= 2 }, noHandler)
	if !errors.Is(err, context.Canceled) || frames.Load() < 2 {
		t.Fatalf("Consume = %v after %d claims; want it to claim again after the lost connection", err, frames.Load())
	}
}

// An ack that fails for a reason waiting cannot cure ends Consume; one that
// fails for a transient reason, or whose answer was lost, does not.
func TestConsumeEndsOnAnAckItCannotCompleteButSurvivesATransientOne(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answer   func(c *fakeConn, request hoarddbwire.Frame) bool
		terminal *hoarddb.Error
	}{
		{"invalid argument", func(c *fakeConn, r hoarddbwire.Frame) bool {
			c.errorWith(r, map[string]any{"code": int32(4), "message": "ack refused", "write_outcome": "not_applied"})
			return true
		}, hoarddb.ErrInvalidArgument},
		{"unavailable", func(c *fakeConn, r hoarddbwire.Frame) bool {
			c.errorWith(r, map[string]any{"code": int32(6), "message": "ack unavailable"})
			return true
		}, nil},
		{"lost answer", func(*fakeConn, hoarddbwire.Frame) bool { return false }, nil},
		{"lease expired", func(c *fakeConn, r hoarddbwire.Frame) bool {
			c.send(r, r.Op, hoarddbwire.LeaseResponse{Success: false, LeaseState: "expired", Message: "the lease expired"})
			return true
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := newConsumeFake(t, oneEntryClaim(t, 30000), func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				return tc.answer(c, request)
			})
			err := runUntil(t, fake.bucket(t), func() bool { return fake.claims.Load() >= 2 }, noHandler, hoarddb.WithLimit(1))
			if tc.terminal != nil {
				if !errors.Is(err, tc.terminal) || fake.claims.Load() != 1 {
					t.Fatalf("Consume = %v after %d claims; want %v at the first failed ack", err, fake.claims.Load(), tc.terminal.Code)
				}
				return
			}
			if !errors.Is(err, context.Canceled) || fake.claims.Load() < 2 {
				t.Fatalf("Consume = %v after %d claims; want it to go on and claim again", err, fake.claims.Load())
			}
		})
	}
}

// OVERLOADED is transient on a claim and on an ack. The retry layer backs it
// off itself until the request timeout gives up; Consume then sees it, waits
// and claims again instead of ending.
func TestConsumeSurvivesOverloadedOnAClaimAndOnAnAck(t *testing.T) {
	overloaded := func(c *fakeConn, request hoarddbwire.Frame) {
		c.errorWith(request, map[string]any{"code": int32(13), "message": "too many requests"})
	}
	for _, tc := range []struct {
		name string
		fake func(t *testing.T) *consumeFake
		// claimsWanted is how many claims show Consume went on.
		claimsWanted int32
	}{
		{"claim", func(t *testing.T) *consumeFake {
			return newConsumeFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ int) { overloaded(c, request) }, nil)
		}, 2},
		{"ack", func(t *testing.T) *consumeFake {
			return newConsumeFake(t, oneEntryClaim(t, 30000), func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				overloaded(c, request)
				return true
			})
		}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fake := tc.fake(t)
			bucket := mustConnect(t, fake.server.uri(), hoarddb.WithRequestTimeout(200*time.Millisecond)).Database("app").Bucket("q")
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- bucket.Consume(ctx, noHandler, hoarddb.WithLimit(1)) }()
			var early error
			waitUntil(t, func() bool {
				select {
				case early = <-done:
					return true
				default:
				}
				return fake.claims.Load() >= tc.claimsWanted
			})
			cancel()
			if early == nil {
				early = <-done
			}
			if !errors.Is(early, context.Canceled) {
				t.Fatalf("Consume = %v after %d claims; want it to wait out OVERLOADED and end only on the context's cancellation", early, fake.claims.Load())
			}
		})
	}
}

// Extend returns the length the server granted, which may differ from the
// length asked for, from Bucket.Extend and from Delivery.Extend alike.
func TestExtendReturnsTheGrantedLengthNotTheRequestedOne(t *testing.T) {
	grants := map[string]int64{"10000": 5000, "3600000": 1800000}
	server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, sent map[string]any) {
		switch request.Op {
		case hoarddbwire.OpQueueClaim:
			c.send(request, request.Op, hoarddbwire.ClaimResponse{Found: true, Items: []hoarddbwire.Document{adaDocument(t)},
				ItemIDs: []string{"0000000000000000"}, LeaseID: aLeaseID, LeaseMS: 30000})
		case hoarddbwire.OpQueueExtend:
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "extended",
				LeaseMS: grants[fmt.Sprint(sent["lease_ms"])]})
		}
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	ctx := testContext(t)
	claimed, err := bucket.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for asked, want := range map[time.Duration]time.Duration{10 * time.Second: 5 * time.Second, time.Hour: 30 * time.Minute} {
		if got, err := bucket.Extend(ctx, claimed.LeaseID, asked); err != nil || got != want {
			t.Errorf("Bucket.Extend(%s) = %s, %v; want the granted %s", asked, got, err, want)
		}
		if got, err := claimed.Items[0].Extend(ctx, asked); err != nil || got != want {
			t.Errorf("Delivery.Extend(%s) = %s, %v; want the granted %s", asked, got, err, want)
		}
	}
}
