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
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// Real-server tests of leases, the dead list and Consume. Each starts its own
// server and needs HOARDDB_SERVER_BIN.

func pushNumbers(t *testing.T, bucket *hoarddb.Bucket, count int) {
	t.Helper()
	for n := 1; n <= count; n++ {
		if _, err := bucket.Push(testContext(t), hoarddb.Map{"n": n}, hoarddb.WithPriority(int64(n))); err != nil {
			t.Fatalf("Push %d: %v", n, err)
		}
	}
}

func queueNumbers(t *testing.T, documents []hoarddb.Document) []int64 {
	t.Helper()
	numbers := make([]int64, len(documents))
	for position, one := range documents {
		numbers[position] = intField(t, one, "n")
	}
	return numbers
}

func deliveredNumbers(t *testing.T, items []hoarddb.Delivery) []int64 {
	t.Helper()
	documents := make([]hoarddb.Document, len(items))
	for position, item := range items {
		documents[position] = item.Document
	}
	return queueNumbers(t, documents)
}

// A claim hides entries from Pop, Peek and Find; an ack by item id removes only
// those; the rest stay under the lease.
func TestClaimThenAckAgainstTheServer(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	for _, tc := range []struct {
		kind  hoarddb.StoreType
		first []int64 // what a claim of two takes, from pushes of 1 to 4
		rest  int64   // what a peek then shows first
	}{
		{hoarddb.FIFO, []int64{1, 2}, 3},
		{hoarddb.LIFO, []int64{4, 3}, 2},
		{hoarddb.Heap, []int64{4, 3}, 2},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			bucket := bucketOf(t, client, tc.kind)
			pushNumbers(t, bucket, 4)
			claimed, err := bucket.Claim(ctx, hoarddb.WithLimit(2), hoarddb.WithLease(time.Minute))
			if err != nil || !slices.Equal(deliveredNumbers(t, claimed.Items), tc.first) {
				t.Fatalf("Claim = %+v, %v; want %v", claimed, err, tc.first)
			}
			if claimed.Granted != time.Minute || claimed.Pending != 2 || claimed.LeaseID.String() == "" {
				t.Errorf("Claim = %+v; want a minute, 2 pending", claimed)
			}
			peeked, err := bucket.Peek(ctx, hoarddb.WithLimit(10))
			if err != nil || len(peeked) != 2 || queueNumbers(t, peeked)[0] != tc.rest {
				t.Fatalf("Peek after the claim = %v, %v; the claimed entries must be invisible", queueNumbers(t, peeked), err)
			}
			acked, err := bucket.Ack(ctx, claimed.LeaseID, hoarddb.WithItemIDs(claimed.Items[0].ItemID))
			if err != nil || acked.State != hoarddb.LeaseAcked || acked.Acked != 1 || acked.Remaining != 1 {
				t.Fatalf("Ack of one = %+v, %v; want 1 acked and 1 remaining", acked, err)
			}
			acked, err = bucket.Ack(ctx, claimed.LeaseID) // the rest of the lease
			if err != nil || acked.Acked != 1 {
				t.Fatalf("Ack of the rest = %+v, %v", acked, err)
			}
			// Both are gone for good: popping the other two empties the store.
			left, err := bucket.Pop(ctx, hoarddb.WithLimit(10))
			if err != nil || len(left) != 2 {
				t.Fatalf("Pop of the rest = %v, %v; want the 2 unclaimed entries", queueNumbers(t, left), err)
			}
			if _, err := bucket.Ack(ctx, claimed.LeaseID); !errors.Is(err, hoarddb.ErrLeaseUnknown) {
				t.Fatalf("Ack of a settled lease = %v; want ErrLeaseUnknown", err)
			}
		})
	}
}

func TestClaimOfAnEmptyQueueAndOfAHashAgainstTheServer(t *testing.T) {
	client := connectTo(t, startServer(t))
	ctx := testContext(t)
	if claimed, err := bucketOf(t, client, hoarddb.FIFO).Claim(ctx); err != nil || len(claimed.Items) != 0 {
		t.Fatalf("Claim of an empty queue = %+v, %v; want no items and a nil error", claimed, err)
	}
	_, err := bucketOf(t, client, hoarddb.Hash).Claim(ctx)
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || err == nil {
		t.Fatalf("Claim of a hash = %v; want the server's refusal, not an empty queue", err)
	}
}

// A release returns the entries at once and the next claim counts a second
// delivery; an extend moves the deadline and grants what it was asked.
func TestReleaseRedeliversAndExtendGrantsAgainstTheServer(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	ctx := testContext(t)
	pushNumbers(t, bucket, 1)
	first, err := bucket.Claim(ctx)
	if err != nil || first.Items[0].Deliveries != 1 || first.Granted != 30*time.Second {
		t.Fatalf("Claim = %+v, %v; want a first delivery on the default 30 seconds", first, err)
	}
	granted, err := bucket.Extend(ctx, first.LeaseID, 2*time.Minute)
	if err != nil || granted != 2*time.Minute {
		t.Fatalf("Extend = %s, %v", granted, err)
	}
	released, err := bucket.Release(ctx, first.LeaseID)
	if err != nil || released.State != hoarddb.LeaseReleased || released.Released != 1 {
		t.Fatalf("Release = %+v, %v", released, err)
	}
	again, err := bucket.Claim(ctx)
	if err != nil || again.Items[0].Deliveries != 2 {
		t.Fatalf("second Claim = %+v, %v; want the second delivery", again, err)
	}
	if _, err := bucket.Extend(ctx, first.LeaseID, time.Minute); !errors.Is(err, hoarddb.ErrLeaseUnknown) {
		t.Fatalf("Extend of a released lease = %v; want ErrLeaseUnknown", err)
	}
}

// A lease whose time ran out is expired or unknown to the verbs: either is a
// typed error and the entry is claimable again.
func TestAckAfterTheLeaseRanOut(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	ctx := testContext(t)
	pushNumbers(t, bucket, 1)
	claimed, err := bucket.Claim(ctx, hoarddb.WithLease(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(2500 * time.Millisecond)
	result, err := bucket.Ack(ctx, claimed.LeaseID)
	if !errors.Is(err, hoarddb.ErrLeaseExpired) && !errors.Is(err, hoarddb.ErrLeaseUnknown) {
		t.Fatalf("Ack after the lease ran out = %+v, %v; want ErrLeaseExpired or ErrLeaseUnknown", result, err)
	}
	if result.Message == "" || result.State == "" {
		t.Errorf("the result beside the error is empty: %+v", result)
	}
}

// Consume acknowledges what its handler finished, leaves a failed entry for
// redelivery, and on cancellation releases it so it is available at once.
func TestConsumeAcksSuccessAndReleasesTheRestOnCancel(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	pushNumbers(t, bucket, 5)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	var handled atomic.Int32
	err := bucket.Consume(ctx, func(_ context.Context, d hoarddb.Delivery) error {
		defer func() {
			if handled.Add(1) == 5 {
				cancel()
			}
		}()
		if intField(t, d.Document, "n") == 3 {
			return errors.New("entry 3 cannot be handled")
		}
		return nil
	}, hoarddb.WithLimit(5), hoarddb.WithLease(time.Minute))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v; want an error that wraps context.Canceled", err)
	}
	left, err := bucket.Peek(testContext(t), hoarddb.WithLimit(10))
	if err != nil || !slices.Equal(queueNumbers(t, left), []int64{3}) {
		t.Fatalf("after Consume the queue holds %v (%v); want only the entry whose handler failed, released on cancel", queueNumbers(t, left), err)
	}
}

// Handlers of one batch run at the same time, and an Extend inside a handler
// moves the handler deadline past the lease the claim began with.
func TestConsumeRunsHandlersTogetherAndExtendMovesTheDeadline(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	pushNumbers(t, bucket, 3)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	var (
		mu         sync.Mutex
		running    int
		mostAtOnce int
		finished   atomic.Int32
		cut        atomic.Int32
	)
	err := bucket.Consume(ctx, func(handlerCtx context.Context, d hoarddb.Delivery) error {
		mu.Lock()
		running++
		mostAtOnce = max(mostAtOnce, running)
		mu.Unlock()
		defer func() {
			mu.Lock()
			running--
			mu.Unlock()
			if finished.Add(1) == 3 {
				cancel()
			}
		}()
		if _, err := d.Extend(handlerCtx, time.Minute); err != nil {
			return err
		}
		time.Sleep(1300 * time.Millisecond) // past the 1 s lease the claim began with
		if err := handlerCtx.Err(); err != nil {
			cut.Add(1) // the handler deadline did not move with the extend
			return err
		}
		return nil
	}, hoarddb.WithLimit(3), hoarddb.WithLease(time.Second))
	if cut.Load() != 0 {
		t.Errorf("%d handlers were cut off at the claim's deadline; Delivery.Extend must move it", cut.Load())
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v", err)
	}
	if mostAtOnce != 3 {
		t.Errorf("%d handlers ran at once; want the whole batch of 3", mostAtOnce)
	}
	if left, err := bucket.Peek(testContext(t)); err != nil || len(left) != 0 {
		t.Fatalf("Peek after Consume = %v, %v; want every entry acknowledged (the extend kept the lease)", left, err)
	}
}

// A handler that panics is not acknowledged and does not take Consume down.
func TestConsumeSurvivesAPanickingHandler(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	pushNumbers(t, bucket, 2)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	var handled atomic.Int32
	err := bucket.Consume(ctx, func(_ context.Context, d hoarddb.Delivery) error {
		defer func() {
			if handled.Add(1) == 2 {
				cancel()
			}
		}()
		if intField(t, d.Document, "n") == 1 {
			panic("handler bug")
		}
		return nil
	}, hoarddb.WithLimit(2))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v", err)
	}
	left, err := bucket.Peek(testContext(t), hoarddb.WithLimit(5))
	if err != nil || !slices.Equal(queueNumbers(t, left), []int64{1}) {
		t.Fatalf("the queue holds %v (%v); want the panicking entry released, the other acked", queueNumbers(t, left), err)
	}
}

// Consume backs off on an empty queue and picks up what arrives later.
func TestConsumeWaitsForWorkAndStopsOnATerminalError(t *testing.T) {
	client := connectTo(t, startServer(t))
	bucket := bucketOf(t, client, hoarddb.FIFO)
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	got := make(chan int64, 1)
	done := make(chan error, 1)
	go func() {
		done <- bucket.Consume(ctx, func(_ context.Context, d hoarddb.Delivery) error {
			got <- intField(t, d.Document, "n")
			return nil
		})
	}()
	time.Sleep(300 * time.Millisecond) // the queue is empty: Consume is backing off
	pushNumbers(t, bucket, 1)
	select {
	case n := <-got:
		if n != 1 {
			t.Fatalf("handled n=%d", n)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Consume never picked up the entry pushed after it started")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v after cancel", err)
	}

	// A bucket that cannot be claimed from ends Consume with the refusal, at once.
	hash := bucketOf(t, client, hoarddb.Hash)
	start := time.Now()
	err := hash.Consume(testContext(t), func(context.Context, hoarddb.Delivery) error { return nil })
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || time.Since(start) > 5*time.Second {
		t.Fatalf("Consume of a hash bucket = %v after %s; want the refusal without backing off", err, time.Since(start))
	}
}

func TestLeaseIDsSurviveAProcessRestart(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	ctx := testContext(t)
	pushNumbers(t, bucket, 1)
	claimed, err := bucket.Claim(ctx)
	if err != nil {
		t.Fatal(err)
	}
	kept := claimed.LeaseID.String()
	restored, err := hoarddb.ParseLeaseID(kept)
	if err != nil {
		t.Fatal(err)
	}
	if acked, err := bucket.Ack(ctx, restored); err != nil || acked.Acked != 1 {
		t.Fatalf("Ack with a parsed lease id = %+v, %v; want %s settled", acked, err, fmt.Sprint(kept))
	}
}
