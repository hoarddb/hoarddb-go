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

package hoarddb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Consume is the claim, handle, ack loop (handbook/api-go.md section 5.2). The
// constants are testdata/driver-vectors/consume-constants.json.
const (
	consumeLimitDefault = 10
	consumeLeaseDefault = 30 * time.Second
	// A handler's deadline is the lease minus a margin: a tenth of the lease, at
	// least 250 ms, so the ack goes out before the lease runs out.
	consumeMarginMin      = 250 * time.Millisecond
	consumeMarginFraction = 10
	// releaseTimeout bounds the release of a cancelled batch, which has no
	// caller context left to run under.
	releaseTimeout = 5 * time.Second
)

type consumeConfig struct {
	limit   int64
	lease   time.Duration
	invalid error
}

// ConsumeOption changes [Bucket.Consume]: [WithLimit] and [WithLease].
type ConsumeOption interface{ applyConsume(*consumeConfig) }

func (option LimitOption) applyConsume(config *consumeConfig) {
	config.limit, config.invalid = option.within("a consume claims", claimLimitMax, "")
}

func (option LeaseOption) applyConsume(config *consumeConfig) {
	config.lease, config.invalid = option.checked()
}

// consumeBatch is the shared handler deadline of one claimed batch.
type consumeBatch struct {
	mu       sync.Mutex
	deadline time.Time
	margin   time.Duration
	timer    *time.Timer
}

// moveDeadline pushes the handler deadline out to a lease of granted that began
// when the extending request was sent.
func (batch *consumeBatch) moveDeadline(sentAt time.Time, granted time.Duration) {
	batch.mu.Lock()
	defer batch.mu.Unlock()
	if later := sentAt.Add(granted - batch.margin); later.After(batch.deadline) {
		batch.deadline = later
		batch.timer.Reset(time.Until(later))
	}
}

// Consume claims entries, runs handle for each, and acknowledges the ones whose
// handler returned nil, until ctx ends or a claim fails for a reason waiting
// will not cure. [WithLimit] sets how many entries one claim takes (10 by
// default) and [WithLease] the lease (30 seconds by default).
//
// The handlers of a batch run at the same time, each with a context that ends at
// the handler deadline: the lease minus a margin, counted from the process's own
// clock. The batch is acknowledged when every handler has returned or that
// deadline has passed, whichever is first, so one slow handler does not hold
// back the entries that finished. A handler that needs longer calls
// [Delivery.Extend], which extends the whole lease and moves the deadline. A
// handler that returns an error or panics is not acknowledged: its entry
// expires and is delivered again, and after 5 deliveries it is dead-lettered.
// Handlers must return when their context ends; Consume waits for them before it
// returns.
//
// An empty queue backs off, 100 ms doubling to 5 s with full jitter; a claim
// that took nothing but returned expired leases claims again at once.
//
// Consume backs off and goes on after a transient error, because delivery is
// at-least-once and claiming again is safe: UNAVAILABLE (a lost connection is
// one), TIMEOUT and OVERLOADED on a claim, and the same on an ack, or an ack of
// a lease that expired or is unknown (the entries are delivered again). It ends
// and returns the error for one waiting will not cure: a claim refused as
// [ErrClaimEntryTooLarge] or any other [ErrInvalidArgument] (a wrong store type
// among them), [ErrPermissionDenied], a missing bucket or database
// ([ErrNotFound]), an unsupported member feature, and any other error. It also
// ends when ctx ends: it releases what it has not acknowledged and returns an
// error that wraps the context's.
func (b *Bucket) Consume(ctx context.Context, handle func(ctx context.Context, d Delivery) error, opts ...ConsumeOption) error {
	target := b.path()
	config := consumeConfig{limit: consumeLimitDefault, lease: consumeLeaseDefault}
	for _, option := range opts {
		option.applyConsume(&config)
	}
	if config.invalid != nil {
		return refusal("consume", target, config.invalid.Error()+"; nothing was sent")
	}
	empty := 0
	for {
		if ctx.Err() != nil {
			return interrupted("consume", target, ctx.Err())
		}
		sentAt := time.Now()
		claimed, err := b.Claim(ctx, WithLimit(int(config.limit)), WithLease(config.lease))
		switch {
		case err != nil && transient(err):
			empty++ // waiting may cure it, and re-claiming is safe: delivery is at-least-once
			if err := sleepContext(ctx, backoffDelay(empty)); err != nil {
				return interrupted("consume", target, err)
			}
			continue
		case err != nil:
			return err
		}
		if len(claimed.Items) == 0 {
			if claimed.Returned > 0 {
				empty = 0
				continue // expired leases went back: take them at once
			}
			empty++ // an empty queue: back off
			if err := sleepContext(ctx, backoffDelay(empty)); err != nil {
				return interrupted("consume", target, err)
			}
			continue
		}
		empty = 0
		if err := b.runBatch(ctx, claimed, sentAt, handle); err != nil {
			return err
		}
	}
}

// runBatch runs the handlers of one claim and settles the lease.
func (b *Bucket) runBatch(ctx context.Context, claimed Claimed, sentAt time.Time, handle func(context.Context, Delivery) error) error {
	margin := max(consumeMarginMin, claimed.Granted/consumeMarginFraction)
	batch := &consumeBatch{margin: margin, deadline: sentAt.Add(claimed.Granted - margin)}
	handlers, stop := context.WithCancel(ctx)
	defer stop()
	batch.timer = time.AfterFunc(time.Until(batch.deadline), stop)
	defer batch.timer.Stop()

	var (
		finished sync.WaitGroup
		mu       sync.Mutex
		itemIDs  []string
	)
	for _, delivery := range claimed.Items {
		delivery.batch = batch
		finished.Add(1)
		go func() {
			defer finished.Done()
			defer func() {
				if recovered := recover(); recovered != nil {
					b.db.client.settings.logger.Warn("a consume handler panicked; its entry is not acknowledged and will be delivered again",
						"bucket", b.path(), "panic", fmt.Sprint(recovered))
				}
			}()
			if handle(handlers, delivery) == nil {
				mu.Lock()
				itemIDs = append(itemIDs, delivery.ItemID)
				mu.Unlock()
			}
		}()
	}
	allReturned := make(chan struct{})
	go func() {
		finished.Wait()
		close(allReturned)
	}()
	select {
	case <-allReturned:
	case <-handlers.Done():
	}
	mu.Lock()
	acknowledge := append([]string(nil), itemIDs...)
	mu.Unlock()

	// A cancelled caller's context cannot carry the ack or the release; they
	// run under their own short deadline.
	settle, stopSettle := ctx, func() {}
	if ctx.Err() != nil {
		settle, stopSettle = context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
	}
	defer stopSettle()
	var ackErr error
	if len(acknowledge) > 0 {
		_, ackErr = b.Ack(settle, claimed.LeaseID, WithItemIDs(acknowledge...))
	}
	// Wait for every handler before deciding anything: Consume never returns
	// with a handler still running, and a handler that finishes while the
	// context is ending is settled by the one release below.
	<-allReturned
	if ctx.Err() != nil {
		// One release, after the wait, covers a cancel that came before the
		// handlers finished and one that came after: what is left unacknowledged
		// goes back now, not when the lease runs out.
		b.releaseOnCancel(ctx, claimed.LeaseID)
		return interrupted("consume", b.path(), ctx.Err())
	}
	// An ack that came too late, or whose answer was lost, costs a redelivery and
	// nothing else: the next claim goes on.
	if ackErr != nil && !errors.Is(ackErr, ErrLeaseExpired) && !errors.Is(ackErr, ErrLeaseUnknown) && !transient(ackErr) {
		return ackErr
	}
	return nil
}

// transient reports whether waiting may cure the error: the server could not
// answer here and now (UNAVAILABLE, which a lost connection is too), took too
// long (TIMEOUT) or was overloaded. Everything else is one a person must fix.
func transient(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrTimeout) || errors.Is(err, ErrOverloaded)
}

// releaseOnCancel gives the lease's unacknowledged entries back, under a short
// deadline of its own because the caller's context has ended. A lease that is
// already settled is the expected answer after an ack of everything.
func (b *Bucket) releaseOnCancel(cancelled context.Context, lease LeaseID) {
	ctx, stop := context.WithTimeout(context.WithoutCancel(cancelled), releaseTimeout)
	defer stop()
	if _, err := b.Release(ctx, lease); err != nil && !errors.Is(err, ErrLeaseUnknown) && !errors.Is(err, ErrLeaseExpired) {
		b.db.client.settings.logger.Warn("consume could not release its lease on cancellation; the entries return when it expires",
			"bucket", b.path(), "error", err)
	}
}
