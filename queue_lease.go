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
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Leases (handbook/api-go.md section 5, handbook/operations.md "Leases and the
// dead list"). A claim takes entries under a lease and leaves them in the store;
// an ack removes the ones named. Claim and Ack are the lossless pair: a lost
// answer to either costs a redelivery, never an entry, so the driver never
// resends one. They work on fifo, lifo and heap buckets.

// The limits of the lease verbs, from testdata/driver-vectors/consume-constants.json.
const (
	claimLimitMax = 100
	leaseMin      = time.Second
	leaseMax      = time.Hour
	ackItemIDsMax = 100
)

// LeaseID names one lease: the 20 bytes a claim answers in lease_id. Keep the
// String form to ack from another process or after a restart.
type LeaseID struct{ raw [20]byte }

// String is the 40 hexadecimal characters the server uses.
func (l LeaseID) String() string { return hex.EncodeToString(l.raw[:]) }

// ParseLeaseID reads the String form of a lease id back, for an ack from another
// process or after a restart.
func ParseLeaseID(text string) (LeaseID, error) {
	var parsed LeaseID
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) != len(parsed.raw) {
		return LeaseID{}, &Error{Code: CodeInvalidArgument, Op: "parse lease id", Target: text, Message: fmt.Sprintf(
			"%q is not a lease id: a lease id is the %d hexadecimal characters Claimed.LeaseID.String() gives. "+
				"Claim again with Bucket.Claim and keep the id it returns", text, 2*len(parsed.raw))}
	}
	copy(parsed.raw[:], decoded)
	return parsed, nil
}

// LeaseState is where a lease stands after an ack, release or extend.
type LeaseState string

// The states the server reports.
const (
	LeaseAcked    LeaseState = "acked"
	LeaseReleased LeaseState = "released"
	LeaseExtended LeaseState = "extended"
	// LeaseExpired: the lease's time ran out and its entries went back.
	LeaseExpired LeaseState = "expired"
	// LeaseUnknown: nothing holds the lease.
	LeaseUnknown LeaseState = "unknown"
)

// Delivery is one claimed entry.
type Delivery struct {
	// ItemID is the handle Ack names. It is not the document's _id_.
	ItemID   string
	Document Document
	// Deliveries is at least how many times this entry has been claimed,
	// counting this one: 1 on the first.
	Deliveries int

	bucket *Bucket
	lease  LeaseID
	// batch is set on a delivery Consume made, so an Extend moves the deadline
	// of the handlers it started.
	batch *consumeBatch
}

// Extend moves the whole lease's deadline to now plus by (1 second to 1 hour)
// and returns the length the server granted. It extends every entry of the
// lease, not only this one. Inside a [Bucket.Consume] handler it also moves the
// handler deadline of the batch.
func (d Delivery) Extend(ctx context.Context, by time.Duration) (time.Duration, error) {
	sentAt := time.Now()
	granted, err := d.bucket.Extend(ctx, d.lease, by)
	if err == nil && d.batch != nil {
		d.batch.moveDeadline(sentAt, granted)
	}
	return granted, err
}

// Claimed is what one claim took.
type Claimed struct {
	LeaseID LeaseID
	// Granted is the length of the lease. Compute a handler deadline from it and
	// the clock of the process, not from a time the server sent.
	Granted time.Duration
	Items   []Delivery
	// Pending and Dead count the entries held under a lease and on the dead
	// list after this claim; 0 when none.
	Pending, Dead int64
	// Returned counts expired leases this claim gave back. More than 0 with no
	// items means entries are available again: claim again at once.
	Returned int64
}

// AckResult is what an ack did.
type AckResult struct {
	State LeaseState
	// Acked entries were removed. Missing ids named nothing the lease holds.
	// Remaining entries are still under the lease.
	Acked, Missing, Remaining int64
	// Message is the server's sentence, set when the lease was unknown or expired.
	Message string
}

// ReleaseResult is what a release did.
type ReleaseResult struct {
	State LeaseState
	// Released entries went back to the queue at once.
	Released int64
	Message  string
}

type claimConfig struct {
	write   writeConfig
	limit   int64
	lease   time.Duration
	invalid error
}

// ClaimOption changes one claim: [Bucket.Claim]. [WithLimit], [WithLease] and
// every [WriteOption] are ClaimOptions.
type ClaimOption interface{ applyClaim(*claimConfig) }

func (option WriteOption) applyClaim(config *claimConfig) { option(&config.write) }

func (option LimitOption) applyClaim(config *claimConfig) {
	config.limit, config.invalid = option.within("a claim takes", claimLimitMax, "; claim again for more")
}

// LeaseOption is how long a claim holds its entries. [WithLease] makes one; it
// is a [ClaimOption] and a [ConsumeOption].
type LeaseOption struct{ length time.Duration }

// WithLease sets how long a claim holds its entries before they are available
// again: 1 second to 1 hour, 30 seconds by default. A length outside that is an
// [ErrInvalidArgument] and nothing is sent.
func WithLease(length time.Duration) LeaseOption { return LeaseOption{length: length} }

func (option LeaseOption) checked() (time.Duration, error) {
	if option.length < leaseMin || option.length > leaseMax {
		return 0, fmt.Errorf("WithLease(%s): a lease is from %s to %s; extend a running lease with Delivery.Extend instead of claiming longer", option.length, leaseMin, leaseMax)
	}
	return option.length, nil
}

func (option LeaseOption) applyClaim(config *claimConfig) {
	config.lease, config.invalid = option.checked()
}

func applyClaimOptions(opts []ClaimOption) claimConfig {
	var config claimConfig
	for _, option := range opts {
		option.applyClaim(&config)
	}
	return config
}

// Claim takes up to [WithLimit] entries (1 to 100, one by default) in pop order
// under a lease of [WithLease] (30 seconds by default) and leaves them in the
// store, invisible to Pop, Peek and Find until the lease runs out, is released or
// is acknowledged. It is at-least-once: an entry whose lease runs out is
// delivered again, and after 5 deliveries without an ack it is dead-lettered
// (see [Bucket.DeadList]).
//
// An empty store is a [Claimed] with no items and a nil error: check
// len(Items). When Returned is above 0 the claim took nothing but gave expired
// leases back, and the caller claims again at once. A claim is a write and is never resent: a
// lost answer costs one lease of delay. When the entry at the front is over the
// hard cap the error is [ErrClaimEntryTooLarge] and only Pop unblocks the queue.
func (b *Bucket) Claim(ctx context.Context, opts ...ClaimOption) (Claimed, error) {
	target := b.path()
	config := applyClaimOptions(opts)
	if config.invalid != nil {
		return Claimed{}, refusal("claim", target, config.invalid.Error()+"; nothing was sent")
	}
	if err := config.write.refusePriority("claim", target); err != nil {
		return Claimed{}, err
	}
	concern, err := b.db.client.writeConcern("claim", target, config.write)
	if err != nil {
		return Claimed{}, err
	}
	var answer hoarddbwire.ClaimResponse
	err = b.db.client.do(ctx, call{op: "claim", target: target, opcode: hoarddbwire.OpQueueClaim, answer: hoarddbwire.OpQueueClaim,
		request: hoarddbwire.ClaimRequest{Database: b.db.name, Bucket: b.name, Limit: config.limit,
			LeaseMS: config.lease.Milliseconds(), WriteConcern: concern},
		response: &answer})
	if err != nil {
		return Claimed{}, claimFailure(err)
	}
	if !answer.Found || len(answer.Items) == 0 {
		if answer.Found || answer.Returned == 0 {
			if _, err := emptyList("claim", target, answer.Message); err != nil {
				return Claimed{}, err
			}
		}
		return Claimed{Items: []Delivery{}, Pending: answer.Pending, Dead: answer.Dead, Returned: answer.Returned}, nil
	}
	return b.claimed(target, answer)
}

// claimFailure names the one claim refusal the vectors do not class: the hard
// cap, an INVALID_ARGUMENT whose sentence starts "claim refused".
func claimFailure(err error) error {
	var failure *Error
	if errors.As(err, &failure) && failure.Code == CodeInvalidArgument && strings.HasPrefix(failure.Message, "claim refused") {
		failure.claimTooLarge = true
	}
	return err
}

// claimed builds the result of a claim that took entries.
func (b *Bucket) claimed(target string, answer hoarddbwire.ClaimResponse) (Claimed, error) {
	lease, err := ParseLeaseID(answer.LeaseID)
	if err != nil || len(answer.ItemIDs) != len(answer.Items) ||
		(len(answer.Deliveries) != 0 && len(answer.Deliveries) != len(answer.Items)) {
		return Claimed{}, &Error{Code: CodeInternal, Op: "claim", Target: target, Message: fmt.Sprintf(
			"the server's claim answer is not one this driver can read: %d items, %d item ids, %d delivery counts and lease id %q; "+
				"this driver and the server disagree about its shape", len(answer.Items), len(answer.ItemIDs), len(answer.Deliveries), answer.LeaseID)}
	}
	claimed := Claimed{LeaseID: lease, Granted: time.Duration(answer.LeaseMS) * time.Millisecond,
		Pending: answer.Pending, Dead: answer.Dead, Returned: answer.Returned, Items: make([]Delivery, len(answer.Items))}
	for position, stored := range answer.Items {
		delivery := Delivery{ItemID: answer.ItemIDs[position], Document: Document(stored), Deliveries: 1, bucket: b, lease: lease}
		if len(answer.Deliveries) != 0 {
			delivery.Deliveries = int(answer.Deliveries[position])
		}
		claimed.Items[position] = delivery
	}
	return claimed, nil
}

type ackConfig struct {
	write   writeConfig
	itemIDs []string
	invalid error
}

// AckOption changes one ack: [Bucket.Ack]. [WithItemIDs] and every
// [WriteOption] are AckOptions.
type AckOption interface{ applyAck(*ackConfig) }

func (option WriteOption) applyAck(config *ackConfig) { option(&config.write) }

type itemIDsOption []string

// WithItemIDs acknowledges only the entries with these handles ([Delivery.ItemID]),
// at most 100; the others stay under the lease. Without it an ack settles the
// whole lease.
func WithItemIDs(itemIDs ...string) AckOption { return itemIDsOption(itemIDs) }

func (option itemIDsOption) applyAck(config *ackConfig) {
	switch {
	case len(option) == 0:
		config.invalid = fmt.Errorf("WithItemIDs needs at least one id; leave the option out to acknowledge the whole lease")
	case len(option) > ackItemIDsMax:
		config.invalid = fmt.Errorf("WithItemIDs(%d ids): an ack names at most %d; ack in groups", len(option), ackItemIDsMax)
	default:
		config.itemIDs = option
	}
}

// leaseVerb sends an ack, release or extend and classifies the answer. An
// unknown or expired lease is an in-band success:false; the answer comes back
// beside the typed error so its counts stay readable. The answer is nil when
// the server gave none.
func (b *Bucket) leaseVerb(ctx context.Context, op string, opcode hoarddbwire.Opcode, request hoarddbwire.LeaseRequest, write writeConfig) (*hoarddbwire.LeaseResponse, error) {
	target := b.path()
	if err := write.refusePriority(op, target); err != nil {
		return nil, err
	}
	concern, err := b.db.client.writeConcern(op, target, write)
	if err != nil {
		return nil, err
	}
	request.Database, request.Bucket, request.WriteConcern = b.db.name, b.name, concern
	var answer hoarddbwire.LeaseResponse
	err = b.db.client.do(ctx, call{op: op, target: target, opcode: opcode, answer: opcode, request: request, response: &answer})
	if err != nil {
		return nil, err
	}
	if answer.Success {
		return &answer, nil
	}
	failure := writeRefusal(op, target, answer.Message, answer.WriteReport)
	switch LeaseState(answer.LeaseState) {
	case LeaseUnknown:
		failure.Code = CodeLeaseUnknown
	case LeaseExpired:
		failure.Code = CodeLeaseExpired
	}
	return &answer, failure
}

// Ack removes the claimed entries it names, or every entry of the lease when it
// names none, and reports how many it removed. A lease nothing holds is
// [ErrLeaseUnknown] and one whose time ran out is [ErrLeaseExpired]: the result
// returned beside either error carries the state and the server's sentence, and
// nothing was removed. An ack is a write and is never resent: a lost answer
// costs a redelivery, never an entry.
func (b *Bucket) Ack(ctx context.Context, l LeaseID, opts ...AckOption) (AckResult, error) {
	var config ackConfig
	for _, option := range opts {
		option.applyAck(&config)
	}
	if config.invalid != nil {
		return AckResult{}, refusal("ack", b.path(), config.invalid.Error()+"; nothing was sent")
	}
	answer, err := b.leaseVerb(ctx, "ack", hoarddbwire.OpQueueAck,
		hoarddbwire.LeaseRequest{LeaseID: l.String(), ItemIDs: config.itemIDs}, config.write)
	if answer == nil {
		return AckResult{}, err
	}
	return AckResult{State: LeaseState(answer.LeaseState), Acked: answer.Acked, Missing: answer.Missing,
		Remaining: answer.Remaining, Message: answer.Message}, err
}

// Release gives every entry of the lease back to the queue at once. They count
// as delivered once. A lease nothing holds is [ErrLeaseUnknown] and an expired
// one [ErrLeaseExpired], with the result beside the error.
func (b *Bucket) Release(ctx context.Context, l LeaseID, opts ...WriteOption) (ReleaseResult, error) {
	answer, err := b.leaseVerb(ctx, "release", hoarddbwire.OpQueueRelease,
		hoarddbwire.LeaseRequest{LeaseID: l.String()}, applyWriteOptions(opts))
	if answer == nil {
		return ReleaseResult{}, err
	}
	return ReleaseResult{State: LeaseState(answer.LeaseState), Released: answer.Released, Message: answer.Message}, err
}

// Extend moves the lease's deadline to now plus length (1 second to 1 hour) and
// returns the length granted. A lease nothing holds is [ErrLeaseUnknown] and an
// expired one [ErrLeaseExpired].
func (b *Bucket) Extend(ctx context.Context, l LeaseID, length time.Duration, opts ...WriteOption) (time.Duration, error) {
	target := b.path()
	granted, err := LeaseOption{length: length}.checked()
	if err != nil {
		return 0, refusal("extend", target, err.Error()+"; nothing was sent")
	}
	answer, err := b.leaseVerb(ctx, "extend", hoarddbwire.OpQueueExtend,
		hoarddbwire.LeaseRequest{LeaseID: l.String(), LeaseMS: granted.Milliseconds()}, applyWriteOptions(opts))
	if err != nil {
		return 0, err
	}
	return time.Duration(answer.LeaseMS) * time.Millisecond, nil
}
