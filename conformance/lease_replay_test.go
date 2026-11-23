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

package conformance_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
)

// The public verbs of leg 3 for the lease and dead-list vectors
// (handbook/operations.md "Leases and the dead list").

// leaseLimits are the bounds of testdata/driver-vectors/consume-constants.json
// the driver holds a call to before it sends it.
const (
	claimLimitMax = 100
	leaseMsMin    = 1000
	leaseMsMax    = 3_600_000
)

// normalisedLease is what a vector writes for a lease id that differs from run to
// run: the id the setup's claim answered, or a claim of the request's own.
var normalisedLease = []byte(`"normalised"`)

// withLease writes the lease id the setup's last claim answered into every
// "normalised" of the payload; the payload is unchanged when there is no lease.
func withLease(payload []byte, lease string) []byte {
	if lease == "" {
		return payload
	}
	return bytes.ReplaceAll(payload, normalisedLease, []byte(strconv.Quote(lease)))
}

// claimOptions reads the claim members of a request.
func claimOptions(request requestOf) []hoarddb.ClaimOption {
	var options []hoarddb.ClaimOption
	if limit := request.number("limit"); limit != 0 {
		options = append(options, hoarddb.WithLimit(int(limit)))
	}
	if leaseMs := request.number("lease_ms"); leaseMs != 0 {
		options = append(options, hoarddb.WithLease(time.Duration(leaseMs)*time.Millisecond))
	}
	return options
}

func verbClaim(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Claim(ctx, claimOptions(request)...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if found, _ := answer["found"].(bool); !found {
		if refusal := refusalOf(answer); refusal != "" {
			return inBandRefusal("Claim", got.Items, err, refusal)
		}
		if err != nil || len(got.Items) != 0 {
			return fmt.Errorf("Claim = %+v, %v; the vector is an empty store, which is no items and a nil error", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	documents := make([]hoarddb.Document, len(got.Items))
	itemIDs := make([]any, len(got.Items))
	deliveries := make([]any, len(got.Items))
	redelivered := false
	for position, delivery := range got.Items {
		documents[position], itemIDs[position], deliveries[position] = delivery.Document, delivery.ItemID, number(int64(delivery.Deliveries))
		redelivered = redelivered || delivery.Deliveries != 1
	}
	if got.LeaseID.String() == "" || len(got.LeaseID.String()) != 40 {
		return fmt.Errorf("the lease id %q is not 40 hexadecimal characters", got.LeaseID)
	}
	if err := documentsMatch(vector, "items", answer["items"], documents); err != nil {
		return err
	}
	reported := requestOf{tree: answer}
	want := map[string]any{"item_ids": answer["item_ids"], "lease_ms": number(reported.number("lease_ms"))}
	have := map[string]any{"item_ids": itemIDs, "lease_ms": number(got.Granted.Milliseconds())}
	if listed, present := answer["deliveries"].([]any); present {
		counts := make([]any, len(listed))
		for position, one := range listed {
			counts[position] = number(requestOf{tree: map[string]any{"n": one}}.number("n"))
		}
		want["deliveries"], have["deliveries"] = counts, deliveries
	} else if redelivered {
		return fmt.Errorf("the deliveries are %v; the vector sends none, which is all 1", deliveries)
	}
	for _, count := range []string{"pending", "dead", "returned"} {
		if _, present := answer[count]; present {
			want[count] = number(reported.number(count))
		}
	}
	have["pending"], have["dead"], have["returned"] = number(got.Pending), number(got.Dead), number(got.Returned)
	for _, count := range []string{"pending", "dead", "returned"} {
		if _, present := want[count]; !present && have[count] != number(int64(0)) {
			return fmt.Errorf("%s = %v; the vector has none, which is 0", count, have[count])
		}
		if _, present := want[count]; !present {
			delete(have, count)
		}
	}
	return treesMatch(vector, want, have)
}

// leaseOf is the lease id of a lease request, as the driver holds one.
func leaseOf(request requestOf) (hoarddb.LeaseID, error) {
	return hoarddb.ParseLeaseID(request.text("lease_id"))
}

// stateMatches holds the outcome of an ack, release or extend to the vector's
// lease_state: success is nil, an unknown or expired lease is its typed error.
func stateMatches(answer map[string]any, state hoarddb.LeaseState, err error) error {
	want, _ := answer["lease_state"].(string)
	if state != hoarddb.LeaseState(want) && err == nil {
		return fmt.Errorf("the lease state is %q; the vector says %q", state, want)
	}
	switch want {
	case "unknown":
		if !errors.Is(err, hoarddb.ErrLeaseUnknown) {
			return fmt.Errorf("error %v; the vector's lease is unknown, which is ErrLeaseUnknown", err)
		}
	case "expired":
		if !errors.Is(err, hoarddb.ErrLeaseExpired) {
			return fmt.Errorf("error %v; the vector's lease is expired, which is ErrLeaseExpired", err)
		}
	default:
		return err
	}
	var failure *hoarddb.Error
	message, _ := answer["message"].(string)
	if !errors.As(err, &failure) || failure.Message != message {
		return fmt.Errorf("the error's message %v is not the server's sentence %q", err, message)
	}
	return nil
}

func verbAck(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	lease, err := leaseOf(request)
	if err != nil {
		return err
	}
	var options []hoarddb.AckOption
	if listed, _ := request.tree["item_ids"].([]any); len(listed) != 0 {
		ids := make([]string, len(listed))
		for position, id := range listed {
			ids[position] = fmt.Sprint(id)
		}
		options = append(options, hoarddb.WithItemIDs(ids...))
	}
	got, err := bucketFor(client, request).Ack(ctx, lease, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if problem := stateMatches(answer, got.State, err); problem != nil {
		return problem
	}
	want := requestOf{tree: answer}
	if got.Acked != want.number("acked") || got.Missing != want.number("missing") || got.Remaining != want.number("remaining") {
		return fmt.Errorf("Ack = %+v; the vector acked %d, missed %d and left %d", got, want.number("acked"), want.number("missing"), want.number("remaining"))
	}
	return nil
}

func verbRelease(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	lease, err := leaseOf(request)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Release(ctx, lease)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if problem := stateMatches(answer, got.State, err); problem != nil {
		return problem
	}
	if want := (requestOf{tree: answer}).number("released"); got.Released != want {
		return fmt.Errorf("Release released %d; the vector released %d", got.Released, want)
	}
	return nil
}

func verbExtend(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	lease, err := leaseOf(request)
	if err != nil {
		return err
	}
	granted, err := bucketFor(client, request).Extend(ctx, lease, time.Duration(request.number("lease_ms"))*time.Millisecond)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return stateMatches(answer, "", err)
	}
	if want := (requestOf{tree: answer}).number("lease_ms"); granted.Milliseconds() != want {
		return fmt.Errorf("Extend granted %s; the vector granted %d ms", granted, want)
	}
	return nil
}

func verbDeadList(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	var options []hoarddb.DeadListOption
	if limit := request.number("limit"); limit != 0 {
		options = append(options, hoarddb.WithLimit(int(limit)))
	}
	if after := request.text("after"); after != "" {
		options = append(options, hoarddb.After(after))
	}
	page, err := bucketFor(client, request).DeadList(ctx, options...)
	if err != nil {
		return err
	}
	wanted, _ := answer["items"].([]any)
	if len(page.Items) != len(wanted) || page.Next != fmt.Sprint(orEmpty(answer["next"])) {
		return fmt.Errorf("DeadList = %d entries, next %q; the vector has %d and next %v", len(page.Items), page.Next, len(wanted), answer["next"])
	}
	return nil
}

// orEmpty is "" for an absent member, which fmt would print as <nil>.
func orEmpty(value any) any {
	if value == nil {
		return ""
	}
	return value
}

// moveVerb is a requeue or a discard: both answer the same.
func moveVerb(move func(*hoarddb.Bucket, context.Context, hoarddb.DeadSelector) (hoarddb.DeadMoveResult, error)) verbCall {
	return func(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
		answer, err := answerOf(vector)
		if err != nil {
			return err
		}
		selector := hoarddb.DeadByID(request.text("dead_id"))
		if all, _ := request.tree["all"].(bool); all {
			selector = hoarddb.AllDead()
		}
		if after := request.text("after"); after != "" {
			selector = selector.After(after)
		}
		got, err := move(bucketFor(client, request), ctx, selector)
		if refused, problem := refusedAs(vector, err); refused {
			return problem
		}
		want := requestOf{tree: answer}
		refusals, _ := answer["refused"].([]any)
		if success, _ := answer["success"].(bool); success != (err == nil) {
			return fmt.Errorf("the vector's success is %t; the verb's error is %v", success, err)
		}
		if got.Done != want.number("done") || got.Left != want.number("left") || len(got.Refused) != len(refusals) {
			return fmt.Errorf("result %+v; the vector moved %d, left %d and refused %d", got, want.number("done"), want.number("left"), len(refusals))
		}
		for position, refused := range refusals {
			entry, _ := refused.(map[string]any)
			if got.Refused[position].DeadID != entry["dead_id"] || got.Refused[position].Message != entry["message"] {
				return fmt.Errorf("refusal %d is %+v; the vector's is %v", position, got.Refused[position], entry)
			}
		}
		return nil
	}
}

var (
	verbRequeue = moveVerb(func(b *hoarddb.Bucket, ctx context.Context, sel hoarddb.DeadSelector) (hoarddb.DeadMoveResult, error) {
		return b.Requeue(ctx, sel)
	})
	verbDiscard = moveVerb(func(b *hoarddb.Bucket, ctx context.Context, sel hoarddb.DeadSelector) (hoarddb.DeadMoveResult, error) {
		return b.Discard(ctx, sel)
	})
)

// outOfRange reports whether the driver refuses a claim or extend request
// before sending it, so the vector is replayed on the wire layer.
func outOfRange(vector conformance.Vector, input requestOf) bool {
	switch vector.Request.OpcodeName {
	case "QUEUE_CLAIM":
		leaseMs := input.number("lease_ms")
		return input.number("limit") < 0 || input.number("limit") > claimLimitMax ||
			(leaseMs != 0 && (leaseMs < leaseMsMin || leaseMs > leaseMsMax))
	case "QUEUE_EXTEND":
		leaseMs := input.number("lease_ms")
		return leaseMs < leaseMsMin || leaseMs > leaseMsMax
	}
	return false
}
