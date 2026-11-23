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
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Unit tests of the lease verbs against the in-process fake server, which
// answers what handbook/operations.md says the server answers. The real-server
// proofs follow in this file and in the conformance legs.

const aLeaseID = "0123456789abcdef0123456789abcdef01234567"

func TestLeaseIDRoundTripsAndRefusesWhatIsNotOne(t *testing.T) {
	parsed, err := hoarddb.ParseLeaseID(aLeaseID)
	if err != nil || parsed.String() != aLeaseID {
		t.Fatalf("ParseLeaseID(%q) = %v, %v; want it back as it was", aLeaseID, parsed, err)
	}
	for _, bad := range []string{"", "normalised", aLeaseID[:38], aLeaseID + "00", strings.Repeat("zz", 20)} {
		_, err := hoarddb.ParseLeaseID(bad)
		if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "40 hexadecimal characters") {
			t.Errorf("ParseLeaseID(%q) = %v; want ErrInvalidArgument that says what a lease id is", bad, err)
		}
	}
}

// leaseFake answers each lease opcode with the given function and records the
// requests.
func leaseFake(t *testing.T, answer func(c *fakeConn, request hoarddbwire.Frame, sent map[string]any)) (*fakeServer, *requestRecorder) {
	t.Helper()
	recorder := &requestRecorder{}
	server := startFake(t, serveFramesWith([]string{"write_concern"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		recorder.add(t, request)
		sent := recorder.requests()
		answer(c, request, sent[len(sent)-1].members)
		return true
	}))
	return server, recorder
}

func TestClaimSendsItsOptionsAndReadsTheParallelLists(t *testing.T) {
	ada, grace := adaDocument(t), adaDocument(t)
	server, recorder := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		c.send(request, hoarddbwire.OpQueueClaim, hoarddbwire.ClaimResponse{Found: true, Items: []hoarddbwire.Document{ada, grace},
			ItemIDs: []string{"0000000000000000", "0000000000000001"}, Deliveries: []int64{2, 1}, LeaseID: aLeaseID, LeaseMS: 60000,
			Pending: 2, Dead: 1})
	})
	client := mustConnect(t, server.uri())
	claimed, err := client.Database("app").Bucket("jobs").Claim(testContext(t), hoarddb.WithLimit(2), hoarddb.WithLease(time.Minute),
		hoarddb.WithWriteConcern(hoarddb.Majority))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed.LeaseID.String() != aLeaseID || claimed.Granted != time.Minute || claimed.Pending != 2 || claimed.Dead != 1 || len(claimed.Items) != 2 {
		t.Fatalf("Claim = %+v; want lease %s granted for a minute, 2 pending, 1 dead and 2 items", claimed, aLeaseID)
	}
	if first, second := claimed.Items[0], claimed.Items[1]; first.ItemID != "0000000000000000" || first.Deliveries != 2 ||
		second.ItemID != "0000000000000001" || second.Deliveries != 1 {
		t.Errorf("deliveries = %+v and %+v; want the handles and counts in the order the server listed them", first, second)
	}
	members := recorder.requests()[0].members
	if members["limit"] != int64(2) && members["limit"] != int32(2) || members["lease_ms"] == nil || members["writeconcern"] != "majority" {
		t.Errorf("claim sent %v; want limit 2, lease_ms 60000 and the write concern", members)
	}
}

func TestClaimOfNothingIsAnEmptyResultAndNoError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		answer   hoarddbwire.ClaimResponse
		wantErr  error
		returned int64
	}{
		{"an empty queue", hoarddbwire.ClaimResponse{Message: "queue is empty"}, nil, 0},
		{"an empty heap", hoarddbwire.ClaimResponse{Message: "heap is empty"}, nil, 0},
		{"expired leases came back", hoarddbwire.ClaimResponse{Message: "queue is empty", Returned: 3, Pending: 1}, nil, 3},
		{"a refusal is not an empty queue", hoarddbwire.ClaimResponse{Message: "claim is not defined for hash buckets: a hash has no order to take from."}, hoarddb.ErrInvalidArgument, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
				c.send(request, hoarddbwire.OpQueueClaim, tc.answer)
			})
			client := mustConnect(t, server.uri())
			claimed, err := client.Database("app").Bucket("q").Claim(testContext(t))
			if tc.wantErr == nil {
				if err != nil || len(claimed.Items) != 0 || claimed.Returned != tc.returned {
					t.Fatalf("Claim = %+v, %v; want no items, no error and Returned %d", claimed, err, tc.returned)
				}
				return
			}
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Claim = %+v, %v; want %v", claimed, err, tc.wantErr)
			}
			if errors.Is(tc.wantErr, hoarddb.ErrInvalidArgument) && !strings.Contains(err.Error(), "claim is not defined for hash buckets") {
				t.Errorf("the server's sentence was lost: %v", err)
			}
		})
	}
}

func TestClaimOfAnOversizedEntryIsErrClaimEntryTooLarge(t *testing.T) {
	server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		c.errorWith(request, map[string]any{"code": int32(4), "write_outcome": "not_applied",
			"message": "claim refused: the entry at the front of app.q needs 5 MiB to claim. Nothing was taken. Take it with db.q.pop(), which removes it at most once"})
	})
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("q").Claim(testContext(t))
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrClaimEntryTooLarge) || !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) ||
		failure.Code != hoarddb.CodeInvalidArgument || failure.Outcome != hoarddb.OutcomeNotApplied ||
		!strings.Contains(failure.Message, "Take it with db.q.pop()") {
		t.Fatalf("Claim = %v; want ErrClaimEntryTooLarge (and ErrInvalidArgument, code INVALID_ARGUMENT) with the server's sentence", err)
	}
	// Any other INVALID_ARGUMENT of a claim stays a plain one.
	other, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		c.errorWith(request, map[string]any{"code": int32(4), "write_outcome": "not_applied", "message": "invalid claim limit: 101 is above the claim maximum of 100."})
	})
	_, err = mustConnect(t, other.uri()).Database("app").Bucket("q").Claim(testContext(t))
	if errors.Is(err, hoarddb.ErrClaimEntryTooLarge) || !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Claim = %v; want a plain ErrInvalidArgument", err)
	}
}

func TestLeaseOptionsOutsideTheirRangeAreRefusedBeforeSending(t *testing.T) {
	server, recorder := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		c.errorFrame(request, 5, "nothing should arrive")
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	ctx := testContext(t)
	lease, _ := hoarddb.ParseLeaseID(aLeaseID)
	for name, call := range map[string]func() error{
		"claim limit 0":   func() error { _, err := bucket.Claim(ctx, hoarddb.WithLimit(0)); return err },
		"claim limit 101": func() error { _, err := bucket.Claim(ctx, hoarddb.WithLimit(101)); return err },
		"claim lease 999ms": func() error {
			_, err := bucket.Claim(ctx, hoarddb.WithLease(999*time.Millisecond))
			return err
		},
		"claim lease 2h": func() error { _, err := bucket.Claim(ctx, hoarddb.WithLease(2*time.Hour)); return err },
		"extend 0":       func() error { _, err := bucket.Extend(ctx, lease, 0); return err },
		"extend 2h":      func() error { _, err := bucket.Extend(ctx, lease, 2*time.Hour); return err },
		"ack no ids":     func() error { _, err := bucket.Ack(ctx, lease, hoarddb.WithItemIDs()); return err },
		"ack 101 ids": func() error {
			_, err := bucket.Ack(ctx, lease, hoarddb.WithItemIDs(make([]string, 101)...))
			return err
		},
		"claim priority": func() error { _, err := bucket.Claim(ctx, hoarddb.WithPriority(1)); return err },
		"consume limit":  func() error { return bucket.Consume(ctx, nil, hoarddb.WithLimit(101)) },
		"consume lease":  func() error { return bucket.Consume(ctx, nil, hoarddb.WithLease(time.Millisecond)) },
	} {
		if err := call(); !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "nothing was sent") {
			t.Errorf("%s = %v; want ErrInvalidArgument that says nothing was sent", name, err)
		}
	}
	if sent := len(recorder.requests()); sent != 0 {
		t.Fatalf("%d requests were sent; none should have been", sent)
	}
}

func TestAckReleaseAndExtendReadTheirAnswers(t *testing.T) {
	server, recorder := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		switch request.Op {
		case hoarddbwire.OpQueueAck:
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "acked", Acked: 1, Missing: 1, Remaining: 3})
		case hoarddbwire.OpQueueRelease:
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "released", Released: 4})
		case hoarddbwire.OpQueueExtend:
			c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "extended", LeaseMS: 120000})
		}
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	ctx := testContext(t)
	lease, _ := hoarddb.ParseLeaseID(aLeaseID)
	acked, err := bucket.Ack(ctx, lease, hoarddb.WithItemIDs("0000000000000000", "00000000000000ff"))
	if err != nil || acked != (hoarddb.AckResult{State: hoarddb.LeaseAcked, Acked: 1, Missing: 1, Remaining: 3}) {
		t.Fatalf("Ack = %+v, %v", acked, err)
	}
	released, err := bucket.Release(ctx, lease)
	if err != nil || released != (hoarddb.ReleaseResult{State: hoarddb.LeaseReleased, Released: 4}) {
		t.Fatalf("Release = %+v, %v", released, err)
	}
	granted, err := bucket.Extend(ctx, lease, 2*time.Minute)
	if err != nil || granted != 2*time.Minute {
		t.Fatalf("Extend = %s, %v; want the 2 minutes the server granted", granted, err)
	}
	sent := recorder.requests()
	if ids := fmt.Sprint(sent[0].members["item_ids"]); ids != "[0000000000000000 00000000000000ff]" || sent[0].members["lease_id"] != aLeaseID {
		t.Errorf("ack sent %v; want the lease and both ids", sent[0].members)
	}
	if _, present := sent[1].members["item_ids"]; present || sent[1].members["lease_id"] != aLeaseID {
		t.Errorf("release sent %v; want the lease alone", sent[1].members)
	}
	if sent[2].members["lease_ms"] == nil {
		t.Errorf("extend sent %v; want lease_ms", sent[2].members)
	}
}

// An unknown or expired lease is an in-band success:false; the typed error and
// the result both come back, and the result's counts stay readable.
func TestAckOfAnUnknownOrExpiredLeaseIsATypedErrorBesideTheResult(t *testing.T) {
	for _, tc := range []struct {
		state string
		want  *hoarddb.Error
	}{{"unknown", hoarddb.ErrLeaseUnknown}, {"expired", hoarddb.ErrLeaseExpired}} {
		t.Run(tc.state, func(t *testing.T) {
			const sentence = "the lease is gone. Nothing was changed. Claim again with db.q.claim()"
			server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
				c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: false, LeaseState: tc.state, Message: sentence})
			})
			bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
			lease, _ := hoarddb.ParseLeaseID(aLeaseID)
			for name, call := range map[string]func() (hoarddb.LeaseState, error){
				"ack": func() (hoarddb.LeaseState, error) {
					result, err := bucket.Ack(testContext(t), lease)
					return result.State, err
				},
				"release": func() (hoarddb.LeaseState, error) {
					result, err := bucket.Release(testContext(t), lease)
					return result.State, err
				},
			} {
				state, err := call()
				var failure *hoarddb.Error
				if !errors.Is(err, tc.want) || !errors.As(err, &failure) || failure.Message != sentence || string(state) != tc.state {
					t.Errorf("%s = state %q, %v; want %v with the server's sentence and the state beside it", name, state, err, tc.want.Code)
				}
				if errors.Is(err, hoarddb.ErrInvalidArgument) {
					t.Errorf("%s: a lease that is gone is not an invalid argument", name)
				}
			}
			if _, err := bucket.Extend(testContext(t), lease, time.Minute); !errors.Is(err, tc.want) {
				t.Errorf("Extend = %v; want %v", err, tc.want.Code)
			}
		})
	}
}
