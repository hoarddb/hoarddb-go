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
	"strings"
	"sync"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Unit tests of the dead list against the in-process fake server (see
// lease_test.go for the helpers). The real-server proofs are in
// dead_integration_test.go and the conformance legs.

func TestDeadListReadsEntriesAndPages(t *testing.T) {
	died := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	ada := adaDocument(t)
	var sent map[string]any
	server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, members map[string]any) {
		sent = members
		c.send(request, request.Op, hoarddbwire.DeadListResponse{Found: true, Next: "0000000000000002", Items: []hoarddbwire.DeadEntry{
			{DeadID: "0000000000000001", Data: ada, Deliveries: 5, DeadAt: died, Reason: "max_deliveries"},
			{DeadID: "0000000000000002", Data: ada, Deliveries: 3, DeadAt: died, Reason: "collided"}}})
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	page, err := bucket.DeadList(testContext(t), hoarddb.WithLimit(2), hoarddb.After("0000000000000000"))
	if err != nil || len(page.Items) != 2 || page.Next != "0000000000000002" {
		t.Fatalf("DeadList = %+v, %v; want 2 entries and a next", page, err)
	}
	first := page.Items[0]
	if first.DeadID != "0000000000000001" || first.Deliveries != 5 || !first.DeadAt.Equal(died) || first.Reason != "max_deliveries" || len(first.Document) == 0 {
		t.Errorf("first entry = %+v", first)
	}
	if sent["after"] != "0000000000000000" || sent["limit"] == nil {
		t.Errorf("dead list sent %v; want limit and after", sent)
	}
}

func TestDeadListEmptyIsAnEmptyPageAndARefusalIsAnError(t *testing.T) {
	for _, tc := range []struct {
		name    string
		answer  hoarddbwire.DeadListResponse
		wantErr bool
	}{{"none", hoarddbwire.DeadListResponse{}, false}, {"a message means a refusal", hoarddbwire.DeadListResponse{Message: "dead is not defined for hash buckets"}, true}} {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
				c.send(request, request.Op, tc.answer)
			})
			page, err := mustConnect(t, server.uri()).Database("app").Bucket("q").DeadList(testContext(t))
			if tc.wantErr {
				if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), tc.answer.Message) {
					t.Fatalf("DeadList = %+v, %v; want the refusal verbatim", page, err)
				}
				return
			}
			if err != nil || page == nil || len(page.Items) != 0 || page.Next != "" {
				t.Fatalf("DeadList = %+v, %v; want an empty page and no error", page, err)
			}
		})
	}
}

func TestRequeueAndDiscardSendTheSelectorAndReadRefusals(t *testing.T) {
	var (
		mu   sync.Mutex
		sent []map[string]any
	)
	server, _ := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, members map[string]any) {
		mu.Lock()
		sent = append(sent, members)
		mu.Unlock()
		if members["dead_id"] == "0000000000000009" {
			c.send(request, request.Op, hoarddbwire.DeadMoveResponse{Success: false, Done: 0,
				Refused: []hoarddbwire.DeadRefusal{{DeadID: "0000000000000009", Message: "a live element holds this key"}}})
			return
		}
		c.send(request, request.Op, hoarddbwire.DeadMoveResponse{Success: true, Done: 100, Left: 7, Message: "7 dead entries are left"})
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	ctx := testContext(t)
	moved, err := bucket.Requeue(ctx, hoarddb.AllDead().After("0000000000000003"))
	if err != nil || moved.Done != 100 || moved.Left != 7 || len(moved.Refused) != 0 {
		t.Fatalf("Requeue(AllDead) = %+v, %v", moved, err)
	}
	if _, err := bucket.Discard(ctx, hoarddb.DeadByID("0000000000000001")); err != nil {
		t.Fatalf("Discard: %v", err)
	}
	refused, err := bucket.Requeue(ctx, hoarddb.DeadByID("0000000000000009"))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "a live element holds this key") ||
		len(refused.Refused) != 1 || refused.Refused[0].DeadID != "0000000000000009" {
		t.Fatalf("Requeue of a refused entry = %+v, %v; want the refusal as an error and in the result", refused, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if sent[0]["all"] != true || sent[0]["after"] != "0000000000000003" || sent[0]["dead_id"] != nil {
		t.Errorf("requeue all sent %v", sent[0])
	}
	if sent[1]["dead_id"] != "0000000000000001" || sent[1]["all"] != nil {
		t.Errorf("discard one sent %v", sent[1])
	}
}

// DeadList is a read: a connection lost before the answer is followed by one
// resend on a fresh connection, where a claim or a requeue would never be.
func TestDeadListIsRetriedOnceAfterAConnectionLoss(t *testing.T) {
	var log frameLog
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if log.add(request.Op) == 1 {
			return false // close without answering
		}
		c.send(request, request.Op, hoarddbwire.DeadListResponse{})
		return true
	}))
	page, err := mustConnect(t, server.uri()).Database("app").Bucket("q").DeadList(testContext(t))
	if err != nil || page == nil || len(page.Items) != 0 {
		t.Fatalf("DeadList = %+v, %v; want the resend's empty page", page, err)
	}
	if sent := log.count(hoarddbwire.OpDeadList); sent != 2 {
		t.Fatalf("DEAD_LIST sent %d times, want 2", sent)
	}
}

func TestDeadListLimitOutsideOneToAThousandIsRefusedBeforeSending(t *testing.T) {
	server, recorder := leaseFake(t, func(c *fakeConn, request hoarddbwire.Frame, _ map[string]any) {
		c.errorFrame(request, 5, "nothing should arrive")
	})
	bucket := mustConnect(t, server.uri()).Database("app").Bucket("q")
	for _, limit := range []int{0, -3, 1001} {
		if _, err := bucket.DeadList(testContext(t), hoarddb.WithLimit(limit)); !errors.Is(err, hoarddb.ErrInvalidArgument) ||
			!strings.Contains(err.Error(), "1 to 1000") || !strings.Contains(err.Error(), "nothing was sent") {
			t.Errorf("DeadList(WithLimit(%d)) = %v; want ErrInvalidArgument naming 1 to 1000 and that nothing was sent", limit, err)
		}
	}
	if sent := len(recorder.requests()); sent != 0 {
		t.Fatalf("%d requests were sent; none should have been", sent)
	}
}
