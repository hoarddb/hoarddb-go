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
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// Real-server tests of the dead list. Each starts its own server and needs
// HOARDDB_SERVER_BIN.

// An entry delivered 5 times without an ack is dead: DeadList shows it, Requeue
// brings it back, Discard drops what is left.
func TestDeadListRequeueAndDiscardAgainstTheServer(t *testing.T) {
	bucket := bucketOf(t, connectTo(t, startServer(t)), hoarddb.FIFO)
	ctx := testContext(t)
	pushNumbers(t, bucket, 2)
	for delivery := 1; delivery <= 5; delivery++ {
		claimed, err := bucket.Claim(ctx, hoarddb.WithLimit(2))
		if err != nil || len(claimed.Items) != 2 {
			t.Fatalf("Claim %d = %+v, %v", delivery, claimed, err)
		}
		if _, err := bucket.Release(ctx, claimed.LeaseID); err != nil {
			t.Fatalf("Release %d: %v", delivery, err)
		}
	}
	if left, err := bucket.Claim(ctx); err != nil || len(left.Items) != 0 {
		t.Fatalf("Claim after five deliveries = %+v, %v; want both entries dead and the queue empty", left, err)
	}
	page, err := bucket.DeadList(ctx, hoarddb.WithLimit(1))
	if err != nil || len(page.Items) != 1 || page.Next == "" {
		t.Fatalf("DeadList(limit 1) = %+v, %v; want one entry and a next", page, err)
	}
	dead := page.Items[0]
	if dead.Reason != "max_deliveries" || dead.Deliveries != 5 || dead.DeadAt.IsZero() || dead.DeadID == "" || intField(t, dead.Document, "n") != 1 {
		t.Fatalf("dead entry = %+v", dead)
	}
	second, err := bucket.DeadList(ctx, hoarddb.After(page.Next))
	if err != nil || len(second.Items) != 1 || second.Next != "" || intField(t, second.Items[0].Document, "n") != 2 {
		t.Fatalf("DeadList(After) = %+v, %v; want the second entry and no next", second, err)
	}
	requeued, err := bucket.Requeue(ctx, hoarddb.DeadByID(dead.DeadID))
	if err != nil || requeued.Done != 1 {
		t.Fatalf("Requeue = %+v, %v", requeued, err)
	}
	back, err := bucket.Peek(ctx)
	if err != nil || intField(t, back[0], "n") != 1 {
		t.Fatalf("Peek after the requeue = %v, %v; want entry 1 back in the queue", back, err)
	}
	discarded, err := bucket.Discard(ctx, hoarddb.AllDead())
	if err != nil || discarded.Done != 1 {
		t.Fatalf("Discard(AllDead) = %+v, %v; want the one dead entry dropped", discarded, err)
	}
	if page, err := bucket.DeadList(ctx); err != nil || len(page.Items) != 0 {
		t.Fatalf("DeadList after the discard = %+v, %v; want empty", page, err)
	}
	if _, err := bucket.Requeue(ctx, hoarddb.DeadByID("0000000000000000")); !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("Requeue of nothing = %v; want the in-band refusal as an error", err)
	}
}

func TestDeadListOfAHashIsARefusal(t *testing.T) {
	_, err := bucketOf(t, connectTo(t, startServer(t)), hoarddb.Hash).DeadList(testContext(t))
	if err == nil {
		t.Fatal("DeadList of a hash bucket answered without an error")
	}
}
