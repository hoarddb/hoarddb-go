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

package ring

import (
	"sync/atomic"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestDropBucketForgetsTheCachedHeader: after a drop the next call on the same Bucket describes
// the bucket again instead of sending the header of the bucket that went away. Without the
// forget the cached header survives until a stale-header refusal, which NoResend turns off.
func TestDropBucketForgetsTheCachedHeader(t *testing.T) {
	var describes atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingKeyed {
			payload, err := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
			if err != nil {
				t.Error(err)
				return
			}
			conn.reply(frame, payload)
			return
		}
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		if request.Cmd == ringwire.AdminDropBucket {
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 77, State: "dropping"})
			return
		}
		describes.Add(1)
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 77, State: "active",
			DeclVersion: 1, Fields: userFields()})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users := client.Bucket("db", "User")
	if _, err := users.Info(testContext(t)); err != nil { // caches the header
		t.Fatal(err)
	}
	if err := client.DropBucket(testContext(t), "db", "User"); err != nil {
		t.Fatal(err)
	}
	if _, err := users.Insert(testContext(t), "ada", Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if got := describes.Load(); got != 2 {
		t.Fatalf("%d describe(s) went out, want 2: the drop must forget the cached header, so the next call describes again", got)
	}
}
