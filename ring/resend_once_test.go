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
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// resendContext bounds a call whose server keeps answering with the same refusal, so a client
// that resends without a bound returns a deadline error instead of running until the test times
// out. A bounded client answers well inside it.
func resendContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestASchemaNotCurrentRefusalIsResentOnlyOnce: a server that answers SCHEMA_NOT_CURRENT every
// time is asked twice, not forever: one send and the one resend the stale-header rule allows.
// The second refusal is returned, because a fresh header no longer explains it.
func TestASchemaNotCurrentRefusalIsResentOnlyOnce(t *testing.T) {
	var decl, schema, keyed atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			describe(conn, frame)
			return
		}
		keyed.Add(1)
		decl.Add(1) // every describe names a declaration the next request has not seen
		conn.refuse(frame, ringwire.CodeSchemaNotCurrent, "the bucket's schema moved")
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Bucket("db", "User").Insert(resendContext(t), "ada", Doc{"Name": "Ada"})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != ringwire.CodeSchemaNotCurrent {
		t.Fatalf("err = %v, want the second SCHEMA_NOT_CURRENT refusal", err)
	}
	if got := keyed.Load(); got != 2 {
		t.Fatalf("%d keyed frame(s) went out, want 2: one send and one resend", got)
	}
}

// TestAnUnknownBucketIDRefusalIsRedescribedOnlyOnce: a server that describes a new bucket id and
// then answers the unknown-id refusal again is asked twice, not forever: the describe happens
// once, because the second refusal cannot be fixed by describing again.
func TestAnUnknownBucketIDRefusalIsRedescribedOnlyOnce(t *testing.T) {
	var head atomic.Uint64
	head.Store(77)
	var describes, keyed atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			request, err := ringwire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			describes.Add(1)
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: head.Load(), State: "active",
				DeclVersion: 1, Fields: userFields()})
			return
		}
		keyed.Add(1)
		head.Add(1) // the describe after a refusal always names another id
		conn.refuse(frame, ringwire.CodePermissionDenied,
			"this session cannot use that bucket: no bucket has the id the request carried on this node, or the user holds no role")
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users := client.Bucket("db", "User")
	if _, err := users.Info(testContext(t)); err != nil { // caches id 77
		t.Fatal(err)
	}
	_, err = users.Insert(resendContext(t), "ada", Doc{"Name": "Ada"})
	var refusal *Error
	if !errors.As(err, &refusal) || !strings.Contains(refusal.Message, unknownBucketID) {
		t.Fatalf("err = %v, want the second unknown-bucket-id refusal", err)
	}
	if got := keyed.Load(); got != 2 {
		t.Fatalf("%d keyed frame(s) went out, want 2: one send and one resend", got)
	}
	if got := describes.Load(); got != 2 {
		t.Fatalf("%d describe(s) went out, want 2: the first use and the one re-describe", got)
	}
}

// TestABatchIsResentOnlyOnce: a batch whose answer keeps saying SCHEMA_NOT_CURRENT is sent twice,
// not forever. The answers of the second send are returned as they are.
func TestABatchIsResentOnlyOnce(t *testing.T) {
	var decl, schema, frames atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingBatch {
			describe(conn, frame)
			return
		}
		frames.Add(1)
		decl.Add(1)
		applied := ringwire.RingReply{WriteOutcome: "applied"}
		moved := ringwire.RingError{Code: ringwire.CodeSchemaNotCurrent, Message: "the bucket's schema moved"}
		payload, err := ringwire.EncodeRingReply(ringwire.RingReply{Outcome: "partial",
			Answers: []ringwire.RingAnswer{{Reply: &applied}, {Error: &moved}}})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	batch := client.NewBatch()
	bucket := client.Bucket("db", "User").In(batch)
	for _, id := range []string{"ada", "bob"} {
		if _, err := bucket.Insert(testContext(t), id, Doc{"Name": id}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := batch.Send(resendContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "partial" || len(result.Answers) != 2 {
		t.Fatalf("result %+v, want the second send's answers", result)
	}
	if got := frames.Load(); got != 2 {
		t.Fatalf("%d batch frame(s) went out, want 2: one send and one resend", got)
	}
}
