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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// describeHandler answers describe_bucket with the given versions and refuses nothing else.
func describeHandler(t *testing.T, decl, seen *atomic.Int64) func(*fakeConn, hoarddbwire.Frame) {
	return func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingAdmin {
			return
		}
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 77, State: "active",
			DeclVersion: decl.Load(), SchemaVersionSeen: seen.Load(), Fields: userFields()})
	}
}

func TestDialRefusesAConnectionThatTrustsNothing(t *testing.T) {
	_, err := Dial(testContext(t), Options{Addr: "127.0.0.1:1", User: "root", Password: "pw"})
	if err == nil {
		t.Fatal("Dial connected with neither a fingerprint nor Insecure")
	}
	if !strings.Contains(err.Error(), "Fingerprint") || !strings.Contains(err.Error(), "Insecure") {
		t.Fatalf("the refusal does not name the two options: %v", err)
	}
}

func TestDialPinsTheServerFingerprint(t *testing.T) {
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	good, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatalf("dialing with the right fingerprint: %v", err)
	}
	if err := good.Close(); err != nil {
		t.Fatal(err)
	}
	wrong := server.options()
	wrong.Fingerprint = strings.Repeat("00", 32)
	if bad, err := Dial(testContext(t), wrong); err == nil {
		_ = bad.Close()
		t.Fatal("Dial accepted a server whose fingerprint does not match the pin")
	}
}

func TestDialInsecureConnects(t *testing.T) {
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	client, err := Dial(testContext(t), Options{Addr: server.addr(), User: "root", Password: "pw", Insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectedLoginNamesTheUser(t *testing.T) {
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	server.authFails = true
	_, err := Dial(testContext(t), server.options())
	if err == nil || !strings.Contains(err.Error(), `"root"`) || !strings.Contains(err.Error(), "password") {
		t.Fatalf("a rejected login must name the user and the password, got %v", err)
	}
}

// TestPipelinedRepliesAreMatchedByRequestID: the server answers the requests in the reverse
// of the order it read them, and each caller still gets its own bucket.
func TestPipelinedRepliesAreMatchedByRequestID(t *testing.T) {
	const callers = 8
	var held []hoarddbwire.Frame
	var lock sync.Mutex
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		lock.Lock()
		held = append(held, frame)
		ready := len(held) == callers
		batch := append([]hoarddbwire.Frame(nil), held...)
		lock.Unlock()
		if !ready {
			return
		}
		for i := len(batch) - 1; i >= 0; i-- {
			request, err := ringwire.DecodeRingAdmin(batch[i].Payload)
			if err != nil {
				t.Error(err)
				return
			}
			// The bucket id is the length of the name, so each reply is checkable.
			adminReplyFrame(t, conn, batch[i], ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: uint64(len(request.Bucket)), State: "active", Fields: userFields()})
		}
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	var group sync.WaitGroup
	for i := 1; i <= callers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			info, err := client.DescribeBucket(testContext(t), "db", strings.Repeat("b", i))
			if err != nil {
				t.Error(err)
				return
			}
			if info.ID != uint64(i) {
				t.Errorf("the caller that asked for a %d-letter bucket got bucket id %d: replies were not matched by request id", i, info.ID)
			}
		}()
	}
	group.Wait()
}

func TestEveryRequestGetsItsOwnID(t *testing.T) {
	var ids sync.Map
	var duplicates atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if _, loaded := ids.LoadOrStore(frame.ID, true); loaded {
			duplicates.Add(1)
		}
		request, _ := ringwire.DecodeRingAdmin(frame.Payload)
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 1, State: "active", Fields: userFields()})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	for range 50 {
		if _, err := client.DescribeBucket(testContext(t), "db", "User"); err != nil {
			t.Fatal(err)
		}
	}
	if duplicates.Load() != 0 {
		t.Fatalf("%d requests reused an id", duplicates.Load())
	}
}

func TestWithRequestIDSendsThatID(t *testing.T) {
	var seen atomic.Value
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingKeyed {
			seen.Store([16]byte(frame.ID))
			reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
			conn.reply(frame, reply)
			return
		}
		describe(conn, frame)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	want := [16]byte{9, 8, 7}
	if _, err := client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"}, WithRequestID(want)); err != nil {
		t.Fatal(err)
	}
	if got, _ := seen.Load().([16]byte); got != want {
		t.Fatalf("the insert went out under id %x, want %x", got, want)
	}
}

// TestSchemaNotCurrentRefreshesTheHeaderAndRetriesUnderTheSameID: the first insert is refused,
// the client describes the bucket again and sends the insert once more with the new versions
// and the old request id.
func TestSchemaNotCurrentRefreshesTheHeaderAndRetriesUnderTheSameID(t *testing.T) {
	var decl, schema atomic.Int64
	decl.Store(1)
	describe := describeHandler(t, &decl, &schema)
	var keyedIDs [][16]byte
	var keyedDecl []int64
	var lock sync.Mutex
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			describe(conn, frame)
			return
		}
		request, err := ringwire.DecodeRingKeyed(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		lock.Lock()
		keyedIDs, keyedDecl = append(keyedIDs, frame.ID), append(keyedDecl, request.Op.DeclVersion)
		first := len(keyedIDs) == 1
		lock.Unlock()
		if first {
			decl.Store(2) // the declaration moved on
			conn.refuse(frame, ringwire.CodeSchemaNotCurrent, "the bucket's schema moved")
			return
		}
		reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	result, err := client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied() {
		t.Fatalf("result %+v", result)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(keyedIDs) != 2 || keyedIDs[0] != keyedIDs[1] {
		t.Fatalf("the retry must reuse the request id: %x", keyedIDs)
	}
	if keyedDecl[0] != 1 || keyedDecl[1] != 2 {
		t.Fatalf("declaration versions sent %v, want [1 2]", keyedDecl)
	}
}

func TestARefusalIsATypedError(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingKeyed {
			conn.refuse(frame, ringwire.CodePermissionDenied, "run 'grant role writer@db to root'")
			return
		}
		describe(conn, frame)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != ringwire.CodePermissionDenied || !strings.Contains(refusal.Message, "grant role") {
		t.Fatalf("want a *Error with the code and the message, got %T %v", err, err)
	}
}

func TestAnUndeclaredFieldIsRefusedBeforeAnythingIsSent(t *testing.T) {
	var decl, schema atomic.Int64
	var keyed atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingKeyed {
			keyed.Add(1)
			return
		}
		describe(conn, frame)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users := client.Bucket("db", "User")
	_, err = users.Insert(testContext(t), "ada", Doc{"Nmae": "Ada"})
	if err == nil || !strings.Contains(err.Error(), "Nmae") || !strings.Contains(err.Error(), "Name") {
		t.Fatalf("the refusal must name the typo and the declared fields: %v", err)
	}
	_, err = users.Insert(testContext(t), "ada", Doc{"Purchases": "x"})
	if err == nil || !strings.Contains(err.Error(), "store") {
		t.Fatalf("a store field in a Doc must say it is a store: %v", err)
	}
	_, err = users.Push(testContext(t), "ada", "Name", bson.D{{Key: "a", Value: 1}})
	if err == nil || !strings.Contains(err.Error(), "not a store") {
		t.Fatalf("a push to a scalar must say it is not a store: %v", err)
	}
	if keyed.Load() != 0 {
		t.Fatalf("%d keyed requests reached the server", keyed.Load())
	}
}

func TestInsertSendsTheDeclaredFieldIDsAndTypedValues(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	var got ringwire.RingKeyedRequest
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			describe(conn, frame)
			return
		}
		got, _ = ringwire.DecodeRingKeyed(frame.Payload)
		reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if got.Op.Class != ringwire.OpInsert || got.Op.BucketID != 77 || len(got.Op.Fields) != 1 || got.Op.Fields[0].FieldID != 2 {
		t.Fatalf("insert sent %+v", got.Op)
	}
	if want := "\x02\x04\x00\x00\x00Ada\x00"; string(got.Op.Fields[0].Value) != want {
		t.Fatalf("the Name value is %q, want %q", got.Op.Fields[0].Value, want)
	}
	if want := "\x02\x04\x00\x00\x00ada\x00"; string(got.Op.IDValue) != want {
		t.Fatalf("the id value is %q, want %q", got.Op.IDValue, want)
	}
}

// TestCloseUnblocksAPendingRequestAndStopsTheReader: Close returns while a request waits for
// an answer that never comes, the waiter gets an error and later calls fail fast.
func TestCloseUnblocksAPendingRequestAndStopsTheReader(t *testing.T) {
	arrived := make(chan struct{}, 1)
	server := startFakeServer(t, func(_ *fakeConn, _ hoarddbwire.Frame) { arrived <- struct{}{} })
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	waiting := make(chan error, 1)
	go func() {
		_, err := client.DescribeBucket(context.Background(), "db", "User")
		waiting <- err
	}()
	<-arrived
	closed := make(chan error, 1)
	go func() { closed <- client.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return: the reader goroutine did not exit")
	}
	select {
	case err := <-waiting:
		if err == nil {
			t.Fatal("a request pending at Close succeeded")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a request pending at Close was never released")
	}
	if _, err := client.DescribeBucket(testContext(t), "db", "User"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("a call after Close must say the client is closed: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("a second Close must be harmless: %v", err)
	}
}

func TestAContextDeadlineEndsTheWaitAndSaysTheOutcomeIsUnknown(t *testing.T) {
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = client.DescribeBucket(ctx, "db", "User")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "request id") {
		t.Fatalf("want a deadline error that names the request id for the retry, got %v", err)
	}
}

// TestARequestIDAlreadyWaitingIsRefusedBeforeAnythingIsSent: two calls under one request id
// would share one answer slot, so the second is refused and never reaches the server.
func TestARequestIDAlreadyWaitingIsRefusedBeforeAnythingIsSent(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	arrived := make(chan struct{}, 4)
	release := make(chan struct{})
	var keyed atomic.Int64
	var releaseOnce sync.Once
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			describe(conn, frame)
			return
		}
		keyed.Add(1)
		arrived <- struct{}{}
		<-release
		reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) }) // runs before the server's own cleanup
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	id := client.NextRequestID()
	first := make(chan error, 1)
	go func() {
		_, err := client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"}, WithRequestID(id))
		first <- err
	}()
	<-arrived
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelSecond()
	_, err = client.Bucket("db", "User").Insert(secondCtx, "bob", Doc{"Name": "Bob"}, WithRequestID(id))
	if err == nil || !strings.Contains(err.Error(), "already in flight") {
		t.Fatalf("the second call under a waiting id returned %v, want the already-in-flight refusal", err)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatalf("the first call: %v", err)
	}
	if got := keyed.Load(); got != 1 {
		t.Fatalf("%d keyed frames reached the server, want 1", got)
	}
}

// TestADroppedAndRecreatedBucketIsRedescribedOnce: the cached header names the old bucket id,
// the server answers the unknown-id refusal, and the client describes the bucket again and
// retries once under the same request id with the new id.
func TestADroppedAndRecreatedBucketIsRedescribedOnce(t *testing.T) {
	var bucketID atomic.Uint64
	bucketID.Store(77)
	var describes atomic.Int64
	var lock sync.Mutex
	var keyedIDs [][16]byte
	var sentBucket []uint64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingAdmin {
			request, err := ringwire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			describes.Add(1)
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: bucketID.Load(), State: "active", DeclVersion: 1, Fields: userFields()})
			return
		}
		request, err := ringwire.DecodeRingKeyed(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		lock.Lock()
		keyedIDs, sentBucket = append(keyedIDs, frame.ID), append(sentBucket, request.Op.BucketID)
		lock.Unlock()
		if request.Op.BucketID != 99 {
			conn.refuse(frame, ringwire.CodePermissionDenied, "this session cannot use that bucket: no bucket has the id the request carried on this node, or the user holds no role")
			return
		}
		reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
		conn.reply(frame, reply)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.Bucket("db", "User").Info(testContext(t)); err != nil { // caches id 77
		t.Fatal(err)
	}
	bucketID.Store(99) // dropped and created again under the same name
	result, err := client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied() {
		t.Fatalf("result %+v", result)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(keyedIDs) != 2 || keyedIDs[0] != keyedIDs[1] || sentBucket[0] != 77 || sentBucket[1] != 99 {
		t.Fatalf("keyed ids %x bucket ids %v, want one request id twice and bucket ids [77 99]", keyedIDs, sentBucket)
	}
	if describes.Load() != 2 {
		t.Fatalf("%d describes, want 2 (first use, then the one refresh)", describes.Load())
	}
}

// TestAnUnchangedHeaderIsNotRetriedAfterAnUnknownIDRefusal: when the describe returns the same
// header the refusal is real (no grant), and it is returned after exactly one refresh.
func TestAnUnchangedHeaderIsNotRetriedAfterAnUnknownIDRefusal(t *testing.T) {
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	var keyed atomic.Int64
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingKeyed {
			describe(conn, frame)
			return
		}
		keyed.Add(1)
		conn.refuse(frame, ringwire.CodePermissionDenied, "this session cannot use that bucket: no bucket has the id the request carried on this node, or the user holds no role")
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Bucket("db", "User").Insert(testContext(t), "ada", Doc{"Name": "Ada"})
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != ringwire.CodePermissionDenied {
		t.Fatalf("err = %v, want the PERMISSION_DENIED refusal", err)
	}
	if keyed.Load() != 1 {
		t.Fatalf("%d keyed frames, want 1: an unchanged header must not be retried", keyed.Load())
	}
}
