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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

// dialNoResend connects to server with Options.NoResend set.
func dialNoResend(t *testing.T, server *fakeServer) *Client {
	t.Helper()
	options := server.options()
	options.NoResend = true
	client, err := Dial(testContext(t), options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestNoResendSendsOneFrameOnSchemaNotCurrent: with NoResend a keyed request refused because
// the cached header is stale is sent once: the refusal comes back as it is, with no describe
// and no second send. The header is forgotten, so the next call describes the bucket again
// and is applied. The same holds for the refusal of a bucket id that was dropped and created
// again.
func TestNoResendSendsOneFrameOnSchemaNotCurrent(t *testing.T) {
	cases := []struct {
		name    string
		code    ringwire.Code
		message string
	}{
		{"the schema moved", ringwire.CodeSchemaNotCurrent, "the bucket's schema moved"},
		{"the bucket was dropped and created again", ringwire.CodeNotFound, unknownBucketID + " 77"},
	}
	for _, refused := range cases {
		t.Run(refused.name, func(t *testing.T) {
			var decl, schema, describes atomic.Int64
			decl.Store(1)
			describe := describeHandler(t, &decl, &schema)
			var keyedDecl []int64
			var lock sync.Mutex
			server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
				if frame.Op != hoarddbwire.OpRingKeyed {
					describes.Add(1)
					describe(conn, frame)
					return
				}
				request, err := ringwire.DecodeRingKeyed(frame.Payload)
				if err != nil {
					t.Error(err)
					return
				}
				lock.Lock()
				keyedDecl = append(keyedDecl, request.Op.DeclVersion)
				first := len(keyedDecl) == 1
				lock.Unlock()
				if first {
					decl.Store(2) // the declaration moved on
					conn.refuse(frame, refused.code, refused.message)
					return
				}
				reply, _ := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
				conn.reply(frame, reply)
			})
			client := dialNoResend(t, server)
			users := client.Bucket("db", "User")

			_, err := users.Insert(testContext(t), "ada", Doc{"Name": "Ada"})
			var refusal *Error
			if !errors.As(err, &refusal) || refusal.Code != refused.code {
				t.Fatalf("the insert returned %v, want the %s refusal as the server sent it", err, refused.code)
			}
			lock.Lock()
			sent := len(keyedDecl)
			lock.Unlock()
			if sent != 1 || describes.Load() != 1 {
				t.Fatalf("%d keyed frame(s) and %d describe(s) went out, want one keyed frame after the one describe that learned the header", sent, describes.Load())
			}

			result, err := users.Insert(testContext(t), "ada", Doc{"Name": "Ada"})
			if err != nil || !result.Applied() {
				t.Fatalf("the next insert: %+v, %v; want it applied under a freshly described header", result, err)
			}
			lock.Lock()
			defer lock.Unlock()
			if len(keyedDecl) != 2 || keyedDecl[1] != 2 || describes.Load() != 2 {
				t.Fatalf("declaration versions sent %v after %d describes, want [1 2] after 2", keyedDecl, describes.Load())
			}
		})
	}
}

// TestNoResendBatchIsSentOnce: a batch with one operation refused SCHEMA_NOT_CURRENT comes back
// with that refusal in its place, after one frame. A second Send describes the bucket again.
func TestNoResendBatchIsSentOnce(t *testing.T) {
	var decl, schema, describes atomic.Int64
	decl.Store(1)
	describe := describeHandler(t, &decl, &schema)
	var declSeen [][]int64
	var lock sync.Mutex
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op != hoarddbwire.OpRingBatch {
			describes.Add(1)
			describe(conn, frame)
			return
		}
		request, err := ringwire.DecodeRingBatch(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		versions := make([]int64, len(request.Ops))
		for i, op := range request.Ops {
			versions[i] = op.DeclVersion
		}
		lock.Lock()
		declSeen = append(declSeen, versions)
		first := len(declSeen) == 1
		lock.Unlock()
		applied := ringwire.RingReply{WriteOutcome: "applied"}
		answers := []ringwire.RingAnswer{{Reply: &applied}, {Reply: &applied}}
		outcome := "ok"
		if first {
			decl.Store(2) // the declaration moved on
			moved := ringwire.RingError{Code: ringwire.CodeSchemaNotCurrent, Message: "the bucket's schema moved"}
			answers[1] = ringwire.RingAnswer{Error: &moved}
			outcome = "partial"
		}
		payload, err := ringwire.EncodeRingReply(ringwire.RingReply{Outcome: outcome, Answers: answers})
		if err != nil {
			t.Error(err)
			return
		}
		conn.reply(frame, payload)
	})
	client := dialNoResend(t, server)
	batch := client.NewBatch()
	bucket := client.Bucket("db", "User").In(batch)
	for _, id := range []string{"ada", "bob"} {
		if _, err := bucket.Insert(testContext(t), id, Doc{"Name": id}); err != nil {
			t.Fatal(err)
		}
	}
	result, err := batch.Send(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	var refusal *Error
	if result.Outcome != "partial" || len(result.Answers) != 2 || !result.Answers[0].Write.Applied() ||
		!errors.As(result.Answers[1].Err, &refusal) || refusal.Code != ringwire.CodeSchemaNotCurrent {
		t.Fatalf("result %+v, want the first answer applied and the second refused SCHEMA_NOT_CURRENT", result)
	}
	lock.Lock()
	sent := len(declSeen)
	lock.Unlock()
	if sent != 1 || describes.Load() != 1 {
		t.Fatalf("%d batch frame(s) and %d describe(s) went out, want one of each", sent, describes.Load())
	}
	if result, err = batch.Send(testContext(t)); err != nil || result.Outcome != "ok" {
		t.Fatalf("the second Send: %+v, %v", result, err)
	}
	lock.Lock()
	defer lock.Unlock()
	if len(declSeen) != 2 || declSeen[1][0] != 2 || declSeen[1][1] != 2 || describes.Load() != 2 {
		t.Fatalf("declaration versions sent %v after %d describes, want [[1 1] [2 2]] after 2", declSeen, describes.Load())
	}
}

// TestNoResendGetManyIsSentOnce: the keys of a bucket whose schema moved keep their refusal
// after one frame; the other bucket's keys are answered. The next GetMany describes the moved
// bucket again and reads every key.
func TestNoResendGetManyIsSentOnce(t *testing.T) {
	fake, server := newGetManyFake(t, "User", "Post")
	fake.answer = func(_ int, read ringwire.RingRead, _ ringwire.RingPrologue) ringwire.RingAnswer {
		if fake.bucketName(read.BucketID) == "User" && read.DeclVersion < 2 {
			return ringwire.RingAnswer{Error: &ringwire.RingError{Code: ringwire.CodeSchemaNotCurrent, Message: "the schema moved"}}
		}
		return fake.found(read)
	}
	client := dialNoResend(t, server)
	users, posts := client.Bucket("app", "User"), client.Bucket("app", "Post")
	if _, err := users.Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := posts.Info(testContext(t)); err != nil {
		t.Fatal(err)
	}
	fake.decl.Store(2)
	keys := []Key{{Bucket: users, ID: "a"}, {Bucket: posts, ID: "p"}, {Bucket: users, ID: "b"}}
	answers, err := client.GetMany(testContext(t), keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{0, 2} {
		var refusal *Error
		if !errors.As(answers[index].Err, &refusal) || refusal.Code != ringwire.CodeSchemaNotCurrent {
			t.Fatalf("answer %d is %+v, want the SCHEMA_NOT_CURRENT refusal", index, answers[index])
		}
	}
	if docID(t, answers[1]) != "p" {
		t.Fatalf("the Post key was not answered: %+v", answers[1])
	}
	describesOf := func(bucket string) int64 {
		count, _ := fake.describes.Load(bucket)
		return count.(*atomic.Int64).Load()
	}
	if fake.frames.Load() != 1 || describesOf("User") != 1 {
		t.Fatalf("%d get-many frame(s) and %d describe(s) of User, want one frame and the one describe that learned the header", fake.frames.Load(), describesOf("User"))
	}
	if answers, err = client.GetMany(testContext(t), keys); err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"a", "p", "b"} {
		if got := docID(t, answers[i]); got != want {
			t.Fatalf("the second GetMany answered key %d with %q, want %q", i, got, want)
		}
	}
	if fake.frames.Load() != 2 || describesOf("User") != 2 || describesOf("Post") != 1 {
		t.Fatalf("after the second GetMany: %d frames, User described %d times, Post %d; want 2, 2 and 1",
			fake.frames.Load(), describesOf("User"), describesOf("Post"))
	}
}

// presentedKey is the ed25519 public key the server at address presents, read from a
// handshake that checks nothing.
func presentedKey(t *testing.T, address string) ed25519.PublicKey {
	t.Helper()
	conn, err := tls.Dial("tcp", address, &tls.Config{MinVersion: tls.VersionTLS13, NextProtos: []string{trust.ALPN},
		InsecureSkipVerify: true}) //nolint:gosec // test-only: reads the fake server's own key
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	key, isEd25519 := conn.ConnectionState().PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !isEd25519 {
		t.Fatal("the fake server's key is not ed25519")
	}
	return key
}

// knownServersFile writes a known-servers file of one line, address and key, in a private
// directory and returns its path.
func knownServersFile(t *testing.T, address string, key ed25519.PublicKey) string {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "known_servers")
	if err := os.WriteFile(path, []byte(address+" "+hex.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestKnownServersPinsTheServer: with Options.KnownServers Dial connects to a server the file
// records under the key it presents, and to no other: a server recorded with another key and a
// server not recorded are both refused, Insecure does not loosen that, nothing is ever added to
// the file, and Fingerprint, when set, is what decides.
func TestKnownServersPinsTheServer(t *testing.T) {
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	serverKey := presentedKey(t, server.addr())
	otherKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	options := func(known string) Options {
		return Options{Addr: server.addr(), User: "root", Password: "pw", KnownServers: known}
	}

	t.Run("a recorded server is trusted", func(t *testing.T) {
		client, err := Dial(testContext(t), options(knownServersFile(t, server.addr(), serverKey)))
		if err != nil {
			t.Fatalf("Dial refused a server the known-servers file records with the key it presents: %v", err)
		}
		_ = client.Close()
	})
	t.Run("a server recorded with another key is refused", func(t *testing.T) {
		for _, insecure := range []bool{false, true} {
			withOther := options(knownServersFile(t, server.addr(), otherKey))
			withOther.Insecure = insecure
			client, err := Dial(testContext(t), withOther)
			var changed *trust.KeyChangedError
			if !errors.As(err, &changed) {
				if client != nil {
					_ = client.Close()
				}
				t.Fatalf("Insecure %v: Dial returned %v, want the key-changed refusal", insecure, err)
			}
			if changed.Presented != server.fingerprint || !strings.Contains(err.Error(), withOther.KnownServers) {
				t.Fatalf("the refusal must name the presented fingerprint and the file: %v", err)
			}
		}
	})
	t.Run("a server not recorded is refused and not added", func(t *testing.T) {
		elsewhere := knownServersFile(t, "192.0.2.1:7433", serverKey)
		before, err := os.ReadFile(elsewhere)
		if err != nil {
			t.Fatal(err)
		}
		client, err := Dial(testContext(t), options(elsewhere))
		var unknown *trust.UnknownServerError
		if !errors.As(err, &unknown) {
			if client != nil {
				_ = client.Close()
			}
			t.Fatalf("Dial returned %v, want the unknown-server refusal", err)
		}
		if !strings.Contains(err.Error(), server.fingerprint) || !strings.Contains(err.Error(), "KnownServers") {
			t.Fatalf("the refusal must give the fingerprint to pin and name the option: %v", err)
		}
		if after, err := os.ReadFile(elsewhere); err != nil || string(after) != string(before) {
			t.Fatalf("the known-servers file changed: %q, %v", after, err)
		}
	})
	t.Run("Fingerprint decides when both are set", func(t *testing.T) {
		both := options(knownServersFile(t, server.addr(), otherKey))
		both.Fingerprint = server.fingerprint
		client, err := Dial(testContext(t), both)
		if err != nil {
			t.Fatalf("Dial with the right Fingerprint: %v", err)
		}
		_ = client.Close()
	})
	t.Run("the refusal of no trust at all names the option", func(t *testing.T) {
		_, err := Dial(testContext(t), Options{Addr: server.addr(), User: "root", Password: "pw"})
		if err == nil || !strings.Contains(err.Error(), "KnownServers") {
			t.Fatalf("Dial returned %v, want a refusal that names KnownServers beside Fingerprint and Insecure", err)
		}
	})
}

// TestNoResendSendsOneKeyedFrameAfterRelearningAnUndeclaredField: NoResend promises that the
// keyed write frame is sent at most once, not that a call sends exactly one frame. A cached
// header that predates an alter by another client sends a DESCRIBE, then the one keyed frame.
func TestNoResendSendsOneKeyedFrameAfterRelearningAnUndeclaredField(t *testing.T) {
	var decl, schema, describes, keyed atomic.Int64
	decl.Store(1)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		if frame.Op == hoarddbwire.OpRingKeyed {
			keyed.Add(1)
			reply, err := ringwire.EncodeRingReply(ringwire.RingReply{WriteOutcome: "applied"})
			if err != nil {
				t.Error(err)
				return
			}
			conn.reply(frame, reply)
			return
		}
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		describes.Add(1)
		fields := userFields()
		if decl.Load() >= 2 {
			fields = append(fields, ringwire.RingAdminReplyField{ID: 4, Name: "Total", Type: "int"})
		}
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 77, State: "active",
			DeclVersion: decl.Load(), SchemaVersionSeen: schema.Load(), Fields: fields})
	})
	client := dialNoResend(t, server)
	users := client.Bucket("db", "User")
	if _, err := users.Info(testContext(t)); err != nil { // caches the header without Total
		t.Fatal(err)
	}
	decl.Store(2) // another client added Total
	result, err := users.Insert(testContext(t), "ada", Doc{"Total": int64(5)})
	if err != nil || !result.Applied() {
		t.Fatalf("the insert after the relearn: %+v, %v; want it applied", result, err)
	}
	if got := keyed.Load(); got != 1 {
		t.Fatalf("%d keyed frame(s) went out, want 1: NoResend promises at most one", got)
	}
	if got := describes.Load(); got != 2 {
		t.Fatalf("%d describe(s) went out, want 2: the one that cached the old header and the relearn", got)
	}
}
