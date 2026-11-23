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
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

const mebibyte = 1 << 20

// limitsContext bounds a leg 6 check: the large ones move tens of megabytes
// under the race detector.
func limitsContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

func connectDirect(t *testing.T, server *servertest.Server) *hoarddb.Client {
	t.Helper()
	client, err := hoarddb.Connect(limitsContext(t), fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, server.Address, server.Fingerprint))
	if err != nil {
		t.Fatalf("connecting: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// textDocument is a document of exactly size encoded bytes: one string field
// T. Its BSON is 4 (length) + 1 (type) + 2 ("T\0") + 4 (string length) + the
// text + 1 (its NUL) + 1 (the end of the document).
func textDocument(size int) hoarddb.Map {
	return hoarddb.Map{"T": strings.Repeat("x", size-13)}
}

// rawTLS opens a TLS connection to the server pinned to its key and offering
// hoarddb/1, and nothing else: the frames written on it are the test's own.
func rawTLS(t *testing.T, server *servertest.Server) *tls.Conn {
	t.Helper()
	config, err := trust.NewTLSConfig(server.Address, trust.Policy{Pin: server.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: config}
	conn, err := dialer.DialContext(limitsContext(t), "tcp", server.Address)
	if err != nil {
		t.Fatalf("dialling %s: %v", server.Address, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn.(*tls.Conn)
}

// header is a frame header as a client that does not check its sizes would
// write it: version, length word, opcode, request ID.
func header(version byte, length uint32, op hoarddbwire.Opcode, id hoarddbwire.RequestID) []byte {
	var frame bytes.Buffer
	frame.WriteByte(version)
	_ = binary.Write(&frame, binary.LittleEndian, length)
	frame.WriteByte(byte(op))
	frame.Write(id[:])
	return frame.Bytes()
}

// TestLeg6Limits is conformance leg 6 (handbook/testing.md section 2): the
// size limits of handbook/wire.md section 10 and the connection-level
// refusals, each provoked live on a server tuned for it.
func TestLeg6Limits(t *testing.T) {
	checks := []struct {
		name string
		run  func(t *testing.T)
	}{
		{"a document just under and just over 24 MiB", checkDocumentLimit},
		{"a blob at 16 MiB and just over", checkBlobLimit},
		{"a write that cannot fit a replication frame", checkReplicationFit},
		{"the driver refuses a frame over 32 MiB before sending", checkDriverFrameLimit},
		{"the server answers an oversize frame by its request ID", checkOversizeAnsweredByID},
		{"a bad version byte is closed without an answer", checkBadVersionByte},
		{"a query over the result ceiling, then PING on the same connection", checkResultCeiling},
		{"the per-address cap is OVERLOADED with a zero request ID, and the pool survives it", checkPerAddressCap},
	}
	passed := 0
	for _, check := range checks {
		if t.Run(check.name, check.run) {
			passed++
		}
	}
	t.Logf("leg 6 limits: %d checks, %d passed", len(checks), passed)
}

func checkDocumentLimit(t *testing.T) {
	client := connectDirect(t, servertest.Start(t, servertest.Options{}))
	ctx := limitsContext(t)
	bucket, err := client.Database("limits").CreateBucket(ctx, "docs", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	under := textDocument(24*mebibyte - 1024)
	if _, err := bucket.Put(ctx, "under", under); err != nil {
		t.Fatalf("a document 1 KiB under 24 MiB: %v", err)
	}
	stored, err := bucket.Get(ctx, "under")
	if err != nil {
		t.Fatalf("reading it back: %v", err)
	}
	if text, _ := stored.Lookup("T"); fmt.Sprint(len(mustString(t, text))) != fmt.Sprint(len(under["T"].(string))) {
		t.Fatalf("the document read back is not the one written")
	}

	_, err = bucket.Put(ctx, "over", textDocument(24*mebibyte+1024))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), `Document "over" is`) {
		t.Fatalf("a document 1 KiB over 24 MiB: %v; want ErrInvalidArgument with the server's sentence", err)
	}
	t.Logf("over: %v", err)
}

func mustString(t *testing.T, value hoarddb.Value) string {
	t.Helper()
	text, ok := value.AsString()
	if !ok {
		t.Fatalf("%s is not a string", value)
	}
	return text
}

func checkBlobLimit(t *testing.T) {
	client := connectDirect(t, servertest.Start(t, servertest.Options{}))
	ctx := limitsContext(t)
	files, err := client.Database("limits").CreateBucket(ctx, "files", hoarddb.Blob)
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte{0xA5}, 16*mebibyte)
	if _, err := files.PutBlob(ctx, "at.bin", content); err != nil {
		t.Fatalf("a blob of exactly 16 MiB: %v", err)
	}
	stored, err := files.GetBlob(ctx, "at.bin")
	if err != nil || !bytes.Equal(stored.Data, content) {
		t.Fatalf("reading the 16 MiB blob back: %v", err)
	}
	_, err = files.PutBlob(ctx, "over.bin", append(content, 0xA5))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), `Blob "over.bin" is`) ||
		!strings.Contains(err.Error(), "this server's limit is") {
		t.Fatalf("a blob of 16 MiB and 1 byte: %v; want ErrInvalidArgument with the server's sentence", err)
	}
	t.Logf("over: %v", err)
}

// A document at the largest limit an operator can set, under a key long enough
// that the write and its copy for a replica no longer fit one frame.
func checkReplicationFit(t *testing.T) {
	server := servertest.Start(t, servertest.Options{Env: map[string]string{"HOARDDB_MAX_DOCUMENT_BYTES": fmt.Sprint(30 * mebibyte)}})
	client := connectDirect(t, server)
	ctx := limitsContext(t)
	bucket, err := client.Database("limits").CreateBucket(ctx, "docs", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	_, err = bucket.Put(ctx, strings.Repeat("k", 2*mebibyte), textDocument(30*mebibyte-64*1024))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "too much to replicate") {
		t.Fatalf("a write too large to replicate: %v; want ErrInvalidArgument saying it is too much to replicate", err)
	}
	t.Logf("refused: %s", err.Error()[strings.Index(err.Error(), "…"):])
}

func checkDriverFrameLimit(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	proxy := servertest.StartProxy(t, server)
	client, err := hoarddb.Connect(limitsContext(t), fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	_, err = client.Database("limits").Bucket("docs").Put(limitsContext(t), "huge", textDocument(33*mebibyte))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "Nothing was sent") || !strings.Contains(err.Error(), "33554432") {
		t.Fatalf("a 33 MiB document: %v; want the driver's own refusal with the arithmetic", err)
	}
	if exchanges := proxy.Exchanges(); len(exchanges) != 0 {
		t.Fatalf("%d requests crossed the wire; the driver must refuse before writing a byte", len(exchanges))
	}
}

func checkOversizeAnsweredByID(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	conn := rawTLS(t, server)
	var id hoarddbwire.RequestID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	// A length word one byte over the limit, and no body: the server answers
	// from the header alone (handbook/wire.md section 2).
	if _, err := conn.Write(header(1, hoarddbwire.MaxFrameLength+1, hoarddbwire.OpBatchPut, id)); err != nil {
		t.Fatal(err)
	}
	answer, err := hoarddbwire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("no answer to an oversize header: %v", err)
	}
	refusal := hoarddbwire.DecodeError(answer.Payload)
	if answer.Op != hoarddbwire.OpError || answer.ID != id || refusal.Code != hoarddbwire.CodeInvalidArgument ||
		!strings.Contains(refusal.Message, "over the 32 MiB frame limit") {
		t.Fatalf("answer %s, ID %s (sent %s), %+v; want INVALID_ARGUMENT echoing the request ID", answer.Op, answer.ID, id, refusal)
	}
	t.Logf("answered by its request ID: %s", refusal.Message)
}

func checkBadVersionByte(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	conn := rawTLS(t, server)
	var id hoarddbwire.RequestID
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(header(2, 17, hoarddbwire.OpPing, id)); err != nil {
		t.Fatal(err)
	}
	answer, err := hoarddbwire.ReadFrame(conn)
	if err == nil {
		t.Fatalf("the server answered a bad version byte with %s; want the connection closed without an answer", answer.Op)
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		t.Fatalf("the server neither answered nor closed within 10 s: %v", err)
	}
	t.Logf("closed without an answer: %v", err)
}

func checkResultCeiling(t *testing.T) {
	server := servertest.Start(t, servertest.Options{Env: map[string]string{"HOARDDB_QUERY_MAX_RESULT_BYTES": fmt.Sprint(mebibyte)}})
	client := connectDirect(t, server)
	ctx := limitsContext(t)
	bucket, err := client.Database("limits").CreateBucket(ctx, "big", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		if _, err := bucket.Put(ctx, key, textDocument(700_000)); err != nil {
			t.Fatal(err)
		}
	}

	// On the wire: the refusal, then PING on the same connection.
	conn, err := server.Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	reply, err := conn.Do(ctx, hoarddbwire.OpScan, hoarddbwire.ScanRequest{Database: "limits", Bucket: "big", Limit: 100}, nil)
	if err != nil {
		t.Fatal(err)
	}
	refusal := hoarddbwire.DecodeError(reply.Payload)
	if reply.Op != hoarddbwire.OpError || refusal.Code != hoarddbwire.CodeResultTooLarge || refusal.MaxResultBytes != mebibyte || refusal.RetainedBytes <= 0 {
		t.Fatalf("SCAN over the ceiling: %s %+v; want RESULT_TOO_LARGE with both numbers", reply.Op, refusal)
	}
	pong, err := conn.Do(ctx, hoarddbwire.OpPing, nil, nil)
	if err != nil || pong.Op != hoarddbwire.OpPong {
		t.Fatalf("PING on the same connection after RESULT_TOO_LARGE: %s, %v", pong.Op, err)
	}

	// Through the public API: the class and both numbers, and the connection
	// kept.
	huge, err := client.Database("limits").CreateBucket(ctx, "huge", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := huge.Put(ctx, "one", textDocument(1_200_000)); err != nil {
		t.Fatal(err)
	}
	before := client.PoolStats()
	_, err = huge.Find(ctx, nil)
	var failure *hoarddb.Error
	// The server reports retained_bytes as what it held before it stopped: none,
	// when the first document alone is over the ceiling.
	if !errors.As(err, &failure) || !errors.Is(err, hoarddb.ErrResultTooLarge) || failure.MaxResultBytes != mebibyte {
		t.Fatalf("Find of a document over the ceiling: %v; want ErrResultTooLarge with MaxResultBytes", err)
	}
	if after := client.PoolStats(); after.Discarded != before.Discarded {
		t.Fatalf("PoolStats %+v after %+v: RESULT_TOO_LARGE leaves the connection open, so it is kept", after, before)
	}
}

func checkPerAddressCap(t *testing.T) {
	server := servertest.Start(t, servertest.Options{Env: map[string]string{"HOARDDB_TCP_MAX_CONNS_PER_IP": "1"}})
	client := connectDirect(t, server) // holds the address's one connection
	ctx := limitsContext(t)
	bucket, err := client.Database("limits").CreateBucket(ctx, "docs", hoarddb.Hash)
	if err != nil {
		t.Fatal(err)
	}

	// On the wire: a second connection is refused with a zero request ID, then
	// closed.
	conn := rawTLS(t, server)
	refused, err := hoarddbwire.ReadFrame(conn)
	if err != nil {
		t.Fatalf("no refusal on a connection over the cap: %v", err)
	}
	refusal := hoarddbwire.DecodeError(refused.Payload)
	if refused.Op != hoarddbwire.OpError || !refused.ID.IsZero() || refusal.Code != hoarddbwire.CodeOverloaded {
		t.Fatalf("got %s with ID %s, %+v; want OVERLOADED with a zero request ID", refused.Op, refused.ID, refusal)
	}
	if extra, err := hoarddbwire.ReadFrame(conn); err == nil {
		t.Fatalf("the server sent %s after the refusal; want the connection closed", extra.Op)
	}

	// Through the public API: the bulk lane needs a second connection. The
	// driver backs that dial off and tries again until the caller's deadline,
	// then reports OVERLOADED in the server's words.
	const deadline = 1500 * time.Millisecond
	short, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	started := time.Now()
	_, err = bucket.Find(short, nil)
	elapsed := time.Since(started)
	if !errors.Is(err, hoarddb.ErrOverloaded) || !strings.Contains(err.Error(), refusal.Message) {
		t.Fatalf("Find over the cap: %v; want ErrOverloaded with the server's sentence %q", err, refusal.Message)
	}
	if !errors.Is(err, context.DeadlineExceeded) || elapsed < deadline-300*time.Millisecond {
		t.Fatalf("Find over the cap returned after %v (%v); want the dial backed off and retried until the %v deadline", elapsed, err, deadline)
	}
	t.Logf("over the cap, after %v of redialling: %v", elapsed.Round(time.Millisecond), err)

	// A write that needs the refused dial (the bulk lane) is backed off and
	// retried the same way until the deadline, and reports the server's
	// OVERLOADED: nothing of it was sent, so it is never unknown, and nothing is
	// stored.
	writeCtx, cancelWrite := context.WithTimeout(ctx, deadline)
	defer cancelWrite()
	retriesBefore := client.PoolStats().Retries // cumulative: the Find above already added to it
	writeStarted := time.Now()
	_, err = bucket.InsertMany(writeCtx, []any{hoarddb.Map{"Name": "Grace"}})
	writeElapsed := time.Since(writeStarted)
	var refusedWrite *hoarddb.Error
	if !errors.As(err, &refusedWrite) || !errors.Is(err, hoarddb.ErrOverloaded) || refusedWrite.Outcome == hoarddb.OutcomeUnknown {
		t.Fatalf("InsertMany over the cap: %v; want ErrOverloaded and an outcome that is not unknown", err)
	}
	if made := client.PoolStats().Retries - retriesBefore; made == 0 {
		t.Fatal("InsertMany over the cap made no retry of its own; the refused dial is backed off and retried")
	}
	if writeElapsed < deadline-300*time.Millisecond {
		t.Fatalf("InsertMany over the cap returned after %v; want the refused dial retried until the %v deadline, not given up on at the first refusal", writeElapsed, deadline)
	}
	if count, err := bucket.Count(ctx, nil); err != nil || count != 0 {
		t.Fatalf("Count after the refused InsertMany = %d, %v; want 0: nothing was stored", count, err)
	}

	// The pool survives: the point lane's connection still answers.
	if _, err := bucket.Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatalf("Put on the open connection after OVERLOADED: %v", err)
	}
	if stats := client.PoolStats(); stats.Open != 1 {
		t.Fatalf("PoolStats = %+v; want the one connection still open", stats)
	}
}
