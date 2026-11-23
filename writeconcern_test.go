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
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Component tests of the write concern (handbook/build-go.md step 13, D-4)
// against the in-process fake server, which records every request the driver
// sends. The real-server proofs are conformance leg 9
// (conformance/cluster_test.go).

// sentRequest is one request the fake server received, its payload decoded.
type sentRequest struct {
	op      hoarddbwire.Opcode
	members map[string]any
}

// requestRecorder keeps the requests a fake server received, in order.
type requestRecorder struct {
	mu   sync.Mutex
	sent []sentRequest
}

func (recorder *requestRecorder) add(t *testing.T, frame hoarddbwire.Frame) {
	members := map[string]any{}
	if len(frame.Payload) > 0 {
		if err := unmarshal(frame.Payload, &members); err != nil {
			t.Errorf("the driver sent a %s payload that is not a document: %v", frame.Op, err)
		}
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	recorder.sent = append(recorder.sent, sentRequest{op: frame.Op, members: members})
}

func (recorder *requestRecorder) requests() []sentRequest {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]sentRequest(nil), recorder.sent...)
}

// answerWrite answers every write verb and the reads the tests send with a
// success of the right shape.
func answerWrite(t *testing.T, c *fakeConn, request hoarddbwire.Frame) {
	t.Helper()
	ada := adaDocument(t)
	switch request.Op {
	case hoarddbwire.OpPut, hoarddbwire.OpFIFOPush:
		c.send(request, request.Op, hoarddbwire.InsertResponse{ID: "ada", Success: true, Seq: 1})
	case hoarddbwire.OpBatchPut:
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{IDs: []string{"ada"}, Count: 1, Success: true})
	case hoarddbwire.OpDelete:
		c.send(request, request.Op, hoarddbwire.DeleteResponse{Success: true})
	case hoarddbwire.OpUpdate:
		c.send(request, request.Op, hoarddbwire.UpdateResponse{Success: true, Doc: ada})
	case hoarddbwire.OpFIFOPop:
		c.send(request, hoarddbwire.OpFIFOPopResp, hoarddbwire.PopResponse{Found: true, Items: []hoarddbwire.Document{ada}})
	case hoarddbwire.OpFIFOPeek:
		c.send(request, request.Op, hoarddbwire.PeekResponse{Found: true, Items: []hoarddbwire.Document{ada}})
	case hoarddbwire.OpQueueClaim:
		c.send(request, request.Op, hoarddbwire.ClaimResponse{Found: true, Items: []hoarddbwire.Document{ada},
			ItemIDs: []string{"0000000000000000"}, LeaseID: aLeaseID, LeaseMS: 30000})
	case hoarddbwire.OpQueueAck:
		c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "acked", Acked: 1})
	case hoarddbwire.OpQueueRelease:
		c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "released", Released: 1})
	case hoarddbwire.OpQueueExtend:
		c.send(request, request.Op, hoarddbwire.LeaseResponse{Success: true, LeaseState: "extended", LeaseMS: 60000})
	case hoarddbwire.OpDeadRequeue, hoarddbwire.OpDeadDiscard:
		c.send(request, request.Op, hoarddbwire.DeadMoveResponse{Success: true, Done: 1})
	case hoarddbwire.OpBlobWrite:
		c.send(request, request.Op, hoarddbwire.BlobWriteResponse{Success: true, Key: "hello.txt", BlobID: "b1", Size: 5})
	case hoarddbwire.OpGet:
		c.send(request, hoarddbwire.OpGetResponse, hoarddbwire.GetResponse{Found: true, Data: ada})
	case hoarddbwire.OpFieldList:
		c.send(request, request.Op, hoarddbwire.FieldListResponse{Success: true, Entries: []hoarddbwire.FieldEntry{{Element: ada}}})
	case hoarddbwire.OpFieldPush, hoarddbwire.OpFieldPop, hoarddbwire.OpFieldPut, hoarddbwire.OpFieldDelete,
		hoarddbwire.OpFieldPeek, hoarddbwire.OpFieldGet, hoarddbwire.OpFieldLen:
		c.send(request, request.Op, hoarddbwire.FieldResponse{Success: true, Found: true, Element: ada,
			Items: []hoarddbwire.FieldEntry{{Element: ada}}})
	default:
		c.errorFrame(request, 5, "the fake server does not answer "+request.Op.String())
	}
}

// recordingServer is a fake server that advertises capabilities, records every
// request and answers it with answerWrite.
func recordingServer(t *testing.T, capabilities ...string) (*fakeServer, *requestRecorder) {
	recorder := &requestRecorder{}
	server := startFake(t, serveFramesWith(capabilities, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		recorder.add(t, request)
		answerWrite(t, c, request)
		return true
	}))
	return server, recorder
}

// writeVerb is one public write and the opcode it sends.
type writeVerb struct {
	name   string
	opcode hoarddbwire.Opcode
	call   func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error
}

// asPopOptions passes write options to a pop, which takes them too.
func asPopOptions(opts []hoarddb.WriteOption) []hoarddb.PopOption {
	converted := make([]hoarddb.PopOption, len(opts))
	for position, option := range opts {
		converted[position] = option
	}
	return converted
}

// asClaimOptions passes write options to a claim, which takes them too.
func asClaimOptions(opts []hoarddb.WriteOption) []hoarddb.ClaimOption {
	converted := make([]hoarddb.ClaimOption, len(opts))
	for position, option := range opts {
		converted[position] = option
	}
	return converted
}

// asAckOptions passes write options to an ack, which takes them too.
func asAckOptions(opts []hoarddb.WriteOption) []hoarddb.AckOption {
	converted := make([]hoarddb.AckOption, len(opts))
	for position, option := range opts {
		converted[position] = option
	}
	return converted
}

// asBlobOptions passes write options to a blob write, which takes them too.
func asBlobOptions(opts []hoarddb.WriteOption) []hoarddb.BlobOption {
	converted := make([]hoarddb.BlobOption, len(opts))
	for position, option := range opts {
		converted[position] = option
	}
	return converted
}

// everyWriteVerb is every public call that writes (handbook/operations.md
// section 2 and section 5): each must carry the write concern (D-4).
var everyWriteVerb = []writeVerb{
	{"Bucket.Put", hoarddbwire.OpPut, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("users").Put(ctx, "ada", hoarddb.Map{"Name": "Ada"}, opts...)
		return err
	}},
	{"Bucket.Insert", hoarddbwire.OpPut, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("users").Insert(ctx, hoarddb.Map{"Name": "Ada"}, opts...)
		return err
	}},
	{"Bucket.InsertMany", hoarddbwire.OpBatchPut, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("users").InsertMany(ctx, []any{hoarddb.Map{"Name": "Ada"}}, opts...)
		return err
	}},
	{"Bucket.Delete", hoarddbwire.OpDelete, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("users").Delete(ctx, "ada", opts...)
		return err
	}},
	{"Bucket.Update", hoarddbwire.OpUpdate, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("users").Update(ctx, "ada", hoarddb.Update{Set: hoarddb.Map{"Name": "Ada"}}, opts...)
		return err
	}},
	{"Bucket.Push", hoarddbwire.OpFIFOPush, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("jobs").Push(ctx, hoarddb.Map{"Job": "send the invoice"}, opts...)
		return err
	}},
	{"Bucket.Pop", hoarddbwire.OpFIFOPop, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("jobs").Pop(ctx, asPopOptions(opts)...)
		return err
	}},
	{"Bucket.Claim", hoarddbwire.OpQueueClaim, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("jobs").Claim(ctx, asClaimOptions(opts)...)
		return err
	}},
	{"Bucket.Ack", hoarddbwire.OpQueueAck, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		lease, _ := hoarddb.ParseLeaseID(aLeaseID)
		_, err := database.Bucket("jobs").Ack(ctx, lease, asAckOptions(opts)...)
		return err
	}},
	{"Bucket.Release", hoarddbwire.OpQueueRelease, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		lease, _ := hoarddb.ParseLeaseID(aLeaseID)
		_, err := database.Bucket("jobs").Release(ctx, lease, opts...)
		return err
	}},
	{"Bucket.Extend", hoarddbwire.OpQueueExtend, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		lease, _ := hoarddb.ParseLeaseID(aLeaseID)
		_, err := database.Bucket("jobs").Extend(ctx, lease, time.Minute, opts...)
		return err
	}},
	{"Bucket.Requeue", hoarddbwire.OpDeadRequeue, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("jobs").Requeue(ctx, hoarddb.AllDead(), opts...)
		return err
	}},
	{"Bucket.Discard", hoarddbwire.OpDeadDiscard, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("jobs").Discard(ctx, hoarddb.AllDead(), opts...)
		return err
	}},
	{"Bucket.PutBlob", hoarddbwire.OpBlobWrite, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("files").PutBlob(ctx, "hello.txt", []byte("hello"), asBlobOptions(opts)...)
		return err
	}},
	{"FieldStore.Push", hoarddbwire.OpFieldPush, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("User").Field("ada", "Purchases").Push(ctx, hoarddb.Map{"Item": "pen"}, opts...)
		return err
	}},
	{"FieldStore.Pop", hoarddbwire.OpFieldPop, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("User").Field("ada", "Purchases").Pop(ctx, asPopOptions(opts)...)
		return err
	}},
	{"FieldStore.Put", hoarddbwire.OpFieldPut, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("User").Field("ada", "Friends").Put(ctx, "grace", hoarddb.Map{"Since": 1952}, opts...)
		return err
	}},
	{"FieldStore.Delete", hoarddbwire.OpFieldDelete, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("User").Field("ada", "Friends").Delete(ctx, "grace", opts...)
		return err
	}},
	{"FieldStore.PutBlob", hoarddbwire.OpFieldPut, func(ctx context.Context, database *hoarddb.Database, opts ...hoarddb.WriteOption) error {
		_, err := database.Bucket("User").Field("ada", "Data").PutBlob(ctx, "avatar", []byte("png"), asBlobOptions(opts)...)
		return err
	}},
}

// Failure 1 (handbook/build-go.md section 2): a write concern the driver
// leaves off the wire, or sends under another name, is silently replaced by the
// server's default. Every write verb must send writeconcern, from the call's
// option or the client's default, the call's winning.
func TestEveryWriteVerbSendsTheConcern(t *testing.T) {
	cases := []struct {
		name     string
		defaults []hoarddb.Option
		perCall  []hoarddb.WriteOption
		want     string
	}{
		{"per call", nil, []hoarddb.WriteOption{hoarddb.WithWriteConcern(hoarddb.All)}, "all"},
		{"the client's default", []hoarddb.Option{hoarddb.WithDefaultWriteConcern(hoarddb.Majority)}, nil, "majority"},
		{"the call over the default", []hoarddb.Option{hoarddb.WithDefaultWriteConcern(hoarddb.Majority)},
			[]hoarddb.WriteOption{hoarddb.WithWriteConcern(hoarddb.One)}, "one"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, recorder := recordingServer(t, "write_concern")
			client := mustConnect(t, server.uri(), tc.defaults...)
			database := client.Database("app")
			for _, verb := range everyWriteVerb {
				before := len(recorder.requests())
				if err := verb.call(testContext(t), database, tc.perCall...); err != nil {
					t.Errorf("%s: %v", verb.name, err)
					continue
				}
				sent := recorder.requests()[before:]
				if len(sent) != 1 || sent[0].op != verb.opcode {
					t.Errorf("%s sent %v; want one %s", verb.name, sent, verb.opcode)
					continue
				}
				if got, _ := sent[0].members["writeconcern"].(string); got != tc.want {
					t.Errorf("%s sent writeconcern %q (request %v); want %q", verb.name, got, sent[0].members, tc.want)
				}
			}
		})
	}
}

// A write concern belongs to writes: the client's default is not added to a
// read, and no write carries one when none was asked for. Every store-field
// read is called, because they share FieldRequest, which has the member, with
// the field writes.
func TestReadsAndUnconcernedWritesSendNoConcern(t *testing.T) {
	server, recorder := recordingServer(t, "write_concern")
	client := mustConnect(t, server.uri(), hoarddb.WithDefaultWriteConcern(hoarddb.All))
	ctx := testContext(t)
	users := client.Database("app").Bucket("users")
	reads := map[string]func() error{
		"Bucket.Get":  func() error { _, err := users.Get(ctx, "ada"); return err },
		"Bucket.Peek": func() error { _, err := client.Database("app").Bucket("jobs").Peek(ctx); return err },
		"FieldStore.Peek": func() error {
			_, err := users.Field("ada", "Purchases").Peek(ctx)
			return err
		},
		"FieldStore.Get": func() error { _, err := users.Field("ada", "Friends").Get(ctx, "grace"); return err },
		"FieldStore.GetBlob": func() error {
			_, err := users.Field("ada", "Data").GetBlob(ctx, "avatar")
			return err
		},
		"FieldStore.Len": func() error { _, err := users.Field("ada", "Purchases").Len(ctx); return err },
		"FieldStore.List": func() error {
			cursor, err := users.Field("ada", "Purchases").List(ctx)
			if err != nil {
				return err
			}
			return cursor.Close()
		},
	}
	for name, read := range reads {
		if err := read(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	readOps := map[hoarddbwire.Opcode]bool{}
	for _, sent := range recorder.requests() {
		readOps[sent.op] = true
	}
	for _, op := range []hoarddbwire.Opcode{hoarddbwire.OpGet, hoarddbwire.OpFIFOPeek, hoarddbwire.OpFieldPeek,
		hoarddbwire.OpFieldGet, hoarddbwire.OpFieldLen, hoarddbwire.OpFieldList} {
		if !readOps[op] {
			t.Errorf("no %s was sent; the test must call every read", op)
		}
	}
	for _, sent := range recorder.requests() {
		if _, carried := sent.members["writeconcern"]; carried {
			t.Errorf("the read %s carried writeconcern: %v", sent.op, sent.members)
		}
	}

	plain, plainRecorder := recordingServer(t, "write_concern")
	unconcerned := mustConnect(t, plain.uri())
	for _, verb := range everyWriteVerb {
		if err := verb.call(ctx, unconcerned.Database("app")); err != nil {
			t.Errorf("%s: %v", verb.name, err)
		}
	}
	for _, sent := range plainRecorder.requests() {
		if _, carried := sent.members["writeconcern"]; carried {
			t.Errorf("%s carried writeconcern with none asked for: %v", sent.op, sent.members)
		}
	}
}

// A word the server does not know is refused by the driver before anything is
// sent, with the words it accepts: the server's own refusal of it is
// conformance leg 9's to prove, and nothing the driver sends depends on it.
func TestUnknownWriteConcernIsRefusedBeforeSending(t *testing.T) {
	server, recorder := recordingServer(t, "write_concern")
	client := mustConnect(t, server.uri())
	for _, verb := range everyWriteVerb {
		err := verb.call(testContext(t), client.Database("app"), hoarddb.WithWriteConcern("quorum"))
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) {
			t.Errorf("%s with write concern \"quorum\" = %v; want ErrInvalidArgument", verb.name, err)
			continue
		}
		if failure.Outcome != hoarddb.OutcomeNotApplied || !strings.Contains(failure.Message, `"quorum"`) ||
			!strings.Contains(failure.Message, "hoarddb.Majority") || !strings.Contains(failure.Message, "nothing was sent") {
			t.Errorf("%s: %+v; want OutcomeNotApplied and a message naming the word, the accepted words and that nothing was sent", verb.name, failure)
		}
	}
	if sent := recorder.requests(); len(sent) != 0 {
		t.Fatalf("the driver sent %d requests with an unknown write concern; want none", len(sent))
	}

	// An empty word cannot ask one call for the server's default over the
	// client's: the message says so, rather than "leave the option out".
	err := everyWriteVerb[0].call(testContext(t), client.Database("app"), hoarddb.WithWriteConcern(""))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !strings.Contains(err.Error(), "WithDefaultWriteConcern") ||
		!strings.Contains(err.Error(), "server's default") {
		t.Errorf("WithWriteConcern(\"\") = %v; want ErrInvalidArgument saying a call cannot ask for the server's default over WithDefaultWriteConcern", err)
	}

	_, err = hoarddb.Connect(testContext(t), server.uri(), hoarddb.WithDefaultWriteConcern("quorum"))
	if !errors.Is(err, hoarddb.ErrInvalidURI) || !strings.Contains(err.Error(), `"quorum"`) {
		t.Fatalf("Connect with a default write concern \"quorum\" = %v; want the option refused, naming the word", err)
	}
}

// handbook/wire.md section 6: a driver refuses to send an option the server did
// not advertise, rather than send a field an older server drops in silence.
func TestWriteConcernNeedsTheServersCapability(t *testing.T) {
	server, recorder := recordingServer(t, "auth_token")
	client := mustConnect(t, server.uri())
	for _, verb := range everyWriteVerb {
		err := verb.call(testContext(t), client.Database("app"), hoarddb.WithWriteConcern(hoarddb.All))
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrUnsupportedByServer) || !errors.As(err, &failure) {
			t.Errorf("%s with a write concern on a server without write_concern = %v; want ErrUnsupportedByServer", verb.name, err)
			continue
		}
		if failure.Outcome != hoarddb.OutcomeNotApplied || !strings.Contains(failure.Message, "write_concern") ||
			!strings.Contains(failure.Message, "nothing was sent") {
			t.Errorf("%s: %+v; want OutcomeNotApplied, the capability named and that nothing was sent", verb.name, failure)
		}
	}
	if sent := recorder.requests(); len(sent) != 0 {
		t.Fatalf("the driver sent %d requests the server cannot honour; want none", len(sent))
	}
	if _, err := client.Database("app").Bucket("users").Put(testContext(t), "ada", hoarddb.Map{"Name": "Ada"}); err != nil {
		t.Fatalf("a write without a write concern on the same server: %v", err)
	}
}

// A write refused on its write concern is OutcomeNotApplied only when the
// server says so (handbook/errors.md section 4: absent is "treat as unknown
// for a write"), and is sent exactly once either way: INVALID_ARGUMENT is never
// retried, and an outcome the server did not report is never resent.
func TestConcernRefusalOutcomeIsTheServers(t *testing.T) {
	cases := []struct {
		name    string
		members map[string]any
		want    hoarddb.WriteOutcome
	}{
		{"reported not applied", map[string]any{"code": int32(4), "write_outcome": "not_applied",
			"message": `unknown write concern: "majority" is not one of: 1, one, all`}, hoarddb.OutcomeNotApplied},
		// An ordinary ERROR frame on a data write with no stamp is unknown, with the
		// driver's advice after the server's sentence (handbook/errors.md section 4).
		{"not reported", map[string]any{"code": int32(4),
			"message": `unknown write concern: "majority" is not one of: 1, one, all`}, hoarddb.OutcomeUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var log frameLog
			server := startFake(t, serveFramesWith([]string{"write_concern"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				log.add(request.Op)
				c.errorWith(request, tc.members)
				return true
			}))
			client := mustConnect(t, server.uri())
			_, err := client.Database("app").Bucket("jobs").Push(testContext(t), hoarddb.Map{"Job": "x"},
				hoarddb.WithWriteConcern(hoarddb.Majority))
			var failure *hoarddb.Error
			if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) {
				t.Fatalf("Push = %v; want ErrInvalidArgument", err)
			}
			sentence, _ := tc.members["message"].(string)
			if failure.Outcome != tc.want || !strings.HasPrefix(failure.Message, sentence) ||
				(tc.want == hoarddb.OutcomeUnknown) != strings.Contains(failure.Message, "Whether the write was applied is unknown") ||
				(tc.want != hoarddb.OutcomeUnknown && failure.Message != sentence) {
				t.Errorf("error = %+v; want outcome %v and the server's sentence first (verbatim when stamped, then the advice when not)", failure, tc.want)
			}
			time.Sleep(300 * time.Millisecond) // a wrongly scheduled resend would arrive by now
			if sent := log.count(hoarddbwire.OpFIFOPush); sent != 1 {
				t.Fatalf("FIFO_PUSH sent %d times, want 1", sent)
			}
		})
	}
}

// The client's default level is held to the server's capability as a call's
// is: a server that does not advertise write_concern gets nothing, and the
// error names the option the level came from.
func TestDefaultWriteConcernNeedsTheServersCapability(t *testing.T) {
	server, recorder := recordingServer(t, "auth_token")
	client := mustConnect(t, server.uri(), hoarddb.WithDefaultWriteConcern(hoarddb.Majority))
	for _, verb := range everyWriteVerb {
		err := verb.call(testContext(t), client.Database("app"))
		var failure *hoarddb.Error
		if !errors.Is(err, hoarddb.ErrUnsupportedByServer) || !errors.As(err, &failure) {
			t.Errorf("%s with a default write concern on a server without write_concern = %v; want ErrUnsupportedByServer", verb.name, err)
			continue
		}
		if failure.Outcome != hoarddb.OutcomeNotApplied || !strings.Contains(failure.Message, "WithDefaultWriteConcern") ||
			!strings.Contains(failure.Message, "nothing was sent") {
			t.Errorf("%s: %+v; want OutcomeNotApplied, the option named and that nothing was sent", verb.name, failure)
		}
	}
	if sent := recorder.requests(); len(sent) != 0 {
		t.Fatalf("the driver sent %d requests the server cannot honour; want none", len(sent))
	}
}

// D-5 with a write concern: InsertMany halves a batch the server refuses whole
// as over its atomic bound, and every half is sent at the caller's level, not
// the server's default.
func TestInsertManyHalvesCarryTheConcern(t *testing.T) {
	recorder := &requestRecorder{}
	server := startFake(t, serveFramesWith([]string{"write_concern"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		recorder.add(t, request)
		var batch hoarddbwire.BatchPutRequest
		if err := unmarshal(request.Payload, &batch); err != nil {
			t.Error(err)
			return false
		}
		if len(batch.Data) > 1 {
			c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{Success: false, Message: noneWritten})
			return true
		}
		c.send(request, hoarddbwire.OpBatchPutOK, hoarddbwire.BatchPutResponse{IDs: []string{"k"}, Count: 1, Success: true})
		return true
	}))
	client := mustConnect(t, server.uri())
	docs := []any{hoarddb.Map{"N": 1}, hoarddb.Map{"N": 2}, hoarddb.Map{"N": 3}, hoarddb.Map{"N": 4}}
	receipt, err := client.Database("app").Bucket("users").InsertMany(testContext(t), docs, hoarddb.WithWriteConcern(hoarddb.All))
	if err != nil || receipt.Count != len(docs) {
		t.Fatalf("InsertMany = %+v, %v; want the four documents stored in halves", receipt, err)
	}
	sent := recorder.requests()
	if len(sent) != 7 { // 4, then 2 and 2, then four of 1
		t.Fatalf("%d BATCH_PUTs were sent, want 7 (one refused whole, two refused halves, four singles)", len(sent))
	}
	for index, request := range sent {
		if got, _ := request.members["writeconcern"].(string); request.op != hoarddbwire.OpBatchPut || got != "all" {
			t.Errorf("request %d: %s with writeconcern %q; want BATCH_PUT at \"all\"", index+1, request.op, got)
		}
	}
}

// An ordinary ERROR frame on a data write that carries no write_outcome stamp
// is OutcomeUnknown, with the driver's unknown-outcome advice after the
// server's sentence, for every data write verb (handbook/errors.md section 4:
// absent means treat as unknown). A stamped refusal is passed through
// untouched, and so is an unstamped read or DDL answer: the rule is for writes
// whose fate the server did not say.
func TestUnstampedWriteRefusalIsUnknownAndStampedIsUntouched(t *testing.T) {
	const sentence = "the store could not apply this"
	for _, verb := range everyWriteVerb {
		for _, tc := range []struct {
			name    string
			members map[string]any
			want    hoarddb.WriteOutcome
			advice  bool
		}{
			{"unstamped", map[string]any{"code": int32(5), "message": sentence}, hoarddb.OutcomeUnknown, true},
			{"not_applied", map[string]any{"code": int32(5), "message": sentence, "write_outcome": "not_applied"}, hoarddb.OutcomeNotApplied, false},
			{"applied", map[string]any{"code": int32(5), "message": sentence, "write_outcome": "applied"}, hoarddb.OutcomeApplied, false},
			{"unknown", map[string]any{"code": int32(5), "message": sentence, "write_outcome": "unknown"}, hoarddb.OutcomeUnknown, false},
		} {
			t.Run(verb.name+"/"+tc.name, func(t *testing.T) {
				var log frameLog
				server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
					log.add(request.Op)
					c.errorWith(request, tc.members)
					return true
				}))
				client := mustConnect(t, server.uri())
				var failure *hoarddb.Error
				if err := verb.call(testContext(t), client.Database("app")); !errors.As(err, &failure) {
					t.Fatalf("%s = %v; want an *Error", verb.name, err)
				}
				if failure.Outcome != tc.want {
					t.Errorf("Outcome = %v, want %v", failure.Outcome, tc.want)
				}
				// A pop words its own advice: how many entries it may have removed
				// (handbook/api-go.md section 7). Its applied refusal, which carries
				// no entries, says so after the server's sentence.
				pop := verb.opcode == hoarddbwire.OpFIFOPop || verb.opcode == hoarddbwire.OpFieldPop
				adviceText := "Whether the write was applied is unknown"
				if pop {
					adviceText = "outcome unknown: the pop may have removed up to 1 entries; run peek to see what is left"
				}
				if got := strings.Contains(failure.Message, adviceText); got != tc.advice {
					t.Errorf("advice present = %t, want %t: %q", got, tc.advice, failure.Message)
				}
				if pop && tc.name == "applied" {
					if want := "its answer carried no entries: it may have removed up to 1 entries; run peek to see what is left"; !strings.Contains(failure.Message, want) {
						t.Errorf("message = %q; want an applied pop to say %q", failure.Message, want)
					}
					if !strings.HasPrefix(failure.Message, sentence) {
						t.Errorf("message = %q; want the server's sentence first", failure.Message)
					}
					return
				}
				// InsertMany says first which documents were in flight when the outcome is
				// unknown, so the server's sentence follows its account there.
				accounted := verb.opcode == hoarddbwire.OpBatchPut && tc.want == hoarddb.OutcomeUnknown
				if accounted && !strings.Contains(failure.Message, sentence) {
					t.Errorf("message = %q; want the server's sentence in it", failure.Message)
				} else if !accounted && (!strings.HasPrefix(failure.Message, sentence) || (!tc.advice && failure.Message != sentence)) {
					t.Errorf("message = %q; want the server's sentence first and untouched when stamped", failure.Message)
				}
				if sent := log.count(verb.opcode); sent != 1 {
					t.Errorf("%s sent %d times, want 1: an unknown outcome is never resent", verb.opcode, sent)
				}
			})
		}
	}
}

// The same unstamped answer to a read or a DDL request is not a write whose
// fate is unknown: it stays not reported, with the server's sentence verbatim.
func TestUnstampedReadAndDDLRefusalsStayNotReported(t *testing.T) {
	const sentence = "the store could not answer this"
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorFrame(request, 0x05, sentence)
		return true
	}))
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	for name, call := range map[string]func() error{
		"Get":            func() error { _, err := client.Database("app").Bucket("users").Get(ctx, "ada"); return err },
		"CreateDatabase": func() error { return client.CreateDatabase(ctx, "app") },
		"DropBucket":     func() error { return client.Database("app").DropBucket(ctx, "users") },
	} {
		var failure *hoarddb.Error
		if err := call(); !errors.As(err, &failure) || failure.Outcome != hoarddb.OutcomeNotReported || failure.Message != sentence {
			t.Errorf("%s = %+v; want not reported and the server's sentence verbatim", name, failure)
		}
	}
}
