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
	"context"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// replayGroups are the vector groups leg 3 replays: every group with a
// replayable single-node exchange. The cluster group is replayed on three
// nodes by leg 9 (cluster_test.go), which lists the vectors it leaves out.
var replayGroups = []string{"write", "read", "query", "store-fields", "limits", "wire-names", "ddl", "errors", "auth", "session", "bson"}

// TestLeg3Replay is conformance leg 3 (handbook/testing.md section 2): every
// exchange vector of the replayed groups with replay true, sent to a real
// server by the driver's public API where a verb exists and by its raw wire
// layer otherwise. What the driver put on the wire and what the server
// answered are both held to the vector, as in section 1.1; where a public verb
// was used, the value it returned is held to the vector too.
func TestLeg3Replay(t *testing.T) {
	file := loadVectors(t)
	report := conformance.LegReport{Leg: 3, Name: "replay"}
	var public, raw int
	for _, vector := range file.Vectors {
		if vector.Kind != "exchange" || !slices.Contains(replayGroups, vector.Group) || !vector.Replay {
			continue
		}
		if vector.Topology != "single" {
			continue // a cluster vector is replayed by leg 9, on three nodes
		}
		report.Vectors++
		if vector.Provisional() {
			report.Provisional++
		}
		verb, usesPublicAPI := publicVerb(vector)
		if usesPublicAPI {
			public++
		} else {
			raw++
		}
		t.Run(vector.Group+"/"+vector.Name, func(t *testing.T) {
			var err error
			server, replayed := replayServer(t, file, vector)
			if usesPublicAPI {
				err = replayThroughPublicAPI(t, server, replayed, verb)
			} else {
				err = replayThroughWire(t, server, replayed)
			}
			switch {
			case err == nil && !vector.Provisional():
				report.Passed++
			case err == nil:
				t.Logf("provisional vector (%s) passes", vector.Status)
			case vector.Provisional():
				t.Logf("provisional vector (%s) does not pass, and that is not a failure: %v", vector.Status, err)
			default:
				t.Error(err)
			}
		})
	}
	if report.Vectors == 0 {
		t.Fatal("no vectors ran")
	}
	t.Logf("%s (%d through the public API, %d through the wire layer)", report, public, raw)
}

// replayServer starts a fresh server for the vector (its own database name makes
// a shared one equivalent, but a fresh one is what testing.md section 1.1
// says) and runs the vector's setup on a raw connection.
//
// The lease id of a request or of a later setup step differs from run to run, so
// a vector writes "normalised" for it; it is the id the setup's last claim
// answered. The vector returned has it written in.
func replayServer(t *testing.T, file *conformance.File, vector conformance.Vector) (*servertest.Server, conformance.Vector) {
	t.Helper()
	env := map[string]string{}
	for _, source := range []map[string]string{file.ServerEnv, vector.ServerEnv} {
		for name, value := range source {
			if name != "HOARDDB_ROOT_USER" && name != "HOARDDB_ROOT_PASSWORD" { // servertest owns the root credential
				env[name] = value
			}
		}
	}
	// The vectors' own root password, so a recorded AUTH request is the one the
	// server accepts (the user is admin in both).
	server := servertest.Start(t, servertest.Options{Env: env, Password: file.ServerEnv["HOARDDB_ROOT_PASSWORD"]})
	lease := runSetup(t, server, vector)
	vector.Request.PayloadJSON = withLease(vector.Request.PayloadJSON, lease)
	return server, vector
}

// runSetup sends the vector's setup requests to server on a raw connection and
// returns the lease id its last claim answered, "" when it made none.
func runSetup(t *testing.T, server *servertest.Server, vector conformance.Vector) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := server.Dial(ctx)
	if err != nil {
		t.Fatalf("authenticating the setup connection: %v", err)
	}
	defer func() { _ = conn.Close() }()
	var lease string
	for index, step := range vector.Setup {
		payload, err := codec.FromExtendedJSON(withLease(step.PayloadJSON, lease))
		if err != nil {
			t.Fatalf("setup %d (%s): %v", index+1, step.OpcodeName, err)
		}
		reply, err := conn.RoundTrip(ctx, hoarddbwire.Frame{Op: hoarddbwire.Opcode(step.Opcode), Payload: payload})
		if err != nil {
			t.Fatalf("setup %d (%s): %v", index+1, step.OpcodeName, err)
		}
		if reply.Op == hoarddbwire.OpError {
			t.Fatalf("setup %d (%s) was refused: %s", index+1, step.OpcodeName, hoarddbwire.DecodeError(reply.Payload).Message)
		}
		if hoarddbwire.Opcode(step.Opcode) == hoarddbwire.OpQueueClaim {
			var claimed hoarddbwire.ClaimResponse
			if err := codec.Unmarshal(reply.Payload, &claimed); err != nil {
				t.Fatalf("setup %d (%s): reading the lease id: %v", index+1, step.OpcodeName, err)
			}
			lease = claimed.LeaseID
		}
	}
	return lease
}

// replayThroughPublicAPI sends the vector to server, whose setup has run, with
// a public verb through a proxy that records the frames, then checks the
// frames and the verb's own result.
func replayThroughPublicAPI(t *testing.T, server *servertest.Server, vector conformance.Vector, verb verbCall) error {
	t.Helper()
	proxy := servertest.StartProxy(t, server)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, fmt.Sprintf("hoarddb://%s:%s@%s/?fingerprint=%s",
		server.User, server.Password, proxy.Address, proxy.Fingerprint))
	if err != nil {
		return fmt.Errorf("connecting: %w", err)
	}
	defer func() { _ = client.Close() }()

	request, err := conformance.ParseExtended(vector.Request.PayloadJSON)
	if err != nil {
		return err
	}
	resultErr := verb(ctx, client, requestOf{tree: request}, vector)

	exchanges := proxy.Exchanges()
	if len(exchanges) != 1 {
		return fmt.Errorf("the public verb sent %d data requests, want exactly 1: %v", len(exchanges), exchanges)
	}
	exchange := exchanges[0]
	if !exchange.Answered {
		return errors.New("the server never answered the request the driver sent")
	}
	if int(exchange.Request.Op) != vector.Request.Opcode {
		return fmt.Errorf("the driver sent %s, the vector's request is %s", exchange.Request.Op, vector.Request.OpcodeName)
	}
	if err := checkLiveRequest(vector, exchange.Request.Payload); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	if err := checkLiveAnswer(vector, exchange.Response); err != nil {
		return fmt.Errorf("answer: %w", err)
	}
	if resultErr != nil {
		return fmt.Errorf("the public verb's result: %w", resultErr)
	}
	return nil
}

// replayThroughWire sends the vector's request, encoded from the driver's typed
// request, to server, whose setup has run, on a raw wire connection, and
// checks the answer. It is how a vector
// is replayed when no public verb sends it: another opcode of a family the
// driver sends one member of, an option the API does not offer yet.
func replayThroughWire(t *testing.T, server *servertest.Server, vector conformance.Vector) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dial := server.Dial
	if vector.Connection == "fresh" { // the vector is what a new connection sends first
		dial = server.DialUnauthenticated
	}
	conn, err := dial(ctx)
	if err != nil {
		return fmt.Errorf("connecting (%s): %w", vector.Connection, err)
	}
	defer func() { _ = conn.Close() }()

	payload, err := encodeRequest(vector)
	if err != nil {
		return err
	}
	reply, err := conn.RoundTrip(ctx, hoarddbwire.Frame{Op: hoarddbwire.Opcode(vector.Request.Opcode), Payload: payload})
	if err != nil {
		return fmt.Errorf("sending %s: %w", vector.Request.OpcodeName, err)
	}
	if err := checkLiveRequest(vector, payload); err != nil {
		return fmt.Errorf("request: %w", err)
	}
	if err := checkLiveAnswer(vector, reply); err != nil {
		return fmt.Errorf("answer: %w", err)
	}
	return nil
}

// encodeRequest builds the request the driver would send for the vector: its
// typed request, filled from payload_json and written back as BSON. A request
// with payload_hex and no payload_json is raw bytes on purpose.
func encodeRequest(vector conformance.Vector) ([]byte, error) {
	request := vector.Request
	if len(request.PayloadJSON) == 0 || string(request.PayloadJSON) == "null" {
		return hex.DecodeString(request.PayloadHex)
	}
	typed, known := hoarddbwire.RequestFor(hoarddbwire.Opcode(request.Opcode))
	if !known {
		return nil, fmt.Errorf("the driver has no request type for %s", request.OpcodeName)
	}
	if _, isBinary := typed.(encoding.BinaryMarshaler); isBinary {
		return hex.DecodeString(request.PayloadHex)
	}
	wire, err := codec.FromExtendedJSON(request.PayloadJSON)
	if err != nil {
		return nil, err
	}
	if err := codec.Unmarshal(wire, typed); err != nil {
		return nil, fmt.Errorf("filling the typed request: %w", err)
	}
	return codec.Marshal(typed)
}

// checkLiveRequest holds the bytes the driver sent to the vector: every field
// is in allowed_fields and every non-empty member of payload_json is there
// with an equal value (handbook/testing.md section 1.1).
func checkLiveRequest(vector conformance.Vector, payload []byte) error {
	request := vector.Request
	if len(request.PayloadJSON) == 0 || string(request.PayloadJSON) == "null" {
		if got := hex.EncodeToString(payload); got != request.PayloadHex {
			return fmt.Errorf("sent %s, the vector sends raw bytes %s", got, request.PayloadHex)
		}
		return nil
	}
	want, err := conformance.ParseExtended(request.PayloadJSON)
	if err != nil {
		return err
	}
	got, err := conformance.WireTree(payload)
	if err != nil {
		return fmt.Errorf("the driver sent bytes that are not a BSON document: %w", err)
	}
	for _, name := range conformance.TopLevelNames(got) {
		if !slices.Contains(request.AllowedFields, name) {
			return fmt.Errorf("the driver sent %q, which is not in allowed_fields %v", name, request.AllowedFields)
		}
	}
	shape := conformance.ShapeOf(request.OpcodeName, false)
	if difference := conformance.Diff(shape.Normalise(want), shape.Normalise(got)); difference != "" {
		return fmt.Errorf("what the driver sent differs from payload_json: %s", difference)
	}
	return nil
}

// checkLiveAnswer holds the server's real answer to the vector: the opcode, an
// error's code and outcome, every member of payload_json with an equal value
// except the varying ones (whose presence and BSON type are checked instead),
// and no member the vector does not know.
func checkLiveAnswer(vector conformance.Vector, reply hoarddbwire.Frame) error {
	response := vector.Response
	if int(reply.Op) != response.Opcode {
		return fmt.Errorf("the server answered %s, the vector has %s", reply.Op, response.OpcodeName)
	}
	if len(response.PayloadJSON) == 0 || string(response.PayloadJSON) == "null" {
		return checkRawAnswer(response.PayloadHex, reply.Payload)
	}
	if reply.Op == hoarddbwire.OpError {
		if err := checkLiveError(vector, reply.Payload); err != nil {
			return err
		}
	}
	want, err := conformance.ParseExtended(response.PayloadJSON)
	if err != nil {
		return err
	}
	got, err := conformance.WireTree(reply.Payload)
	if err != nil {
		return fmt.Errorf("the server's answer is not a BSON document: %w", err)
	}
	for _, name := range response.AbsentFields {
		if _, present := got[name]; present {
			return fmt.Errorf("the server's answer carries %q, which absent_fields forbids", name)
		}
	}
	if len(response.Fields) > 0 {
		for _, name := range conformance.TopLevelNames(got) {
			if !slices.Contains(response.Fields, name) {
				return fmt.Errorf("the server's answer carries %q, which is not among the vector's fields %v: the server changed and the vectors did not", name, response.Fields)
			}
		}
	}
	if err := conformance.CheckVary(want, got, response.Vary); err != nil {
		return err
	}
	for _, name := range response.AdditiveFields {
		delete(want, name)
		delete(got, name)
	}
	shape := conformance.ShapeOf(response.OpcodeName, true)
	wantTree, gotTree := shape.Normalise(want), shape.Normalise(got)
	conformance.StripVary(wantTree, response.Vary)
	conformance.StripVary(gotTree, response.Vary)
	if difference := conformance.Diff(wantTree, gotTree); difference != "" {
		return fmt.Errorf("the server's answer differs from payload_json: %s", difference)
	}
	return nil
}

// checkRawAnswer holds an answer the vector keeps as bytes (GET_FAST's compact
// form: a status byte, then the stored document) to those bytes. The server does
// not keep a document's field order (handbook/wire.md section 3), so two runs
// differ in order and not in content: the status and the document's fields are
// compared, not the bytes.
func checkRawAnswer(wantHex string, got []byte) error {
	if hex.EncodeToString(got) == wantHex {
		return nil
	}
	wantBytes, err := hex.DecodeString(wantHex)
	if err != nil {
		return fmt.Errorf("payload_hex: %w", err)
	}
	var want, live hoarddbwire.GetFastResponse
	if want.UnmarshalBinary(wantBytes) != nil || live.UnmarshalBinary(got) != nil {
		return fmt.Errorf("the server answered %x, the vector has %s", got, wantHex)
	}
	if want.Found != live.Found {
		return fmt.Errorf("the server answered found=%v, the vector has found=%v", live.Found, want.Found)
	}
	wantTree, err := conformance.WireTree(want.Document)
	if err != nil {
		return err
	}
	liveTree, err := conformance.WireTree(live.Document)
	if err != nil {
		return fmt.Errorf("the server's document is not BSON: %w", err)
	}
	if difference := conformance.Diff(wantTree, liveTree); difference != "" {
		return fmt.Errorf("the server's document differs from the vector's: %s", difference)
	}
	return nil
}

func checkLiveError(vector conformance.Vector, payload []byte) error {
	expect := vector.Error
	if expect == nil {
		return errors.New("an ERROR answer without an error expectation")
	}
	decoded := hoarddbwire.DecodeError(payload)
	if int(decoded.Code) != expect.Code || decoded.Code.String() != expect.Name {
		return fmt.Errorf("the server answered %s (%d), the vector has %s (%d)", decoded.Code, decoded.Code, expect.Name, expect.Code)
	}
	if string(decoded.WriteOutcome) != expect.WriteOutcome {
		return fmt.Errorf("write outcome %q, the vector has %q", decoded.WriteOutcome, expect.WriteOutcome)
	}
	for _, part := range expect.MessageContains {
		if !strings.Contains(decoded.Message, part) {
			return fmt.Errorf("message %q lacks %q", decoded.Message, part)
		}
	}
	return nil
}

// requestOf reads members of a vector's request.
type requestOf struct{ tree map[string]any }

func (request requestOf) text(name string) string {
	value, _ := request.tree[name].(string)
	return value
}

func (request requestOf) number(name string) int64 {
	switch value := request.tree[name].(type) {
	case json.Number:
		parsed, _ := value.Int64()
		return parsed
	case map[string]any:
		for _, wrapped := range value {
			if text, isText := wrapped.(string); isText {
				var parsed int64
				_, _ = fmt.Sscan(text, &parsed)
				return parsed
			}
		}
	}
	return 0
}

// document is the member as a BSON document, the way the driver is handed one.
func (request requestOf) document(name string) (hoarddb.Document, error) {
	member, present := request.tree[name]
	if !present || member == nil {
		return nil, nil
	}
	text, err := json.Marshal(member)
	if err != nil {
		return nil, err
	}
	encoded, err := codec.FromExtendedJSON(text)
	if err != nil {
		return nil, fmt.Errorf("the request's %s: %w", name, err)
	}
	return encoded, nil
}

// empty reports whether the member is absent or an empty value.
func (request requestOf) empty(name string) bool {
	switch value := request.tree[name].(type) {
	case nil:
		return true
	case string:
		return value == ""
	case []any:
		return len(value) == 0
	case map[string]any:
		return len(value) == 0
	}
	return false
}

// verbCall calls one public verb for a vector and checks the value it returns
// against the vector's answer.
type verbCall func(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error

// publicVerb says which public verb sends the vector, if one does. A vector
// that needs what the API does not offer (a conditional delete, another opcode
// of a family) is replayed on the wire layer, and so is one with a write
// concern word the driver refuses before sending: the vector is the server's
// answer to it, which only the wire layer can still ask for.
func publicVerb(vector conformance.Vector) (verbCall, bool) {
	// A request sent before the connection authenticates, or as raw bytes that
	// are not a document, is one the public API cannot send: it logs in first
	// and encodes what it is given. Leg 5 holds the first through the public
	// API (a write to the same address, with a forged login) and the wire layer
	// holds both here.
	if vector.Connection == "fresh" || len(vector.Request.PayloadJSON) == 0 || string(vector.Request.PayloadJSON) == "null" {
		return nil, false
	}
	request, err := conformance.ParseExtended(vector.Request.PayloadJSON)
	if err != nil {
		return nil, false
	}
	input := requestOf{tree: request}
	if !input.empty("expire_if") {
		return nil, false
	}
	if !input.empty("writeconcern") && (!concernVerbs[vector.Request.OpcodeName] || !knownConcern(input.text("writeconcern"))) {
		return nil, false
	}
	// The driver always asks for a page size of its own, so a find the vector
	// sends without a limit (leaning on the server's default) is replayed on the
	// wire layer, which still holds the request and the live answer to it.
	if vector.Request.OpcodeName == "SCAN" && input.empty("limit") {
		return nil, false
	}
	// A range sent with a limit of 0 (the server's own default) is the same:
	// the driver asks for a page of its own size.
	if vector.Request.OpcodeName == "BTREE_RANGE" && input.number("limit") == 0 {
		return nil, false
	}
	// A pop limit the driver refuses before sending is replayed on the wire layer,
	// for the reason a write concern word it refuses is.
	if vector.Request.OpcodeName == "FIFO_POP" && (input.number("limit") < 0 || input.number("limit") > 100) {
		return nil, false
	}
	if outOfRange(vector, input) {
		return nil, false // the driver refuses it before sending; the wire layer sends it
	}
	verb, known := publicVerbs[vector.Request.OpcodeName]
	if !known {
		return nil, false
	}
	return verb, true
}

var publicVerbs = map[string]verbCall{
	"PUT":       verbPut,
	"GET":       verbGet,
	"EXISTS":    verbExists,
	"DELETE":    verbDelete,
	"UPDATE":    verbUpdate,
	"FIFO_PUSH": verbPush,
	"FIFO_POP":  verbPop,
	"FIFO_PEEK": verbPeek,

	"QUEUE_CLAIM":   verbClaim,
	"QUEUE_ACK":     verbAck,
	"QUEUE_RELEASE": verbRelease,
	"QUEUE_EXTEND":  verbExtend,
	"DEAD_LIST":     verbDeadList,
	"DEAD_REQUEUE":  verbRequeue,
	"DEAD_DISCARD":  verbDiscard,

	"BTREE_RANGE": verbRange,
	"BTREE_FIRST": verbEnd,
	"BTREE_LAST":  verbEnd,

	"SCAN":           verbFind,
	"COUNT":          verbCount,
	"DISTINCT":       verbDistinct,
	"SEARCH":         verbSearch,
	"CONSISTENT_GET": verbConsistentGet,

	"BATCH_PUT": verbInsertMany,

	"UPSERT":       verbUpsert,
	"BATCH_UPSERT": verbUpsertMany,

	"BUCKET_INFO": verbInfo,

	"BLOB_WRITE": verbBlobWrite,
	"BLOB_READ":  verbBlobRead,

	"FIELD_PUSH":   verbFieldPush,
	"FIELD_POP":    verbFieldPop,
	"FIELD_PEEK":   verbFieldPeek,
	"FIELD_PUT":    verbFieldPut,
	"FIELD_GET":    verbFieldGet,
	"FIELD_DELETE": verbFieldDelete,
	"FIELD_LEN":    verbFieldLen,
	"FIELD_LIST":   verbFieldList,
}

// concernVerbs are the public verbs of this file that pass the vector's write
// concern on with concernOptions.
var concernVerbs = map[string]bool{"PUT": true, "UPSERT": true, "DELETE": true, "UPDATE": true, "FIFO_PUSH": true, "FIFO_POP": true}

// knownConcern reports whether the driver sends word as it is: one of the
// public API's write concerns.
func knownConcern(word string) bool {
	return slices.Contains([]hoarddb.WriteConcern{hoarddb.One, hoarddb.Majority, hoarddb.All}, hoarddb.WriteConcern(word))
}

// concernOptions is the vector's write concern as a write option, none when
// the request carries none.
func concernOptions(request requestOf) []hoarddb.WriteOption {
	if request.empty("writeconcern") {
		return nil
	}
	return []hoarddb.WriteOption{hoarddb.WithWriteConcern(hoarddb.WriteConcern(request.text("writeconcern")))}
}

func bucketFor(client *hoarddb.Client, request requestOf) *hoarddb.Bucket {
	return client.Database(request.text("database")).Bucket(request.text("bucket"))
}

// answerOf is the vector's answer as a tree.
func answerOf(vector conformance.Vector) (map[string]any, error) {
	return conformance.ParseExtended(vector.Response.PayloadJSON)
}

// documentMatches shows a document a verb returned equals the vector's member,
// except the varying values (their presence and type are checked instead).
func documentMatches(vector conformance.Vector, member string, got hoarddb.Document) error {
	want, err := answerOf(vector)
	if err != nil {
		return err
	}
	tree, err := conformance.WireTree(got)
	if err != nil {
		return fmt.Errorf("the verb returned bytes that are not a document: %w", err)
	}
	return treesMatch(vector, map[string]any{member: want[member]}, map[string]any{member: tree})
}

func treesMatch(vector conformance.Vector, want, got map[string]any) error {
	if err := conformance.CheckVary(want, got, vector.Response.Vary); err != nil {
		return err
	}
	conformance.StripVary(want, vector.Response.Vary)
	conformance.StripVary(got, vector.Response.Vary)
	if difference := conformance.Diff(want, got); difference != "" {
		return fmt.Errorf("the verb returned a value that differs from the vector: %s", difference)
	}
	return nil
}

func verbPut(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	doc, err := request.document("data")
	if err != nil {
		return err
	}
	var receipt hoarddb.Receipt
	key := request.text("key")
	if key == "" {
		receipt, err = bucketFor(client, request).Insert(ctx, doc, concernOptions(request)...)
	} else {
		receipt, err = bucketFor(client, request).Put(ctx, key, doc, concernOptions(request)...)
	}
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receipt.ID == "" || (key != "" && receipt.ID != key) || receipt.Seq == 0 {
		return fmt.Errorf("receipt %+v: want the key (assigned for an insert) and a sequence number", receipt)
	}
	return copiesMatch(vector, receipt.CopiesConfirmed, receipt.ReplicationFactor)
}

func verbUpsert(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	doc, err := request.document("data")
	if err != nil {
		return err
	}
	key := request.text("key")
	receipt, err := bucketFor(client, request).Upsert(ctx, key, doc, concernOptions(request)...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receipt.ID != key || receipt.Seq == 0 {
		return fmt.Errorf("receipt %+v: want the key and a sequence number", receipt)
	}
	return copiesMatch(vector, receipt.CopiesConfirmed, receipt.ReplicationFactor)
}

// copiesMatch holds a receipt's copy counts to the vector's answer: equal when
// the vector reports them, 0 (not reported) when it does not.
func copiesMatch(vector conformance.Vector, confirmed, factor int) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	reported := requestOf{tree: answer}
	if want := reported.number("copies_confirmed"); int64(confirmed) != want {
		return fmt.Errorf("the receipt says %d copies confirmed, the vector %d", confirmed, want)
	}
	if want := reported.number("replication_factor"); int64(factor) != want {
		return fmt.Errorf("the receipt says a replication factor of %d, the vector %d", factor, want)
	}
	return nil
}

// refusalOf is the message of an in-band answer that refuses the verb (a verb
// the store type does not answer) rather than reporting a miss: the server's
// absence and emptiness messages say nothing is there, a refusal says what the
// store type cannot do ("... not supported for bucket type ..."). Empty for a
// miss.
func refusalOf(answer map[string]any) string {
	message, _ := answer["message"].(string)
	if !strings.Contains(message, "not supported") && !strings.Contains(message, " is not defined for ") {
		return ""
	}
	return message
}

// inBandRefusal holds a verb's result to an in-band refusal: no document, and an
// error that is not a miss and carries the server's sentence verbatim.
func inBandRefusal(verb string, got any, err error, message string) error {
	var failure *hoarddb.Error
	if reflect.ValueOf(got).Len() != 0 || !errors.As(err, &failure) || errors.Is(err, hoarddb.ErrNotFound) ||
		!strings.Contains(failure.Message, message) {
		return fmt.Errorf("%s = %s, %v; the vector is a refusal (%q), which is an error that is not a miss, and no document", verb, got, err, message)
	}
	return nil
}

func verbGet(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Get(ctx, request.text("key"))
	if found, _ := answer["found"].(bool); !found {
		if refusal := refusalOf(answer); refusal != "" {
			return inBandRefusal("Get", got, err, refusal)
		}
		if !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
			return fmt.Errorf("Get = %s, %v; the vector is a miss, which is ErrNotFound and no document", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return documentMatches(vector, "data", got)
}

func verbExists(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Exists(ctx, request.text("key"))
	if refused, problem := refusedAs(vector, err); refused {
		if got {
			return errors.New("Exists answered true with an error")
		}
		return problem
	}
	if want, _ := answer["exists"].(bool); err != nil || got != want {
		return fmt.Errorf("Exists = %v, %v; the vector says %v", got, err, want)
	}
	return nil
}

func verbDelete(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Delete(ctx, request.text("key"), concernOptions(request)...)
	if want, _ := answer["success"].(bool); err != nil || got != want {
		return fmt.Errorf("Delete = %v, %v; the vector says success %v, which is %v and no error", got, err, want, want)
	}
	return nil
}

// updateOf is the vector's update request as the public Update: each operator
// the vector carries goes through the public field of the same name.
func updateOf(request requestOf) (hoarddb.Update, error) {
	update := hoarddb.Update{}
	// A document decoded from extended JSON keeps its widths (an int32 stays an
	// int32), so the answer's types match the vector's.
	documents := []struct {
		wire string
		into any
	}{
		{"set", &update.Set}, {"inc", &update.Inc}, {"push", &update.Push}, {"addtoset", &update.AddToSet},
		{"min", &update.Min}, {"max", &update.Max}, {"mul", &update.Mul}, {"rename", &update.Rename},
	}
	for _, operator := range documents {
		operand, err := request.document(operator.wire)
		if err != nil {
			return update, err
		}
		if operand == nil {
			continue
		}
		if err := operand.Decode(operator.into); err != nil {
			return update, fmt.Errorf("the request's %s as the driver's Update field: %w", operator.wire, err)
		}
	}
	if unset, isList := request.tree["unset"].([]any); isList {
		for _, path := range unset {
			update.Unset = append(update.Unset, fmt.Sprint(path))
		}
	}
	return update, nil
}

func verbUpdate(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	update, err := updateOf(request)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Update(ctx, request.text("key"), update, concernOptions(request)...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if success, _ := answer["success"].(bool); !success {
		if !errors.Is(err, hoarddb.ErrNotFound) || got != nil {
			return fmt.Errorf("Update = %s, %v; the vector refuses a missing key, which is ErrNotFound and no document", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return documentMatches(vector, "doc", got)
}

func verbPush(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	doc, err := request.document("data")
	if err != nil {
		return err
	}
	options := concernOptions(request)
	if priority := request.number("priority"); priority != 0 {
		options = append(options, hoarddb.WithPriority(priority))
	}
	receipt, err := bucketFor(client, request).Push(ctx, doc, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if receipt.ID == "" || receipt.Seq == 0 {
		return fmt.Errorf("receipt %+v: want the assigned key and a sequence number", receipt)
	}
	return nil
}

func verbPop(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	options := []hoarddb.PopOption{}
	for _, option := range concernOptions(request) {
		options = append(options, option)
	}
	if limit := request.number("limit"); limit != 0 {
		options = append(options, hoarddb.WithLimit(int(limit)))
	}
	got, err := bucketFor(client, request).Pop(ctx, options...)
	if found, _ := answer["found"].(bool); !found {
		if refusal := refusalOf(answer); refusal != "" {
			return inBandRefusal("Pop", got, err, refusal)
		}
		if err != nil || len(got) != 0 {
			return fmt.Errorf("Pop = %s, %v; the vector is an empty store, which is an empty list and a nil error", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	return documentsMatch(vector, "items", answer["items"], got)
}

func verbPeek(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Peek(ctx)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if found, _ := answer["found"].(bool); !found {
		if err != nil || len(got) != 0 {
			return fmt.Errorf("Peek = %v, %v; the vector is an empty store, which is an empty list and a nil error", got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	items := make([]any, 0, len(got))
	for _, document := range got {
		tree, err := conformance.WireTree(document)
		if err != nil {
			return err
		}
		items = append(items, tree)
	}
	return treesMatch(vector, map[string]any{"items": answer["items"]}, map[string]any{"items": items})
}

func verbRange(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	options := []hoarddb.FindOption{}
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.Limit(int(limit)))
	}
	cursor, err := bucketFor(client, request).Range(ctx, hoarddb.Bounds{Gte: request.text("low"), Lte: request.text("high")}, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		return err
	}
	// The keys of a range are the documents' own _id_ fields.
	wantEntries, _ := answer["entries"].([]any)
	var wantValues []any
	for _, entry := range wantEntries {
		if fields, isMap := entry.(map[string]any); isMap {
			wantValues = append(wantValues, fields["value"])
		}
	}
	gotValues := make([]any, 0, len(documents))
	for _, document := range documents {
		tree, err := conformance.WireTree(document)
		if err != nil {
			return err
		}
		gotValues = append(gotValues, tree)
	}
	if difference := conformance.Diff(map[string]any{"entries": wantValues}, map[string]any{"entries": gotValues}); difference != "" {
		return fmt.Errorf("the range's documents differ from the vector's entries: %s", difference)
	}
	return nil
}

// verbEnd is First and Last: the vector's one entry, or an empty tree.
func verbEnd(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	bucket := bucketFor(client, request)
	call := bucket.First
	if vector.Request.OpcodeName == "BTREE_LAST" {
		call = bucket.Last
	}
	got, err := call(ctx)
	entries, _ := answer["entries"].([]any)
	if len(entries) == 0 {
		if !errors.Is(err, hoarddb.ErrNotFound) {
			return fmt.Errorf("%s = %+v, %v; the vector is an empty tree, which is ErrNotFound", vector.Request.OpcodeName, got, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	want, _ := entries[0].(map[string]any)
	if key, _ := want["key"].(string); key != got.Key {
		return fmt.Errorf("key %q, the vector has %q", got.Key, key)
	}
	tree, err := conformance.WireTree(got.Value)
	if err != nil {
		return err
	}
	if difference := conformance.Diff(map[string]any{"value": want["value"]}, map[string]any{"value": tree}); difference != "" {
		return fmt.Errorf("the entry's document differs from the vector: %s", difference)
	}
	return nil
}

// findOptions builds the query options a SCAN vector's request stands for.
func findOptions(request requestOf) []hoarddb.FindOption {
	var options []hoarddb.FindOption
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.Limit(int(limit)))
	}
	if skip := request.number("skip"); skip > 0 {
		options = append(options, hoarddb.Skip(int(skip)))
	}
	if sort, isTree := request.tree["sort"].(map[string]any); isTree {
		for field := range sort {
			direction := hoarddb.Ascending
			if (requestOf{tree: sort}).number(field) < 0 {
				direction = hoarddb.Descending
			}
			options = append(options, hoarddb.SortBy(field, direction))
		}
	}
	if projection, isTree := request.tree["projection"].(map[string]any); isTree {
		fields := make([]string, 0, len(projection))
		for field := range projection {
			fields = append(fields, field)
		}
		slices.Sort(fields)
		options = append(options, hoarddb.Projection(fields...))
	}
	return options
}

// filterOf is the request's filter as the driver is handed one: nil when the
// vector has none.
func filterOf(request requestOf) (any, error) {
	filter, err := request.document("filter")
	if err != nil || len(filter) == 0 {
		return nil, err
	}
	return filter, nil
}

// documentsMatch holds the documents a cursor read to the vector's list.
func documentsMatch(vector conformance.Vector, member string, want any, got []hoarddb.Document) error {
	trees := make([]any, 0, len(got))
	for _, document := range got {
		tree, err := conformance.WireTree(document)
		if err != nil {
			return err
		}
		trees = append(trees, tree)
	}
	wanted, _ := want.([]any)
	if wanted == nil {
		wanted = []any{}
	}
	return treesMatch(vector, map[string]any{member: wanted}, map[string]any{member: trees})
}

func verbFind(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	filter, err := filterOf(request)
	if err != nil {
		return err
	}
	cursor, err := bucketFor(client, request).Find(ctx, filter, findOptions(request)...)
	if found, _ := answer["found"].(bool); !found {
		if !errors.Is(err, hoarddb.ErrNotFound) || cursor != nil {
			return fmt.Errorf("Find = %v, %v; the vector is a missing bucket, which is ErrNotFound and no cursor", cursor, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if want := (requestOf{tree: answer}).number("total"); cursor.Total() != want {
		return fmt.Errorf("Total = %d, the vector has %d", cursor.Total(), want)
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		return err
	}
	return documentsMatch(vector, "documents", answer["documents"], documents)
}

func verbCount(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	filter, err := filterOf(request)
	if err != nil {
		return err
	}
	got, err := bucketFor(client, request).Count(ctx, filter)
	if want := (requestOf{tree: answer}).number("count"); err != nil || got != want {
		return fmt.Errorf("Count = %d, %v; the vector says %d", got, err, want)
	}
	return nil
}

func verbDistinct(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	filter, err := filterOf(request)
	if err != nil {
		return err
	}
	values, err := bucketFor(client, request).Distinct(ctx, request.text("field"), filter)
	if err != nil {
		return err
	}
	var got, want []string
	for _, value := range values {
		text, isText := value.AsString()
		if !isText {
			return fmt.Errorf("distinct value %s is not a string; the vector's are", value)
		}
		got = append(got, text)
	}
	wantValues, _ := answer["values"].([]any)
	for _, value := range wantValues {
		want = append(want, fmt.Sprint(value))
	}
	// A set: the server does not promise an order.
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		return fmt.Errorf("Distinct = %v, the vector has %v", got, want)
	}
	return nil
}

func verbSearch(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	filter, err := filterOf(request)
	if err != nil {
		return err
	}
	var options []hoarddb.FindOption
	if limit := request.number("limit"); limit > 0 {
		options = append(options, hoarddb.Limit(int(limit)))
	}
	cursor, err := bucketFor(client, request).Search(ctx, filter, options...)
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	if want := (requestOf{tree: answer}).number("total"); cursor.Total() != want {
		return fmt.Errorf("Total = %d, the vector has %d", cursor.Total(), want)
	}
	if want, _ := answer["has_more"].(bool); cursor.HasMore() != want {
		return fmt.Errorf("HasMore = %t, the vector has %t", cursor.HasMore(), want)
	}
	documents, err := cursor.All(ctx)
	if err != nil {
		return err
	}
	return documentsMatch(vector, "results", answer["results"], documents)
}

func verbConsistentGet(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	got, err := bucketFor(client, request).Get(ctx, request.text("key"), hoarddb.Consistent())
	if refused, problem := refusedAs(vector, err); refused {
		return problem
	}
	if err != nil {
		return err
	}
	return documentMatches(vector, "data", got)
}

// verbInfo is Bucket.Info: the vector's bucket row and store_fields held to the
// declaration it returns, or its ERROR answer to the driver's error.
func verbInfo(ctx context.Context, client *hoarddb.Client, request requestOf, vector conformance.Vector) error {
	got, err := bucketFor(client, request).Info(ctx)
	if refused, problem := refusedAs(vector, err); refused {
		if got != nil {
			return errors.New("Info returned a declaration with an error")
		}
		return problem
	}
	if err != nil {
		return err
	}
	answer, err := answerOf(vector)
	if err != nil {
		return err
	}
	row, _ := answer["bucket"].(map[string]any)
	reported := requestOf{tree: row}
	var stores []string
	names, _ := row["stores"].([]any)
	for _, name := range names {
		stores = append(stores, fmt.Sprint(name))
	}
	var fields []hoarddb.StoreFieldInfo
	declared, _ := answer["store_fields"].([]any)
	for _, entry := range declared {
		field, _ := entry.(map[string]any)
		fields = append(fields, hoarddb.StoreFieldInfo{Name: fmt.Sprint(field["name"]), Engine: hoarddb.StoreType(fmt.Sprint(field["engine"]))})
	}
	inherited, _ := row["inherited"].(bool)
	want := hoarddb.BucketDeclaration{
		Name: reported.text("name"), Type: hoarddb.StoreType(reported.text("type")), Entries: reported.number("entries"),
		Stores: stores, StoreFields: fields, ReplicationFactor: int(reported.number("replication_factor")), Inherited: inherited,
	}
	want.BucketCopies.InSync = int(reported.number("in_sync"))
	want.BucketCopies.State = reported.text("state")
	for _, name := range anyList(row["not_in_sync"]) {
		want.BucketCopies.NotInSync = append(want.BucketCopies.NotInSync, fmt.Sprint(name))
	}
	for _, entry := range anyList(row["pending_copies"]) {
		member, _ := entry.(map[string]any)
		want.BucketCopies.PendingCopies = append(want.BucketCopies.PendingCopies,
			hoarddb.PendingCopy{Node: fmt.Sprint(member["node"]), Since: requestOf{tree: member}.number("since")})
	}
	if !reflect.DeepEqual(*got, want) {
		return fmt.Errorf("Info = %+v; the vector has %+v", *got, want)
	}
	return nil
}

// anyList is a decoded JSON array, nil for any other value.
func anyList(value any) []any {
	list, _ := value.([]any)
	return list
}
