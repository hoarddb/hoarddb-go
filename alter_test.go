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
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// alterVector is the part of an ALTER_BUCKET vector these tests read: the
// request and the answer, as the bytes the server's own encoder produced.
type alterVector struct {
	Name    string `json:"name"`
	Request struct {
		PayloadHex string `json:"payload_hex"`
	} `json:"request"`
	Response struct {
		Opcode     uint8  `json:"opcode"`
		PayloadHex string `json:"payload_hex"`
	} `json:"response"`
}

func alterVectors(t *testing.T) map[string]alterVector {
	t.Helper()
	raw, err := os.ReadFile("testdata/driver-vectors/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []alterVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	found := map[string]alterVector{}
	for _, vector := range file.Vectors {
		if strings.HasPrefix(vector.Name, "alter bucket: ") {
			found[strings.TrimPrefix(vector.Name, "alter bucket: ")] = vector
		}
	}
	// The vectors grow: a row added by a later server sync must not break this
	// file, but a row these tests rely on going missing must.
	for _, name := range alterRowsRelied {
		if _, present := found[name]; !present {
			t.Fatalf("the vectors lack the alter bucket row %q these tests are built on (%d alter rows found)", name, len(found))
		}
	}
	return found
}

// alterRowsRelied are the alter bucket rows every test run needs. The rows
// "missing bucket" and "on a standalone node" came with the vector sync of
// server commit fbd5f302 and are read through alterRowAfterSync.
var alterRowsRelied = []string{
	"raise the factor", "lower the factor", "declare the inherited factor", "no change", "refused in-band", "refused, retry later",
}

// alterRowAfterSync returns a row the sync of fbd5f302 added, and skips the
// test when the vectors on this branch predate it (the row is then in
// testdata only after that sync is merged).
func alterRowAfterSync(t *testing.T, name string) alterVector {
	t.Helper()
	vector, present := alterVectors(t)[name]
	if !present {
		t.Skipf("the vectors on this branch have no alter bucket row %q: it arrives with the vector sync of server commit fbd5f302", name)
	}
	return vector
}

func decodeHex(t *testing.T, text string) []byte {
	t.Helper()
	decoded, err := hex.DecodeString(text)
	if err != nil {
		t.Fatal(err)
	}
	return decoded
}

// serveAlter answers every ALTER_BUCKET with the vector's recorded answer
// frame, after handing the decoded request to seen.
func serveAlter(vector alterVector, capabilities []string, seen func(hoarddbwire.AlterBucketRequest, int)) func(*fakeConn) {
	return serveFramesWith(capabilities, func(c *fakeConn, request hoarddbwire.Frame, count int) bool {
		var decoded hoarddbwire.AlterBucketRequest
		if request.Op != hoarddbwire.OpAlterBucket || unmarshal(request.Payload, &decoded) != nil {
			c.errorFrame(request, 0x05, "unexpected request")
			return true
		}
		seen(decoded, count)
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{
			Op: hoarddbwire.Opcode(vector.Response.Opcode), ID: request.ID, Payload: decodeHex(c.t, vector.Response.PayloadHex)})
		return true
	})
}

func TestAlterBucketAnswersTheVectors(t *testing.T) {
	vectors := alterVectors(t)
	cases := []struct {
		vector string
		factor int
		want   hoarddb.AlterBucketResult
	}{
		{"raise the factor", 3, hoarddb.AlterBucketResult{ReplicationFactor: 3, PreviousFactor: 1, Pending: []string{"node2", "node3"}}},
		{"lower the factor", 2, hoarddb.AlterBucketResult{ReplicationFactor: 2, PreviousFactor: 3, Releasing: []string{"node2"}}},
		{"declare the inherited factor", 3, hoarddb.AlterBucketResult{ReplicationFactor: 3, PreviousFactor: 3}},
		{"no change", 2, hoarddb.AlterBucketResult{ReplicationFactor: 2, PreviousFactor: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.vector, func(t *testing.T) {
			vector := vectors[tc.vector]
			var sent atomic.Pointer[hoarddbwire.AlterBucketRequest]
			server := startFake(t, serveAlter(vector, []string{"auth_token", "bucket_rf"}, func(request hoarddbwire.AlterBucketRequest, _ int) {
				sent.Store(&request)
			}))
			client := mustConnect(t, server.uri())

			got, err := client.Database("v245").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: tc.factor})
			if err != nil {
				t.Fatal(err)
			}

			var wantRequest hoarddbwire.AlterBucketRequest
			if err := unmarshal(decodeHex(t, vector.Request.PayloadHex), &wantRequest); err != nil {
				t.Fatal(err)
			}
			wantRequest.Database = "v245" // the vector's own database name is one per exchange
			if request := sent.Load(); request == nil || *request != wantRequest {
				t.Fatalf("request on the wire = %+v, the vector sends %+v", request, wantRequest)
			}
			var answer hoarddbwire.AlterBucketResponse
			if err := unmarshal(decodeHex(t, vector.Response.PayloadHex), &answer); err != nil {
				t.Fatal(err)
			}
			tc.want.Message = answer.Message
			if !reflect.DeepEqual(*got, tc.want) {
				t.Fatalf("result = %+v, want %+v", *got, tc.want)
			}
		})
	}
}

func TestAlterBucketSendsNothingButTheThreeMembers(t *testing.T) {
	vector := alterVectors(t)["no change"]
	var payload atomic.Pointer[[]byte]
	server := startFake(t, serveFramesWith([]string{"auth_token", "bucket_rf"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		payload.Store(&request.Payload)
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{Op: hoarddbwire.OpAlterBucket, ID: request.ID, Payload: decodeHex(t, vector.Response.PayloadHex)})
		return true
	}))
	client := mustConnect(t, server.uri())
	if _, err := client.Database("app").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: 2}); err != nil {
		t.Fatal(err)
	}
	var members map[string]any
	if err := unmarshal(*payload.Load(), &members); err != nil {
		t.Fatal(err)
	}
	if len(members) != 3 || members["database"] != "app" || members["name"] != "users" || members["replication_factor"] == nil {
		t.Fatalf("the request carries %v; a driver sends database, name and replication_factor and no other member", members)
	}
}

// An in-band refusal is the same answer until an operator acts: an
// invalid-argument error carrying the server's sentence, sent once.
func TestAlterBucketRefusedInBandIsNotRetried(t *testing.T) {
	vector := alterVectors(t)["refused in-band"]
	var requests atomic.Int32
	server := startFake(t, serveAlter(vector, []string{"auth_token", "bucket_rf"}, func(hoarddbwire.AlterBucketRequest, int) { requests.Add(1) }))
	client := mustConnect(t, server.uri())

	got, err := client.Database("app").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: 4})
	if got != nil {
		t.Fatalf("a refused alter returned a result: %+v", *got)
	}
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) {
		t.Fatalf("err = %v, want an invalid-argument *Error", err)
	}
	var answer hoarddbwire.AlterBucketResponse
	if err := unmarshal(decodeHex(t, vector.Response.PayloadHex), &answer); err != nil {
		t.Fatal(err)
	}
	if failure.Message != answer.Message || failure.Op != "alter bucket" || failure.Target != "app/users" {
		t.Fatalf("error = %+v, want the server's sentence for alter bucket app/users", failure)
	}
	if requests.Load() != 1 {
		t.Fatalf("the refusal was sent %d times; an in-band refusal is not retried", requests.Load())
	}
}

// UNAVAILABLE with write_outcome not_applied is the refusal a retry clears
// (handbook/errors.md section 6): the driver backs off and sends it again.
func TestAlterBucketRetriesWhatARetryClears(t *testing.T) {
	vectors := alterVectors(t)
	refused, accepted := vectors["refused, retry later"], vectors["lower the factor"]
	var requests atomic.Int32
	server := startFake(t, serveFramesWith([]string{"auth_token", "bucket_rf"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		answer := refused
		if requests.Add(1) > 2 {
			answer = accepted
		}
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{
			Op: hoarddbwire.Opcode(answer.Response.Opcode), ID: request.ID, Payload: decodeHex(t, answer.Response.PayloadHex)})
		return false // the server closes the connection after an UNAVAILABLE
	}))
	client := mustConnect(t, server.uri())

	got, err := client.Database("app").AlterBucket(testContext(t), "orders002", hoarddb.AlterBucketOptions{ReplicationFactor: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got.ReplicationFactor != 2 || got.PreviousFactor != 3 || !reflect.DeepEqual(got.Releasing, []string{"node2"}) {
		t.Fatalf("result = %+v", *got)
	}
	if requests.Load() != 3 {
		t.Fatalf("the server saw %d requests, want 3 (two refusals, then the answer)", requests.Load())
	}
}

// When the caller's deadline ends the waiting, the last refusal comes back
// verbatim, still marked as not applied, so the caller may try later.
func TestAlterBucketRetryLaterSurfacesWhenTheDeadlineEndsTheWaiting(t *testing.T) {
	vector := alterVectors(t)["refused, retry later"]
	server := startFake(t, serveFramesWith([]string{"auth_token", "bucket_rf"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{Op: hoarddbwire.OpError, ID: request.ID, Payload: decodeHex(t, vector.Response.PayloadHex)})
		return false
	}))
	client := mustConnect(t, server.uri())
	ctx, cancel := context.WithTimeout(testContext(t), 1500*time.Millisecond)
	defer cancel()

	_, err := client.Database("app").AlterBucket(ctx, "orders002", hoarddb.AlterBucketOptions{ReplicationFactor: 2})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrUnavailable) || !errors.As(err, &failure) {
		t.Fatalf("err = %v, want an unavailable *Error", err)
	}
	if failure.Outcome != hoarddb.OutcomeNotApplied {
		t.Fatalf("outcome = %v, want not applied: nothing changed and the retry is safe", failure.Outcome)
	}
	if !strings.Contains(failure.Message, "Run 'show buckets' on node2 and retry") {
		t.Fatalf("message = %q, want the server's sentence verbatim", failure.Message)
	}
}

func TestAlterBucketNeedsTheBucketRFCapability(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFramesWith([]string{"auth_token"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.errorFrame(request, 0x05, "unexpected request")
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: 2})
	if !errors.Is(err, hoarddb.ErrUnsupportedByServer) {
		t.Fatalf("err = %v, want ErrUnsupportedByServer", err)
	}
	if !strings.Contains(err.Error(), "bucket_rf") {
		t.Fatalf("err = %q, want it to name the capability", err)
	}
	if frames.Load() != 0 {
		t.Fatalf("the driver sent %d frames; nothing may be sent to a server without the capability", frames.Load())
	}
}

func TestAlterBucketRefusesAFactorBelowOneBeforeSending(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFramesWith([]string{"auth_token", "bucket_rf"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.errorFrame(request, 0x05, "unexpected request")
		return true
	}))
	client := mustConnect(t, server.uri())
	for _, factor := range []int{0, -1} {
		_, err := client.Database("app").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: factor})
		if !errors.Is(err, hoarddb.ErrInvalidArgument) {
			t.Fatalf("factor %d: err = %v, want ErrInvalidArgument", factor, err)
		}
		if !strings.Contains(err.Error(), "ReplicationFactor") {
			t.Fatalf("factor %d: err = %q, want it to name the field and the fix", factor, err)
		}
	}
	if frames.Load() != 0 {
		t.Fatalf("the driver sent %d frames for a factor it refuses", frames.Load())
	}
}

// A missing bucket is an ERROR NOT_FOUND with the server's sentence, whichever
// node the call reaches, and it is the same answer next time: sent once.
func TestAlterBucketOfAMissingBucketIsNotFoundAndNotRetried(t *testing.T) {
	vector := alterRowAfterSync(t, "missing bucket")
	var requests atomic.Int32
	server := startFake(t, serveFramesWith([]string{"auth_token", "bucket_rf"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		requests.Add(1)
		_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{Op: hoarddbwire.OpError, ID: request.ID, Payload: decodeHex(t, vector.Response.PayloadHex)})
		return true
	}))
	client := mustConnect(t, server.uri())

	got, err := client.Database("v253").AlterBucket(testContext(t), "ghost001", hoarddb.AlterBucketOptions{ReplicationFactor: 2})
	if got != nil {
		t.Fatalf("a refused alter returned a result: %+v", *got)
	}
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrNotFound) || !errors.As(err, &failure) {
		t.Fatalf("err = %v, want a not-found *Error", err)
	}
	if !strings.Contains(failure.Message, `bucket "ghost001" not found in database`) || failure.Op != "alter bucket" {
		t.Fatalf("error = %+v, want the server's missing-bucket sentence for alter bucket", failure)
	}
	if failure.Outcome != hoarddb.OutcomeNotApplied {
		t.Fatalf("outcome = %v, want not applied: the server stamped it", failure.Outcome)
	}
	if requests.Load() != 1 {
		t.Fatalf("the missing-bucket answer was requested %d times; NOT_FOUND is not retried", requests.Load())
	}
}

// A standalone node answers a raise in-band: an invalid-argument error that
// carries the server's sentence, sent once.
func TestAlterBucketOnAStandaloneNodeIsAnInBandRefusal(t *testing.T) {
	vector := alterRowAfterSync(t, "on a standalone node")
	var requests atomic.Int32
	server := startFake(t, serveAlter(vector, []string{"auth_token", "bucket_rf"}, func(hoarddbwire.AlterBucketRequest, int) { requests.Add(1) }))
	client := mustConnect(t, server.uri())

	_, err := client.Database("v254").AlterBucket(testContext(t), "users", hoarddb.AlterBucketOptions{ReplicationFactor: 2})
	var failure *hoarddb.Error
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || !errors.As(err, &failure) {
		t.Fatalf("err = %v, want an invalid-argument *Error", err)
	}
	var answer hoarddbwire.AlterBucketResponse
	if err := unmarshal(decodeHex(t, vector.Response.PayloadHex), &answer); err != nil {
		t.Fatal(err)
	}
	if failure.Message != answer.Message || !strings.Contains(failure.Message, "standalone") {
		t.Fatalf("message = %q, want the server's standalone sentence verbatim", failure.Message)
	}
	if requests.Load() != 1 {
		t.Fatalf("the refusal was sent %d times; an in-band refusal is not retried", requests.Load())
	}
}
