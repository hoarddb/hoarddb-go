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
	"sync/atomic"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Component tests of the query verbs against the in-process fake server, for
// what a real server cannot be made to do on demand: advertise no capability.

func TestConsistentGetNeedsTheCapability(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFramesWith([]string{"auth_token", "get_fast"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.errorFrame(request, int32(hoarddbwire.CodeInternal), "the driver should not have sent this")
		return true
	}))
	client := mustConnect(t, server.uri())
	doc, err := client.Database("app").Bucket("users").Get(testContext(t), "ada", hoarddb.Consistent())
	if !errors.Is(err, hoarddb.ErrUnsupportedByServer) || doc != nil {
		t.Fatalf("Get(Consistent) = %s, %v; want ErrUnsupportedByServer and no document", doc, err)
	}
	for _, want := range []string{"consistent_get", "nothing was sent"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the refusal %q should mention %q", err, want)
		}
	}
	if frames.Load() != 0 {
		t.Fatalf("%d frames reached the server; a refused call sends none", frames.Load())
	}
}

func TestConsistentGetIsSentWhenTheCapabilityIsAdvertised(t *testing.T) {
	var opcodes []hoarddbwire.Opcode
	server := startFake(t, serveFramesWith([]string{"consistent_get"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		opcodes = append(opcodes, request.Op)
		doc, err := marshal(hoarddb.Map{"Name": "Ada", "_id_": "ada"})
		if err != nil {
			t.Error(err)
			return false
		}
		c.send(request, hoarddbwire.OpConsistentGet, hoarddbwire.ConsistentGetResponse{Found: true, Data: doc, Version: 3})
		return true
	}))
	client := mustConnect(t, server.uri())
	doc, err := client.Database("app").Bucket("users").Get(testContext(t), "ada", hoarddb.Consistent())
	if err != nil || stringField(t, doc, "Name") != "Ada" {
		t.Fatalf("Get(Consistent) = %s, %v", doc, err)
	}
	if len(opcodes) != 1 || opcodes[0] != hoarddbwire.OpConsistentGet {
		t.Fatalf("sent %v, want one CONSISTENT_GET", opcodes)
	}
}

// An in-band found:false with a sentence the vectors do not pin as an absence
// is a refusal, passed through with the server's words, not a miss.
func TestConsistentGetOnAnUnsupportedStoreIsARefusalNotAMiss(t *testing.T) {
	server := startFake(t, serveFramesWith([]string{"consistent_get"}, func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpConsistentGet, hoarddbwire.ConsistentGetResponse{Message: "consistent read is not supported for bucket type blob"})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("files").Get(testContext(t), "a", hoarddb.Consistent())
	if !errors.Is(err, hoarddb.ErrInvalidArgument) || errors.Is(err, hoarddb.ErrNotFound) || !strings.Contains(err.Error(), "not supported for bucket type blob") {
		t.Fatalf("Get(Consistent) = %v; want the server's refusal as ErrInvalidArgument, not ErrNotFound", err)
	}
}

// A cursor whose server says has_more but returns no document must end, not
// ask the same page for ever.
func TestFindEndsWhenAPageIsEmptyEvenIfTheServerSaysMore(t *testing.T) {
	var scans atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		scans.Add(1)
		c.send(request, hoarddbwire.OpScan, hoarddbwire.ScanResponse{Found: true, HasMore: true, Total: 5})
		return true
	}))
	client := mustConnect(t, server.uri())
	ctx := testContext(t)
	cursor, err := client.Database("app").Bucket("people").Find(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if cursor.Next(ctx) || cursor.Err() != nil {
		t.Fatalf("Next = true or Err = %v; an empty page ends the cursor", cursor.Err())
	}
	if scans.Load() != 1 {
		t.Fatalf("%d SCANs, want 1", scans.Load())
	}
}

// A server that reports no retained_bytes must not make the message claim the
// query held "at least 0 bytes".
func TestResultTooLargeMessageOmitsAnUnreportedRetainedSize(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.errorWith(request, map[string]any{"code": int32(hoarddbwire.CodeResultTooLarge), "message": "too big", "max_result_bytes": int64(1048576)})
		return true
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("people").Find(testContext(t), nil, hoarddb.PageSize(1))
	if !errors.Is(err, hoarddb.ErrResultTooLarge) {
		t.Fatalf("Find = %v, want ErrResultTooLarge", err)
	}
	if !strings.Contains(err.Error(), "1048576") || strings.Contains(err.Error(), "at least") {
		t.Fatalf("message %q: want the ceiling and no claim about a retained size", err)
	}
}
