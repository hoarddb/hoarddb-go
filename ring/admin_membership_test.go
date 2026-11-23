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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// membershipVector is the request and response payloads of one row of the driver vectors, so
// the bytes a test expects are the vectors' and not this module's encoder's.
func membershipVector(t *testing.T, name string) (request, response []byte) {
	t.Helper()
	raw, err := os.ReadFile("../testdata/driver-vectors/v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Name    string `json:"name"`
			Request struct {
				PayloadHex string `json:"payload_hex"`
			} `json:"request"`
			Response struct {
				PayloadHex string `json:"payload_hex"`
			} `json:"response"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, row := range file.Vectors {
		if row.Name != name {
			continue
		}
		if request, err = hex.DecodeString(row.Request.PayloadHex); err != nil {
			t.Fatal(err)
		}
		if response, err = hex.DecodeString(row.Response.PayloadHex); err != nil {
			t.Fatal(err)
		}
		if len(request) == 0 || len(response) == 0 {
			t.Fatalf("vector %q has an empty request or response payload", name)
		}
		return request, response
	}
	t.Fatalf("no vector named %q", name)
	return nil, nil
}

// checkMembershipCall runs one membership call against a server that answers with the vector's
// response bytes, and compares the frame the call sent with the vector's request bytes and the
// result it returned with what the vector's response says.
func checkMembershipCall(t *testing.T, vector string, want MembershipResult, call func(ctx context.Context, client *Client) (MembershipResult, error)) {
	t.Helper()
	wantRequest, response := membershipVector(t, vector)
	sent := make(chan hoarddbwire.Frame, 1)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		sent <- frame
		conn.reply(frame, response)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	got, err := call(testContext(t), client)
	if err != nil {
		t.Fatalf("%s: %v", vector, err)
	}
	select {
	case frame := <-sent:
		if frame.Op != hoarddbwire.OpRingAdmin {
			t.Errorf("%s was sent with opcode 0x%02X, want RING_ADMIN 0x%02X", vector, byte(frame.Op), byte(hoarddbwire.OpRingAdmin))
		}
		if !bytes.Equal(frame.Payload, wantRequest) {
			t.Errorf("%s sent\n  %x\nand the vector's request is\n  %x", vector, frame.Payload, wantRequest)
		}
	default:
		t.Fatalf("%s returned %+v and sent no frame", vector, got)
	}
	if got != want {
		t.Errorf("%s returned %+v, want %+v", vector, got, want)
	}
}

// TestLeaveSendsTheVectorBytes: [GUARD Leave sends cmd leave and node_id, nothing else, byte
// for byte the vector's request, and returns the committed position and the server's line].
func TestLeaveSendsTheVectorBytes(t *testing.T) {
	checkMembershipCall(t, "ring admin: leave", MembershipResult{Epoch: 2, StateSeq: 9, Result: "node-b is leaving"},
		func(ctx context.Context, client *Client) (MembershipResult, error) {
			return client.Leave(ctx, "node-b")
		})
}

// TestRemoveNodeSendsTheVectorBytes: [GUARD RemoveNode sends cmd remove_node and node_id,
// nothing else, byte for byte the vector's request, and returns the reply's three members].
func TestRemoveNodeSendsTheVectorBytes(t *testing.T) {
	checkMembershipCall(t, "ring admin: remove_node", MembershipResult{Epoch: 2, StateSeq: 9, Result: "node-b was removed"},
		func(ctx context.Context, client *Client) (MembershipResult, error) {
			return client.RemoveNode(ctx, "node-b")
		})
}

// TestReplaceNodeSendsTheVectorBytes: [GUARD ReplaceNode sends cmd replace_node, node_id and
// data_addr in that order, byte for byte the vector's request, and returns the reply's three
// members].
func TestReplaceNodeSendsTheVectorBytes(t *testing.T) {
	checkMembershipCall(t, "ring admin: replace_node", MembershipResult{Epoch: 2, StateSeq: 9, Result: "node-b2.example:4433 replaces node-b"},
		func(ctx context.Context, client *Client) (MembershipResult, error) {
			return client.ReplaceNode(ctx, "node-b", "node-b2.example:4433")
		})
}
