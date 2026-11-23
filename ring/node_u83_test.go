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
	"errors"
	"strings"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestU83DescribeNodeReachesTheCaller: [GUARD the request is describe_node with no other
// member, every member of the reply comes back typed, the host part is nil when the server sent
// none, and a refusal (an older server) is the server's *Error].
func TestU83DescribeNodeReachesTheCaller(t *testing.T) {
	host := &ringwire.RingAdminHost{CPUs: 4, HostCPUs: 16, Load: &ringwire.RingAdminLoad{One: 1, Five: 2, Fifteen: 3},
		MemUsed: 5, MemTotal: 9, MemSource: "cgroup", Process: &ringwire.RingAdminProcess{Used: 2, Limit: 8}}
	withHost := true
	older := false
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil || request.Cmd != ringwire.AdminDescribeNode {
			t.Errorf("the request was %+v, %v", request, err)
			return
		}
		if older {
			conn.refuse(frame, ringwire.CodeInvalidArgument, `cmd "describe_node" is not an admin command`)
			return
		}
		node := ringwire.RingAdminNode{NodeID: "node-a", Version: "1.1.0-dev", UptimeMS: 61000, MemberState: "active", Members: 4, Voters: 3,
			Epoch: 7, Leader: "node-b", DefaultRF: 2, Serving: ringwire.ServingNoQuorum}
		if withHost {
			node.Host = host
		}
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, Node: &node})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	got, err := client.DescribeNode(testContext(t))
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "node-a" || got.Version != "1.1.0-dev" || got.UptimeMS != 61000 || got.MemberState != "active" || got.Members != 4 || got.Voters != 3 ||
		got.Epoch != 7 || got.Leader != "node-b" || got.DefaultRF != 2 || got.Serving != "no_quorum" {
		t.Fatalf("DescribeNode gave %+v", got)
	}
	if got.Host == nil || got.Host.CPUs != 4 || got.Host.HostCPUs != 16 || got.Host.Load == nil || *got.Host.Load != (Load{One: 1, Five: 2, Fifteen: 3}) ||
		got.Host.MemUsed != 5 || got.Host.MemTotal != 9 || got.Host.MemSource != "cgroup" || got.Host.Process == nil || *got.Host.Process != (ProcessMemory{Used: 2, Limit: 8}) {
		t.Fatalf("the host part came back as %+v", got.Host)
	}
	withHost = false
	if got, err := client.DescribeNode(testContext(t)); err != nil || got.Host != nil {
		t.Fatalf("a reply without host gave %+v, %v", got.Host, err)
	}
	older = true
	_, err = client.DescribeNode(testContext(t))
	var refusal *Error
	if !errors.As(err, &refusal) || refusal.Code != ringwire.CodeInvalidArgument || !strings.Contains(refusal.Message, "is not an admin command") {
		t.Fatalf("an older server's refusal came back as %v", err)
	}
}

// TestU83AnAdminReplyToAnotherCommandIsRefused: [GUARD a reply whose cmd is not the request's
// is an error naming both, for describe_node and for a sibling command, never a nil dereference].
func TestU83AnAdminReplyToAnotherCommandIsRefused(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: ringwire.AdminShowDatabases, Databases: []ringwire.RingAdminDatabase{}})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.DescribeNode(testContext(t)); err == nil || !strings.Contains(err.Error(), "describe_node") || !strings.Contains(err.Error(), "show_databases") {
		t.Fatalf("DescribeNode on a show_databases reply gave %v", err)
	}
	if _, err := client.ShowBuckets(testContext(t), "app", PageRequest{}); err == nil || !strings.Contains(err.Error(), "show_buckets") {
		t.Fatalf("ShowBuckets on a show_databases reply gave %v", err)
	}
}
