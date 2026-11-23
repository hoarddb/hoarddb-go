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

package hoarddb

import (
	"context"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// Health is one node's answer to a health check.
type Health struct {
	// NodeID is the node's configured ID, Version its build and Uptime how long
	// it has run, in the server's words.
	NodeID, Version, Uptime string
	// BucketCount is how many buckets the node holds.
	BucketCount int64
	// Healthy is the node's own verdict.
	Healthy bool
	// Membership is "confirmed", "not added yet", "membership refused" or
	// "waiting for founders" on a cluster node; a node that is not confirmed
	// refuses requests that name a database or bucket. Empty on a single node.
	Membership string
	// StoreRefusal is why the node's store refuses writes, in the server's
	// words. Empty while the store accepts writes; when set, Healthy is false.
	StoreRefusal string
	// Host is the machine facts the node reported, for display: nothing in the
	// driver or in a caller should decide from them. Nil for a server that sends
	// none.
	Host *HostFacts
}

// HostFacts describes the machine a node runs on (handbook/operations.md
// section 4). CPUs is always present; every other member is zero, or Load is
// nil, when the node cannot read it.
type HostFacts struct {
	// CPUs is how many the node's process may use: a container's CPU quota rounds
	// up and caps it. HostCPUs is the machine's own count, set only when the two
	// differ.
	CPUs, HostCPUs int
	// MemoryTotalBytes is the container's memory limit when one below the
	// machine's is set (MemorySource "cgroup"), else the machine's (MemorySource
	// "host"). MemoryUsedBytes is what is in use within that scope, not counting
	// reclaimable file cache.
	MemoryTotalBytes, MemoryUsedBytes int64
	MemorySource                      string
	// Load is the machine's load average, nil when the node cannot read it. An
	// idle machine's 0.00 is still present.
	Load *HostLoad
}

// HostLoad is the load average of a [HostFacts].
type HostLoad struct {
	OneMinute, FiveMinutes, FifteenMinutes float64
}

// Node is one member of a cluster.
type Node struct {
	// ID is the node's configured ID and Status its state, "active" for a
	// member that serves.
	ID, Status string
	// Address is the node's cluster address, not its driver port: a driver never
	// dials it, so connect to the hosts you were given.
	Address string
	// Fingerprint is the node's key, "SHA256:" and 64 hex characters.
	Fingerprint string
	JoinedAt    time.Time
}

// Health asks the node the client is connected to how it is. The health of
// the node a connection reaches says nothing about the others.
func (c *Client) Health(ctx context.Context) (*Health, error) {
	var answer hoarddbwire.HealthResponse
	// HEALTH_CHECK answers with the GET_RESPONSE opcode (handbook/wire.md
	// section 4).
	err := c.do(ctx, call{op: "health", opcode: hoarddbwire.OpHealthCheck, answer: hoarddbwire.OpGetResponse,
		request: hoarddbwire.EmptyRequest{}, response: &answer})
	if err != nil {
		return nil, err
	}
	return &Health{
		NodeID: answer.NodeID, Version: answer.Version, Uptime: answer.Uptime,
		BucketCount: answer.BucketCount, Healthy: answer.Healthy, Membership: answer.Membership,
		StoreRefusal: answer.StoreRefusal, Host: hostFacts(answer.HostFacts()),
	}, nil
}

// ListNodes lists the members of the cluster the client is connected to. A
// single node answers with no members.
func (c *Client) ListNodes(ctx context.Context) ([]Node, error) {
	var answer hoarddbwire.ListNodesResponse
	err := c.do(ctx, call{op: "list nodes", opcode: hoarddbwire.OpListNodes, answer: hoarddbwire.OpGetResponse,
		response: &answer})
	if err != nil {
		return nil, err
	}
	nodes := make([]Node, 0, len(answer.Nodes))
	for _, entry := range answer.Nodes {
		nodes = append(nodes, Node{ID: entry.NodeID, Status: entry.Status, Address: entry.Addr, Fingerprint: entry.Fingerprint, JoinedAt: entry.JoinedAt})
	}
	return nodes, nil
}

// hostFacts is the public form of the wire's host document, nil when the
// server sent none.
func hostFacts(reported *hoarddbwire.HealthHost) *HostFacts {
	if reported == nil {
		return nil
	}
	facts := &HostFacts{
		CPUs: int(reported.CPUs), HostCPUs: int(reported.HostCPUs),
		MemoryTotalBytes: reported.MemoryTotalBytes, MemoryUsedBytes: reported.MemoryUsedBytes,
		MemorySource: reported.MemorySource,
	}
	if reported.Load != nil {
		facts.Load = &HostLoad{OneMinute: reported.Load.OneMinute, FiveMinutes: reported.Load.FiveMinutes, FifteenMinutes: reported.Load.FifteenMinutes}
	}
	return facts
}
