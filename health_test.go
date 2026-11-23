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
	"reflect"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestHealthDecodesStoreRefusal answers HEALTH_CHECK the way a node does
// while its store refuses writes (handbook/operations.md section 4: the
// optional store_refusal member, healthy false) and checks that the real
// Client.Health decode path accepts it and surfaces every field. The test
// server never refuses writes, so no integration test reaches this key.
func TestHealthDecodesStoreRefusal(t *testing.T) {
	const reason = "disk is 97% full; writes are refused until space is freed"
	cases := []struct {
		name    string
		answer  map[string]any
		healthy bool
		refusal string
	}{
		{
			name: "store refusing writes",
			answer: map[string]any{
				"nodeid": "node1", "uptime": "3h2m", "version": "1.2.3", "bucketcount": int64(7),
				"healthy": false, "membership": "confirmed", "store_refusal": reason,
			},
			healthy: false, refusal: reason,
		},
		{
			name: "store accepting writes carries no store_refusal",
			answer: map[string]any{
				"nodeid": "node1", "uptime": "3h2m", "version": "1.2.3", "bucketcount": int64(7),
				"healthy": true, "membership": "confirmed",
			},
			healthy: true, refusal: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				if request.Op != hoarddbwire.OpHealthCheck {
					c.errorFrame(request, 0x01, "unexpected opcode")
					return false
				}
				c.send(request, hoarddbwire.OpGetResponse, tc.answer)
				return true
			}))
			client := mustConnect(t, server.uri())
			health, err := client.Health(testContext(t))
			if err != nil {
				t.Fatalf("Health: %v", err)
			}
			if health.NodeID != "node1" || health.Uptime != "3h2m" || health.Version != "1.2.3" ||
				health.BucketCount != 7 || health.Membership != "confirmed" {
				t.Fatalf("Health = %+v; known fields not decoded", health)
			}
			if health.Healthy != tc.healthy {
				t.Fatalf("Healthy = %t, want %t", health.Healthy, tc.healthy)
			}
			if health.StoreRefusal != tc.refusal {
				t.Fatalf("StoreRefusal = %q, want %q", health.StoreRefusal, tc.refusal)
			}
		})
	}
}

// TestHealthCarriesTheHostFacts answers HEALTH_CHECK with the host document
// the server added (handbook/operations.md section 4). The health vectors
// leave it out of the comparison because it differs per machine, so nothing
// else reaches Health.Host. A node that cannot read the rest sends cpus alone.
func TestHealthCarriesTheHostFacts(t *testing.T) {
	cases := []struct {
		name   string
		answer map[string]any
		want   *hoarddb.HostFacts
	}{
		{
			name: "every fact",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{
				"cpus": int32(4), "host_cpus": int32(16), "memory_total_bytes": int64(17179869184),
				"memory_used_bytes": int64(1073741824), "memory_source": "cgroup",
				"load": map[string]any{"one_minute": 0.5, "five_minutes": 0.25, "fifteen_minutes": 0.0},
			}},
			want: &hoarddb.HostFacts{CPUs: 4, HostCPUs: 16, MemoryTotalBytes: 17179869184, MemoryUsedBytes: 1073741824,
				MemorySource: "cgroup", Load: &hoarddb.HostLoad{OneMinute: 0.5, FiveMinutes: 0.25}},
		},
		{
			name:   "cpus alone",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{"cpus": int32(2)}},
			want:   &hoarddb.HostFacts{CPUs: 2},
		},
		// A host document that cannot be read costs the facts, never the health call
		// (Capacity and Memtable are handled the same way): Health.Host is nil, or
		// carries what could be read.
		{
			name:   "a host that is a string",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": "not a document"},
			want:   nil,
		},
		{
			name:   "cpus that is not a number",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{"cpus": "four", "host_cpus": int32(8)}},
			want:   nil,
		},
		{
			name:   "cpus that is not an integer",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{"cpus": 2.5}},
			want:   nil,
		},
		{
			name: "a load that is not a document, and memory of the wrong type",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{
				"cpus": int32(2), "load": "heavy", "memory_total_bytes": "lots", "memory_source": int32(7)}},
			want: &hoarddb.HostFacts{CPUs: 2},
		},
		{
			name: "a load with one member of the wrong type",
			answer: map[string]any{"nodeid": "node1", "healthy": true, "host": map[string]any{
				"cpus": int64(2), "load": map[string]any{"one_minute": 0.5, "five_minutes": "x"}}},
			want: &hoarddb.HostFacts{CPUs: 2, Load: &hoarddb.HostLoad{OneMinute: 0.5}},
		},
		{
			name:   "an older server sends no host",
			answer: map[string]any{"nodeid": "node1", "healthy": true},
			want:   nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.send(request, hoarddbwire.OpGetResponse, tc.answer)
				return true
			}))
			client := mustConnect(t, server.uri())
			health, err := client.Health(testContext(t))
			if err != nil {
				t.Fatalf("Health: %v", err)
			}
			if !reflect.DeepEqual(health.Host, tc.want) {
				t.Fatalf("Host = %+v, want %+v", health.Host, tc.want)
			}
		})
	}
}
