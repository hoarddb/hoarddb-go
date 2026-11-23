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

package servertest_test

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

// These tests run a real server; without HOARDDB_SERVER_BIN they fail, naming
// scripts/fetch-server.sh (and are skipped only under -short).

type writeRequest struct {
	Database     string         `bson:"database"`
	Bucket       string         `bson:"bucket"`
	Key          string         `bson:"key"`
	Data         map[string]any `bson:"data"`
	WriteConcern string         `bson:"writeconcern"`
}

type writeReply struct {
	Success           bool   `bson:"success"`
	Message           string `bson:"message"`
	CopiesConfirmed   int    `bson:"copies_confirmed"`
	ReplicationFactor int    `bson:"replication_factor"`
}

type readRequest struct {
	Database string `bson:"database"`
	Bucket   string `bson:"bucket"`
	Key      string `bson:"key"`
}

type readReply struct {
	Found bool           `bson:"found"`
	Data  map[string]any `bson:"data"`
}

type createDatabaseRequest struct {
	Name string `bson:"name"`
}

type createBucketRequest struct {
	Database string `bson:"database"`
	Name     string `bson:"name"`
	Type     string `bson:"type"`
}

func context30(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestStartServesPing(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx := context30(t)

	config, err := trust.NewTLSConfig(server.Address, trust.Policy{Pin: server.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := hoarddbwire.Dial(ctx, server.Address, config, hoarddbwire.DialOptions{})
	if err != nil {
		t.Fatalf("dialing the started server by its logged fingerprint %q: %v", server.Fingerprint, err)
	}
	defer func() { _ = conn.Close() }()
	reply, err := conn.Do(ctx, hoarddbwire.OpPing, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Op != hoarddbwire.OpPong {
		t.Fatalf("PING answered %v, want PONG", reply.Op)
	}
	log, err := os.ReadFile(server.LogPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(log), `"fingerprint":"`+server.Fingerprint+`"`) {
		t.Fatalf("the server's log does not carry the fingerprint %q", server.Fingerprint)
	}
}

func TestTwoServersStartSideBySide(t *testing.T) {
	// With metrics on, a shared default port is the collision this guards
	// against: the second server would log "address already in use".
	metrics := servertest.Options{Env: map[string]string{"HOARDDB_METRICS_ENABLED": "true"}}
	first := servertest.Start(t, metrics)
	second := servertest.Start(t, metrics)
	if first.Address == second.Address {
		t.Fatalf("both servers listen on %s", first.Address)
	}
	ctx := context30(t)
	for _, server := range []*servertest.Server{first, second} {
		conn, err := server.Dial(ctx)
		if err != nil {
			t.Fatalf("authenticating to %s: %v", server.Address, err)
		}
		_ = conn.Close()

		deadline := time.Now().Add(10 * time.Second)
		for {
			log, err := os.ReadFile(server.LogPath)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(log), "address already in use") {
				t.Fatalf("server %s could not bind a port:\n%s", server.Address, log)
			}
			if strings.Contains(string(log), "metrics HTTP server listening") {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("server %s never logged its metrics listener:\n%s", server.Address, log)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestClusterFormsAndReplicates(t *testing.T) {
	cluster := servertest.StartCluster(t, 3, servertest.Options{})
	if got := len(cluster.Addresses()); got != 3 {
		t.Fatalf("the cluster has %d nodes, want 3", got)
	}
	if got := len(cluster.Fingerprints()); got != 3 {
		t.Fatalf("the cluster has %d fingerprints, want 3", got)
	}
	ctx := context30(t)

	writer, err := cluster.Node(0).Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = writer.Close() }()
	if reply, err := writer.Do(ctx, hoarddbwire.OpCreateDB, createDatabaseRequest{Name: "plumbing"}, nil); err != nil || reply.Op == hoarddbwire.OpError {
		t.Fatalf("create database: %v %v", reply.Op, err)
	}
	if reply, err := writer.Do(ctx, hoarddbwire.OpCreateBucket, createBucketRequest{Database: "plumbing", Name: "users", Type: "hash"}, nil); err != nil || reply.Op == hoarddbwire.OpError {
		t.Fatalf("create bucket: %v %v", reply.Op, err)
	}

	var written writeReply
	reply, err := writer.Do(ctx, hoarddbwire.OpPut, writeRequest{
		Database: "plumbing", Bucket: "users", Key: "ada",
		Data: map[string]any{"Name": "Ada"}, WriteConcern: "all",
	}, &written)
	if err != nil || reply.Op != hoarddbwire.OpPut {
		t.Fatalf("put with write concern all: op %v, err %v", reply.Op, err)
	}
	if written.CopiesConfirmed != 3 || written.ReplicationFactor != 3 {
		t.Fatalf("copies_confirmed %d of replication_factor %d, want 3 of 3", written.CopiesConfirmed, written.ReplicationFactor)
	}

	reader, err := cluster.Node(1).Dial(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	var read readReply
	if _, err := reader.Do(ctx, hoarddbwire.OpGet, readRequest{Database: "plumbing", Bucket: "users", Key: "ada"}, &read); err != nil {
		t.Fatal(err)
	}
	if !read.Found || read.Data["Name"] != "Ada" {
		t.Fatalf("node 2 read found=%v data=%v, want the document written on node 1", read.Found, read.Data)
	}
}

// recorder stands in for the *testing.T Binary is given, to see how it ends.
type recorder struct {
	testing.TB
	failed, skipped bool
	message         string
}

func (r *recorder) Helper() {}

func (r *recorder) Fatalf(format string, args ...any) {
	r.failed, r.message = true, fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (r *recorder) Fatal(args ...any) { r.Fatalf("%s", fmt.Sprint(args...)) }

func (r *recorder) Skipf(format string, args ...any) {
	r.skipped, r.message = true, fmt.Sprintf(format, args...)
	runtime.Goexit()
}

func (r *recorder) Skip(args ...any) { r.Skipf("%s", fmt.Sprint(args...)) }

func TestMissingBinaryFailsLoudly(t *testing.T) {
	t.Setenv("HOARDDB_SERVER_BIN", "")
	seen := &recorder{TB: t}
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		servertest.Binary(seen)
	}()
	<-finished

	if testing.Short() {
		if !seen.skipped || seen.failed {
			t.Fatalf("under -short, Binary without HOARDDB_SERVER_BIN: skipped=%v failed=%v, want a skip", seen.skipped, seen.failed)
		}
		return
	}
	if seen.skipped {
		t.Fatalf("Binary skipped (%q): a green run that tested nothing; it must fail", seen.message)
	}
	if !seen.failed || !strings.Contains(seen.message, "run scripts/fetch-server.sh and export HOARDDB_SERVER_BIN") {
		t.Fatalf("Binary: failed=%v message=%q, want a failure naming the fetch script", seen.failed, seen.message)
	}
}
