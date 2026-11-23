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
	"sync/atomic"
	"testing"
	"time"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// These are component tests against the in-process fake server: they cover
// what a real server cannot be made to do on demand (an error frame of a
// chosen shape, a silent server, a wrong answer opcode). The real-server
// tests of the same layer are in ddl_integration_test.go.

func TestErrorFrameBecomesAnErrorWithItsFields(t *testing.T) {
	tests := []struct {
		name        string
		members     map[string]any
		want        error
		wantCode    hoarddb.Code
		wantOutcome hoarddb.WriteOutcome
		wantCopies  int
		wantFactor  int
		wantMax     int64
		wantHeld    int64
	}{
		{name: "not found", members: map[string]any{"code": int32(1), "message": `database "app" not found. Run 'create database app'`},
			want: hoarddb.ErrNotFound, wantCode: hoarddb.CodeNotFound},
		{name: "code zero is never success", members: map[string]any{"code": int32(0), "message": "boom"},
			want: hoarddb.ErrInternal, wantCode: hoarddb.CodeInternal},
		{name: "no code at all is never success", members: map[string]any{"message": "boom"},
			want: hoarddb.ErrInternal, wantCode: hoarddb.CodeInternal},
		{name: "a code from a newer server keeps its number", members: map[string]any{"code": int32(0x41), "message": "new thing"},
			want: hoarddb.ErrInternal, wantCode: 0x41},
		{name: "code as int64", members: map[string]any{"code": int64(2), "message": "exists"},
			want: hoarddb.ErrAlreadyExists, wantCode: hoarddb.CodeAlreadyExists},
		{name: "write concern timeout says the write is stored", members: map[string]any{
			"code": int32(7), "message": "1 of 3 copies confirmed; do not resend a push", "write_outcome": "applied",
			"copies_confirmed": int64(1), "replication_factor": int64(3)},
			want: hoarddb.ErrTimeout, wantCode: hoarddb.CodeTimeout, wantOutcome: hoarddb.OutcomeApplied, wantCopies: 1, wantFactor: 3},
		{name: "unavailable and not applied", members: map[string]any{"code": int32(6), "message": "membership is changing", "write_outcome": "not_applied"},
			want: hoarddb.ErrUnavailable, wantCode: hoarddb.CodeUnavailable, wantOutcome: hoarddb.OutcomeNotApplied},
		{name: "outcome unknown", members: map[string]any{"code": int32(6), "message": "owner changed", "write_outcome": "unknown"},
			want: hoarddb.ErrUnavailable, wantCode: hoarddb.CodeUnavailable, wantOutcome: hoarddb.OutcomeUnknown},
		{name: "result too large carries both numbers", members: map[string]any{
			"code": int32(0x0E), "message": "too big", "max_result_bytes": int64(1000), "retained_bytes": int64(4000)},
			want: hoarddb.ErrResultTooLarge, wantCode: hoarddb.CodeResultTooLarge, wantMax: 1000, wantHeld: 4000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.errorWith(request, test.members)
				return true
			}))
			client := mustConnect(t, server.uri())
			err := client.DoWire(testContext(t), "create database", hoarddbwire.OpCreateDB, hoarddbwire.OpCreateDB,
				hoarddbwire.NameRequest{Name: "app"}, nil)
			if !errors.Is(err, test.want) {
				t.Fatalf("err = %v, want class %v", err, test.want)
			}
			var failure *hoarddb.Error
			if !errors.As(err, &failure) {
				t.Fatalf("err = %#v, want *hoarddb.Error", err)
			}
			message, _ := test.members["message"].(string)
			if failure.Code != test.wantCode || failure.Message != message || failure.Op != "create database" {
				t.Errorf("Code=%v Op=%q Message=%q; want %v, %q and the server's sentence verbatim", failure.Code, failure.Op, failure.Message, test.wantCode, "create database")
			}
			if failure.Outcome != test.wantOutcome || failure.CopiesConfirmed != test.wantCopies || failure.ReplicationFactor != test.wantFactor {
				t.Errorf("Outcome=%v Copies=%d Factor=%d; want %v %d %d", failure.Outcome, failure.CopiesConfirmed, failure.ReplicationFactor,
					test.wantOutcome, test.wantCopies, test.wantFactor)
			}
			if failure.MaxResultBytes != test.wantMax || failure.RetainedBytes != test.wantHeld {
				t.Errorf("MaxResultBytes=%d RetainedBytes=%d; want %d %d", failure.MaxResultBytes, failure.RetainedBytes, test.wantMax, test.wantHeld)
			}
		})
	}
}

func TestOnlyAnswersKnownToLeaveTheConnectionOpenAreReused(t *testing.T) {
	tests := []struct {
		name  string
		code  int32
		reuse bool
	}{
		{"not found", 0x01, true},
		{"already exists", 0x02, true},
		{"result too large", 0x0E, true},
		{"batch too large", 0x11, true},
		{"field type", 0x10, true},
		{"permission denied", 0x03, false},
		{"invalid argument", 0x04, false},
		{"internal", 0x05, false},
		{"unavailable", 0x06, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
				c.errorFrame(request, test.code, "refused")
				return true
			}))
			client := mustConnect(t, server.uri())
			for range 2 {
				if err := client.CreateDatabase(testContext(t), "app"); err == nil {
					t.Fatal("the fake refuses every request")
				}
			}
			wantDials := 2
			if test.reuse {
				wantDials = 1
			}
			if stats := client.PoolStats(); int(stats.Dials) != wantDials {
				t.Fatalf("%d dials after two refused calls, want %d (reuse=%v): %+v", stats.Dials, wantDials, test.reuse, stats)
			}
		})
	}
}

func TestAnswerWithTheWrongOpcodeIsInternalAndTheConnectionIsDropped(t *testing.T) {
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri())
	err := client.CreateDatabase(testContext(t), "app")
	if !errors.Is(err, hoarddb.ErrInternal) || !strings.Contains(err.Error(), "CREATE_DB") || !strings.Contains(err.Error(), "PONG") {
		t.Fatalf("err = %v; want ErrInternal naming the opcode asked and the one answered", err)
	}
	if client.PoolStats().Discarded < 1 {
		t.Fatalf("a connection that answered the wrong opcode must not be reused: %+v", client.PoolStats())
	}
}

func TestRequestLayerTimeoutAndCancellationDropTheConnectionAndTheNextCallWorks(t *testing.T) {
	var silent atomic.Bool
	silent.Store(true)
	release := make(chan struct{})
	defer close(release)
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if silent.Load() {
			<-release
			return false
		}
		c.send(request, hoarddbwire.OpCreateDB, hoarddbwire.AckResponse{Success: true, Message: "created"})
		return true
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithRequestTimeout(150*time.Millisecond))

	started := time.Now()
	answered := make(chan error, 1)
	go func() { answered <- client.CreateDatabase(context.Background(), "app") }()
	var err error
	select {
	case err = <-answered:
	case <-time.After(3 * time.Second):
		t.Fatal("the call is still blocked after 3s: the default request timeout (150ms) was not applied")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v after %v, want context.DeadlineExceeded from the default request timeout", err, time.Since(started))
	}

	cancelled, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	if err := client.CreateDatabase(cancelled, "app"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if client.PoolStats().Discarded < 2 {
		t.Fatalf("each abandoned request must discard its connection, or its late answer lands on the next call: %+v", client.PoolStats())
	}

	silent.Store(false)
	if err := client.CreateDatabase(testContext(t), "app"); err != nil {
		t.Fatalf("the call after an abandoned one failed: %v", err)
	}
}

func TestOversizeRequestIsRefusedLocallyBeforeAnyByteIsSent(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		c.send(request, hoarddbwire.OpCreateDB, hoarddbwire.AckResponse{Success: true})
		return true
	}))
	client := mustConnect(t, server.uri())
	err := client.CreateDatabase(testContext(t), strings.Repeat("a", 33<<20))
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("err = %v, want ErrInvalidArgument", err)
	}
	for _, want := range []string{"33554432", "blob", "create database"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the message must contain %q (the arithmetic and the remedy):\n%v", want, err)
		}
	}
	if err := client.CreateDatabase(testContext(t), "app"); err != nil {
		t.Fatalf("the call after the refusal failed: %v", err)
	}
	if frames.Load() != 1 || client.PoolStats().Dials != 1 {
		t.Fatalf("%d frames over %d dials; the oversize request must not reach the server and must not cost the connection",
			frames.Load(), client.PoolStats().Dials)
	}
}

// The bulk lane is a separate pool: a call waiting for a bulk connection must
// not delay a point call, and the lanes' counters are summed in PoolStats.
func TestBulkLaneDoesNotStarvePoint(t *testing.T) {
	release := make(chan struct{})
	var bulkFrames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		if c.index == 1 { // Connect's connection: the point lane
			c.send(request, hoarddbwire.OpPong, nil)
			return true
		}
		bulkFrames.Add(1)
		<-release
		c.send(request, hoarddbwire.OpPong, nil)
		return true
	}))
	client := mustConnect(t, server.uri(), hoarddb.WithPoolSize(1), hoarddb.WithBulkPoolSize(1))
	// Registered after mustConnect's Close, so it runs first: a failed assertion
	// frees the blocked bulk calls instead of hanging Close behind them.
	var releaseOnce sync.Once
	releaseBulk := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseBulk)

	bulkDone := make(chan error, 2)
	for range 2 {
		go func() { bulkDone <- client.DoOnBulkLane(testContext(t), hoarddbwire.OpPing, hoarddbwire.OpPong) }()
	}
	waitUntil(t, func() bool { return bulkFrames.Load() == 1 && client.PoolStats().Waiting == 1 })

	started := time.Now()
	if err := client.Ping(testContext(t)); err != nil {
		t.Fatalf("a point call behind a saturated bulk lane: %v", err)
	}
	if time.Since(started) > 2*time.Second {
		t.Fatalf("the point call took %v behind a saturated bulk lane", time.Since(started))
	}
	if stats := client.PoolStats(); stats.Open != 2 || stats.InUse != 1 || stats.Waiting != 1 || stats.Dials != 2 {
		t.Fatalf("PoolStats = %+v; want the point and bulk connections summed (2 open, 1 in use, 1 waiting)", stats)
	}

	releaseBulk()
	for range 2 {
		if err := <-bulkDone; err != nil {
			t.Fatalf("bulk call: %v", err)
		}
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.DoOnBulkLane(testContext(t), hoarddbwire.OpPing, hoarddbwire.OpPong); !errors.Is(err, hoarddb.ErrClosed) {
		t.Fatalf("a bulk call after Close: %v, want ErrClosed", err)
	}
}
