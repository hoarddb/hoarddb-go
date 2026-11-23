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

package hoarddbwire

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// fakeServer accepts one connection and hands it to script.
type fakeServer struct {
	address string
	done    chan struct{}
}

func startFake(t *testing.T, script func(net.Conn)) *fakeServer {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeServer{address: listener.Addr().String(), done: make(chan struct{})}
	go func() {
		defer close(server.done)
		accepted, err := listener.Accept()
		_ = listener.Close()
		if err != nil {
			return
		}
		defer func() { _ = accepted.Close() }()
		script(accepted)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-server.done
	})
	return server
}

func connect(t *testing.T, server *fakeServer, options ConnOptions) *Conn {
	t.Helper()
	raw, err := net.Dial("tcp", server.address)
	if err != nil {
		t.Fatal(err)
	}
	conn := NewConn(raw, options)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// echoPong answers every request with PONG carrying the request's ID.
func echoPong(connection net.Conn) {
	for {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		if WriteFrame(connection, Frame{Op: OpPong, ID: request.ID, Payload: request.Payload}) != nil {
			return
		}
	}
}

func TestDoRoundTripEchoesIDAndDecodesResponse(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		var login AuthRequest
		if codec.Unmarshal(request.Payload, &login) != nil || request.Op != OpAuth || request.ID.IsZero() {
			return
		}
		reply, _ := codec.Marshal(AuthResponse{Success: true, ServerVersion: login.Username + "-ok"})
		_ = WriteFrame(connection, Frame{Op: OpAuthOK, ID: request.ID, Payload: reply})
	})
	conn := connect(t, server, ConnOptions{})
	var response AuthResponse
	reply, err := conn.Do(context.Background(), OpAuth, AuthRequest{Username: "root"}, &response)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Op != OpAuthOK || !response.Success || response.ServerVersion != "root-ok" {
		t.Fatalf("reply %v, response %+v", reply.Op, response)
	}
}

func TestDoReturnsErrorFramesUnjudged(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		request, _ := ReadFrame(connection)
		payload, _ := codec.Marshal(map[string]any{"code": int32(CodeNotAuthenticated), "message": "send AUTH first"})
		_ = WriteFrame(connection, Frame{Op: OpError, ID: request.ID, Payload: payload})
		echoPong(connection)
	})
	conn := connect(t, server, ConnOptions{})
	var response struct{ Success bool }
	reply, err := conn.Do(context.Background(), OpGet, nil, &response)
	if err != nil {
		t.Fatalf("an OpError reply is the caller's to judge, not a transport error: %v", err)
	}
	if reply.Op != OpError {
		t.Fatalf("reply %v", reply.Op)
	}
	if decoded := DecodeError(reply.Payload); decoded.Code != CodeNotAuthenticated || decoded.Message != "send AUTH first" {
		t.Fatalf("decoded %+v", decoded)
	}
	// NOT_AUTHENTICATED leaves the connection open.
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); err != nil {
		t.Fatalf("the connection must still work: %v", err)
	}
}

func TestOversizedRequestIsRefusedAndConnectionSurvives(t *testing.T) {
	server := startFake(t, echoPong)
	conn := connect(t, server, ConnOptions{})
	_, err := conn.RoundTrip(context.Background(), Frame{Op: OpPut, Payload: make([]byte, MaxPayload+1)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); err != nil {
		t.Fatalf("a refused oversized request must not damage the connection: %v", err)
	}
}

func TestConcurrentRequestsAreSerialisedAtOneInFlight(t *testing.T) {
	var (
		mu       sync.Mutex
		inFlight int
		peak     int
	)
	server := startFake(t, func(connection net.Conn) {
		for {
			request, err := ReadFrame(connection)
			if err != nil {
				return
			}
			mu.Lock()
			inFlight++
			peak = max(peak, inFlight)
			mu.Unlock()
			time.Sleep(time.Millisecond)
			mu.Lock()
			inFlight--
			mu.Unlock()
			if WriteFrame(connection, Frame{Op: OpPong, ID: request.ID, Payload: request.Payload}) != nil {
				return
			}
		}
	})
	conn := connect(t, server, ConnOptions{})
	var group sync.WaitGroup
	for index := range 40 {
		group.Add(1)
		go func() {
			defer group.Done()
			payload := []byte{byte(index)}
			reply, err := conn.RoundTrip(context.Background(), Frame{Op: OpPing, Payload: payload})
			if err != nil {
				t.Errorf("request %d: %v", index, err)
				return
			}
			if len(reply.Payload) != 1 || reply.Payload[0] != byte(index) {
				t.Errorf("request %d got the reply for %v", index, reply.Payload)
			}
		}()
	}
	group.Wait()
	mu.Lock()
	defer mu.Unlock()
	if peak != 1 {
		t.Fatalf("the server saw %d requests in flight at once; the default is 1", peak)
	}
}

// TestDemultiplexesOutOfOrderReplies proves the table works when the window is
// raised: replies come back in the reverse of the order sent.
func TestDemultiplexesOutOfOrderReplies(t *testing.T) {
	const window = 4
	server := startFake(t, func(connection net.Conn) {
		batch := make([]Frame, 0, window)
		for len(batch) < window {
			request, err := ReadFrame(connection)
			if err != nil {
				return
			}
			batch = append(batch, request)
		}
		for index := len(batch) - 1; index >= 0; index-- {
			_ = WriteFrame(connection, Frame{Op: OpPong, ID: batch[index].ID, Payload: batch[index].Payload})
		}
	})
	conn := connect(t, server, ConnOptions{MaxInFlight: window})
	var group sync.WaitGroup
	for index := range window {
		group.Add(1)
		go func() {
			defer group.Done()
			reply, err := conn.RoundTrip(context.Background(), Frame{Op: OpPing, Payload: []byte{byte(index)}})
			if err != nil || len(reply.Payload) != 1 || reply.Payload[0] != byte(index) {
				t.Errorf("request %d: reply %v, err %v", index, reply.Payload, err)
			}
		}()
	}
	group.Wait()
}

func TestUnexpectedResponseTearsTheConnectionDown(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		request, _ := ReadFrame(connection)
		stranger, _ := NewRequestID()
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: stranger})
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: request.ID})
	})
	conn := connect(t, server, ConnOptions{})
	_, err := conn.Do(context.Background(), OpPing, nil, nil)
	if !errors.Is(err, ErrUnexpectedResponse) {
		t.Fatalf("err = %v, want ErrUnexpectedResponse", err)
	}
	select {
	case <-conn.Done():
	case <-time.After(time.Second):
		t.Fatal("a protocol violation must close the connection")
	}
}

func TestZeroIDErrorIsAConnectionRefusal(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		payload, _ := codec.Marshal(map[string]any{"code": int32(CodeOverloaded), "message": "server connection limit reached (1000)"})
		_ = WriteFrame(connection, Frame{Op: OpError, Payload: payload}) // zero request ID
	})
	conn := connect(t, server, ConnOptions{})
	var refused *RefusedError
	_, err := conn.Do(context.Background(), OpAuth, AuthRequest{Username: "root"}, nil)
	if !errors.As(err, &refused) {
		t.Fatalf("err = %v, want *RefusedError (not a broken pipe, not a mismatched ID)", err)
	}
	if refused.Response.Code != CodeOverloaded || refused.Response.Message != "server connection limit reached (1000)" {
		t.Fatalf("refusal = %+v", refused.Response)
	}
	<-conn.Done()
	if !errors.As(conn.Err(), &refused) {
		t.Fatalf("Err() = %v; the refusal should be the connection's recorded cause", conn.Err())
	}
}

func TestServerClosingWhileWaitingFailsTheRequest(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		_, _ = ReadFrame(connection)
		// Return without replying: the deferred Close ends the connection.
	})
	conn := connect(t, server, ConnOptions{})
	_, err := conn.Do(context.Background(), OpPing, nil, nil)
	if err == nil {
		t.Fatal("a connection closed mid-request must fail it")
	}
	select {
	case <-conn.Done():
	default:
		t.Fatal("Done must be closed once the reader sees the end of the stream")
	}
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); err == nil {
		t.Fatal("a dead connection must keep failing, not hang")
	}
}

func TestMalformedFrameFromServerClosesConnection(t *testing.T) {
	server := startFake(t, func(connection net.Conn) {
		_, _ = ReadFrame(connection)
		_, _ = connection.Write([]byte{9, 17, 0, 0, 0}) // version 9
		_, _ = ReadFrame(connection)
	})
	conn := connect(t, server, ConnOptions{})
	_, err := conn.Do(context.Background(), OpPing, nil, nil)
	if !errors.Is(err, ErrBadFrame) {
		t.Fatalf("err = %v, want ErrBadFrame", err)
	}
}

func TestCancelledRequestAbandonsTheConnection(t *testing.T) {
	received := make(chan struct{})
	server := startFake(t, func(connection net.Conn) {
		_, _ = ReadFrame(connection)
		close(received)
		_, _ = ReadFrame(connection) // never replies; returns when the client abandons the connection
	})
	conn := connect(t, server, ConnOptions{})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := conn.Do(ctx, OpPing, nil, nil)
		result <- err
	}()
	<-received
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); !errors.Is(err, ErrAbandoned) {
		t.Fatalf("a later caller must not see the first caller's cancellation as its own: err = %v, want ErrAbandoned", err)
	}
}

func TestDeadlineOnWaitingForASlotLeavesTheConnectionAlone(t *testing.T) {
	release := make(chan struct{})
	server := startFake(t, func(connection net.Conn) {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		<-release
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: request.ID})
		echoPong(connection)
	})
	conn := connect(t, server, ConnOptions{})
	first := make(chan error, 1)
	go func() {
		_, err := conn.Do(context.Background(), OpPing, nil, nil)
		first <- err
	}()
	// Wait until the first request holds the slot, then time out a second one.
	deadline := time.Now().Add(time.Second)
	for len(conn.slots) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := conn.Do(ctx, OpPing, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatalf("the first request must be unharmed by the second one's timeout: %v", err)
	}
	if conn.Err() != nil {
		t.Fatalf("connection closed: %v", conn.Err())
	}
}

func TestCloseIsIdempotentAndStopsTheReader(t *testing.T) {
	server := startFake(t, echoPong)
	raw, err := net.Dial("tcp", server.address)
	if err != nil {
		t.Fatal(err)
	}
	conn := NewConn(raw, ConnOptions{})
	for range 3 {
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-conn.readerDone:
	default:
		t.Fatal("Close returned while the reader goroutine was still running")
	}
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); !errors.Is(err, ErrConnClosed) {
		t.Fatalf("err = %v, want ErrConnClosed", err)
	}
}

func TestRoundTripWithIDSendsTheCallersID(t *testing.T) {
	given, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan RequestID, 1)
	server := startFake(t, func(connection net.Conn) {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		seen <- request.ID
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: request.ID, Payload: request.Payload})
	})
	conn := connect(t, server, ConnOptions{})
	reply, err := conn.RoundTripWithID(context.Background(), Frame{Op: OpPing, ID: given, Payload: []byte{7}})
	if err != nil {
		t.Fatal(err)
	}
	if got := <-seen; got != given {
		t.Fatalf("the server read request ID %s; the caller chose %s", got, given)
	}
	if reply.ID != given {
		t.Fatalf("the reply carries ID %s; the caller chose %s", reply.ID, given)
	}
}

func TestTheSameIDGoesOutOnTwoConnections(t *testing.T) {
	id, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan RequestID, 2)
	script := func(connection net.Conn) {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		seen <- request.ID
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: request.ID})
	}
	first := connect(t, startFake(t, script), ConnOptions{})
	second := connect(t, startFake(t, script), ConnOptions{})
	for _, conn := range []*Conn{first, second} {
		if _, err := conn.RoundTripWithID(context.Background(), Frame{Op: OpPing, ID: id}); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if got := <-seen; got != id {
			t.Fatalf("a connection read ID %s; the one ID %s must go out on both", got, id)
		}
	}
}

func TestAZeroRequestIDIsRefusedLocally(t *testing.T) {
	var (
		mu     sync.Mutex
		frames []RequestID
	)
	server := startFake(t, func(connection net.Conn) {
		for {
			request, err := ReadFrame(connection)
			if err != nil {
				return
			}
			mu.Lock()
			frames = append(frames, request.ID)
			mu.Unlock()
			if WriteFrame(connection, Frame{Op: OpPong, ID: request.ID, Payload: request.Payload}) != nil {
				return
			}
		}
	})
	conn := connect(t, server, ConnOptions{})
	_, err := conn.RoundTripWithID(context.Background(), Frame{Op: OpPing})
	if !errors.Is(err, ErrBadRequestID) {
		t.Fatalf("err = %v, want ErrBadRequestID", err)
	}
	// The server reads a connection's frames in order, so the ping's reply
	// comes after any frame the refused call might have sent.
	if _, err := conn.Do(context.Background(), OpPing, nil, nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(frames) != 1 {
		t.Fatalf("the server read %d frames; the refused call must send none, so only the ping arrives", len(frames))
	}
}

func TestAnIDInFlightIsRefused(t *testing.T) {
	id, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	letGo := func() { releaseOnce.Do(func() { close(release) }) }
	server := startFake(t, func(connection net.Conn) {
		request, err := ReadFrame(connection)
		if err != nil {
			return
		}
		close(received)
		<-release
		_ = WriteFrame(connection, Frame{Op: OpPong, ID: request.ID, Payload: request.Payload})
		echoPong(connection)
	})
	t.Cleanup(letGo) // a failed assertion must not leave the fake waiting
	conn := connect(t, server, ConnOptions{})
	type outcome struct {
		reply Frame
		err   error
	}
	first := make(chan outcome, 1)
	go func() {
		reply, err := conn.RoundTripWithID(context.Background(), Frame{Op: OpPing, ID: id, Payload: []byte{1}})
		first <- outcome{reply, err}
	}()
	<-received
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := conn.RoundTripWithID(ctx, Frame{Op: OpPing, ID: id}); !errors.Is(err, ErrBadRequestID) {
		t.Fatalf("a second call under the ID in flight: err = %v, want ErrBadRequestID", err)
	}
	letGo()
	got := <-first
	if got.err != nil || got.reply.ID != id {
		t.Fatalf("the first call must still complete: reply ID %s, err %v", got.reply.ID, got.err)
	}
}

func TestRoundTripStillMintsAFreshID(t *testing.T) {
	given, err := NewRequestID()
	if err != nil {
		t.Fatal(err)
	}
	seen := make(chan RequestID, 2)
	server := startFake(t, func(connection net.Conn) {
		for range 2 {
			request, err := ReadFrame(connection)
			if err != nil {
				return
			}
			seen <- request.ID
			if WriteFrame(connection, Frame{Op: OpPong, ID: request.ID}) != nil {
				return
			}
		}
	})
	conn := connect(t, server, ConnOptions{})
	for range 2 {
		if _, err := conn.RoundTrip(context.Background(), Frame{Op: OpPing, ID: given}); err != nil {
			t.Fatal(err)
		}
	}
	first, second := <-seen, <-seen
	if first == given || second == given {
		t.Fatal("RoundTrip sent the caller's ID; it must mint a fresh one")
	}
	if first == second {
		t.Fatal("two RoundTrip calls sent one ID")
	}
}
