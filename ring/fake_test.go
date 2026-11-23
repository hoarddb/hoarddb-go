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
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// fakeServer is a TLS listener that answers AUTH itself and hands every other frame to
// handle, which replies on the connection whenever it likes.
type fakeServer struct {
	t           *testing.T
	listener    net.Listener
	fingerprint string
	authFails   bool
	// onAuth, when set, sees each AUTH frame before the server answers it.
	onAuth func(frame hoarddbwire.Frame)
	handle func(conn *fakeConn, frame hoarddbwire.Frame)
	wait   sync.WaitGroup
}

type fakeConn struct {
	raw  net.Conn
	lock sync.Mutex
}

func (c *fakeConn) send(frame hoarddbwire.Frame) {
	c.lock.Lock()
	defer c.lock.Unlock()
	_ = hoarddbwire.WriteFrame(c.raw, frame)
}

func (c *fakeConn) reply(request hoarddbwire.Frame, payload []byte) {
	c.send(hoarddbwire.Frame{Op: hoarddbwire.OpRingReply, ID: request.ID, Payload: payload})
}

func (c *fakeConn) refuse(request hoarddbwire.Frame, code ringwire.Code, message string) {
	payload, err := ringwire.EncodeRingError(ringwire.RingError{Code: code, Message: message})
	if err != nil {
		panic(err) // test-only: a code this package picked
	}
	c.send(hoarddbwire.Frame{Op: hoarddbwire.OpError, ID: request.ID, Payload: payload})
}

func startFakeServer(t *testing.T, handle func(conn *fakeConn, frame hoarddbwire.Frame)) *fakeServer {
	t.Helper()
	tlsConfig, fingerprint := fakeServerTLS(t)
	listener, err := tls.Listen("tcp", "127.0.0.1:0", tlsConfig)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeServer{t: t, listener: listener, fingerprint: fingerprint, handle: handle}
	server.wait.Add(1)
	go server.accept()
	t.Cleanup(func() {
		_ = listener.Close()
		server.wait.Wait()
	})
	return server
}

func (s *fakeServer) addr() string { return s.listener.Addr().String() }

func (s *fakeServer) accept() {
	defer s.wait.Done()
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wait.Add(1)
		go func() {
			defer s.wait.Done()
			defer func() { _ = raw.Close() }()
			s.serve(&fakeConn{raw: raw})
		}()
	}
}

func (s *fakeServer) serve(conn *fakeConn) {
	reader := bufio.NewReader(conn.raw)
	for {
		frame, err := hoarddbwire.ReadFrame(reader)
		if err != nil {
			return
		}
		if frame.Op == hoarddbwire.OpAuth {
			if s.onAuth != nil {
				s.onAuth(frame)
			}
			if s.authFails {
				conn.send(hoarddbwire.Frame{Op: hoarddbwire.OpAuthFail, ID: frame.ID})
			} else {
				conn.send(hoarddbwire.Frame{Op: hoarddbwire.OpAuthOK, ID: frame.ID})
			}
			continue
		}
		s.handle(conn, frame)
	}
}

func (s *fakeServer) options() Options {
	return Options{Addr: s.addr(), User: "root", Password: "pw", Fingerprint: s.fingerprint}
}

func adminReplyFrame(t *testing.T, conn *fakeConn, request hoarddbwire.Frame, reply ringwire.RingAdminReply) {
	t.Helper()
	payload, err := ringwire.EncodeRingAdminReply(reply)
	if err != nil {
		t.Fatal(err)
	}
	conn.reply(request, payload)
}

func userFields() []ringwire.RingAdminReplyField {
	return []ringwire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Purchases", Type: "fifoStore"}}
}
