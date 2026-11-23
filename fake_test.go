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
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// fakeServer is a TLS server that speaks just enough of the protocol for the
// driver's unit tests, with a script per connection. It is written from the
// specs, like the driver; the integration tests are the check that the specs
// and the real server agree.
type fakeServer struct {
	t           *testing.T
	address     string
	fingerprint string
	connections atomic.Int32
	script      func(*fakeConn)
	group       sync.WaitGroup
}

type fakeConn struct {
	net.Conn
	t     *testing.T
	index int // 1-based, in accept order
}

func newCertificate(t *testing.T) (tls.Certificate, ed25519.PublicKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "hoarddb-fake"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, public
}

// startFake listens with TLS 1.3 and ALPN hoarddb/1 and runs script on every
// accepted connection.
func startFake(t *testing.T, script func(*fakeConn)) *fakeServer {
	t.Helper()
	certificate, public := newCertificate(t)
	return startFakeWith(t, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{hoarddbwire.ALPNv1},
	}, trust.Fingerprint(public), script)
}

func startFakeWith(t *testing.T, config *tls.Config, fingerprint string, script func(*fakeConn)) *fakeServer {
	t.Helper()
	listener, err := tls.Listen("tcp", "127.0.0.1:0", config)
	if err != nil {
		t.Fatal(err)
	}
	server := &fakeServer{t: t, address: listener.Addr().String(), fingerprint: fingerprint, script: script}
	server.group.Add(1)
	go func() {
		defer server.group.Done()
		for {
			accepted, err := listener.Accept()
			if err != nil {
				return
			}
			index := int(server.connections.Add(1))
			server.group.Add(1)
			go func() {
				defer server.group.Done()
				defer func() { _ = accepted.Close() }()
				_ = accepted.SetDeadline(time.Now().Add(30 * time.Second))
				script := server.script
				if script != nil {
					script(&fakeConn{Conn: accepted, t: t, index: index})
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		server.group.Wait()
	})
	return server
}

func (c *fakeConn) read() (hoarddbwire.Frame, error) { return hoarddbwire.ReadFrame(c.Conn) }

func (c *fakeConn) send(request hoarddbwire.Frame, op hoarddbwire.Opcode, payload any) {
	c.t.Helper()
	var encoded []byte
	if payload != nil {
		var err error
		if encoded, err = codec.Marshal(payload); err != nil {
			c.t.Error(err)
			return
		}
	}
	_ = hoarddbwire.WriteFrame(c.Conn, hoarddbwire.Frame{Op: op, ID: request.ID, Payload: encoded})
}

// authRequest reads the first frame and decodes it as AUTH.
func (c *fakeConn) authRequest() (hoarddbwire.Frame, hoarddbwire.AuthRequest, bool) {
	c.t.Helper()
	frame, err := c.read()
	if err != nil {
		return frame, hoarddbwire.AuthRequest{}, false
	}
	var request hoarddbwire.AuthRequest
	if frame.Op != hoarddbwire.OpAuth || codec.Unmarshal(frame.Payload, &request) != nil {
		return frame, request, false
	}
	return frame, request, true
}

// authOK answers an AUTH with a token that lives ttl (and whose chain lives a
// day), and the auth_token capability.
func (c *fakeConn) authOK(request hoarddbwire.Frame, token string, ttl time.Duration) {
	c.t.Helper()
	c.authOKWith(request, token, ttl, "auth_token", "get_fast")
}

// authOKWith is authOK advertising exactly the given capabilities.
func (c *fakeConn) authOKWith(request hoarddbwire.Frame, token string, ttl time.Duration, capabilities ...string) {
	c.t.Helper()
	c.send(request, hoarddbwire.OpAuthOK, hoarddbwire.AuthResponse{
		Success: true, Token: token,
		TokenExpiresAt: time.Now().Add(ttl).Unix(), TokenDeadline: time.Now().Add(24 * time.Hour).Unix(),
		ProtocolVersion: 1, ServerVersion: "fake-1.0", Capabilities: capabilities,
	})
}

func (c *fakeConn) authFail(request hoarddbwire.Frame, reason int32, message string) {
	c.t.Helper()
	c.send(request, hoarddbwire.OpAuthFail, map[string]any{"reason": reason, "message": message})
}

func (c *fakeConn) errorFrame(request hoarddbwire.Frame, code int32, message string) {
	c.t.Helper()
	c.send(request, hoarddbwire.OpError, map[string]any{"code": code, "message": message})
}

// serveAuthThen authenticates the first frame with a long-lived token, then
// answers PINGs through onPing until the connection ends. onPing receives the
// 1-based count of PINGs on this connection and returns false to stop serving.
func serveAuthThen(onPing func(c *fakeConn, request hoarddbwire.Frame, count int) bool) func(*fakeConn) {
	return func(c *fakeConn) {
		first, _, ok := c.authRequest()
		if !ok {
			return
		}
		c.authOK(first, "token-from-"+string(rune('0'+c.index)), time.Hour)
		count := 0
		for {
			frame, err := c.read()
			if err != nil {
				return
			}
			if frame.Op != hoarddbwire.OpPing {
				continue
			}
			count++
			if onPing == nil {
				c.send(frame, hoarddbwire.OpPong, nil)
				continue
			}
			if !onPing(c, frame, count) {
				return
			}
		}
	}
}

// serveFramesWith authenticates the first frame, advertising capabilities, then
// calls onFrame for every later frame until it returns false or the connection
// ends. onFrame receives the 1-based count of frames on this connection.
func serveFramesWith(capabilities []string, onFrame func(c *fakeConn, request hoarddbwire.Frame, count int) bool) func(*fakeConn) {
	return func(c *fakeConn) {
		first, _, ok := c.authRequest()
		if !ok {
			return
		}
		c.authOKWith(first, "token-from-"+string(rune('0'+c.index)), time.Hour, capabilities...)
		count := 0
		for {
			frame, err := c.read()
			if err != nil {
				return
			}
			count++
			if !onFrame(c, frame, count) {
				return
			}
		}
	}
}

// serveFrames is serveFramesWith advertising what authOK does.
func serveFrames(onFrame func(c *fakeConn, request hoarddbwire.Frame, count int) bool) func(*fakeConn) {
	return serveFramesWith([]string{"auth_token", "get_fast"}, onFrame)
}

// errorWith sends an ERROR frame with arbitrary members.
func (c *fakeConn) errorWith(request hoarddbwire.Frame, members map[string]any) {
	c.t.Helper()
	c.send(request, hoarddbwire.OpError, members)
}

func marshal(document any) ([]byte, error)       { return codec.Marshal(document) }
func unmarshal(payload []byte, target any) error { return codec.Unmarshal(payload, target) }

func selfSignedECDSA(t *testing.T, key *ecdsa.PrivateKey) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "not-ed25519"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return der
}

// knownPath is a known_servers path in a private (0700) directory.
func knownPath(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "known_servers")
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}

func pong(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
	c.send(request, hoarddbwire.OpPong, nil)
	return true
}
