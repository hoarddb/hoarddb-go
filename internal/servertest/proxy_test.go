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
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

func dialProxy(t *testing.T, ctx context.Context, proxy *servertest.Proxy) *hoarddbwire.Conn {
	t.Helper()
	config, err := trust.NewTLSConfig(proxy.Address, trust.Policy{Pin: proxy.Fingerprint})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := hoarddbwire.Dial(ctx, proxy.Address, config, hoarddbwire.DialOptions{})
	if err != nil {
		t.Fatalf("dialling the proxy: %v", err)
	}
	return conn
}

// The auth leg counts what a driver puts on the wire through this proxy, so the
// proxy's own record has to be right: each connection, each AUTH with its
// method, the server's real answer, and who closed the connection.
func TestProxyRecordsEveryAuthAndTheServersAnswer(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	proxy := servertest.StartProxy(t, server)
	ctx := context30(t)

	login := func(conn *hoarddbwire.Conn, password, token string) hoarddbwire.Frame {
		t.Helper()
		reply, err := conn.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
			Username: server.User, Password: password, Token: token, ProtocolVersion: 1,
			Capabilities: []string{}, Client: "hoarddb-go/proxy-test",
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return reply
	}

	good := dialProxy(t, ctx, proxy)
	var minted hoarddbwire.AuthResponse
	reply, err := good.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
		Username: server.User, Password: server.Password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-go/proxy-test",
	}, &minted)
	if err != nil || reply.Op != hoarddbwire.OpAuthOK || minted.Token == "" {
		t.Fatalf("password AUTH through the proxy: op %v, token %q, err %v", reply.Op, minted.Token, err)
	}
	if reply := login(good, "", minted.Token); reply.Op != hoarddbwire.OpAuthOK {
		t.Fatalf("token refresh through the proxy answered %v", reply.Op)
	}
	if reply, err := good.Do(ctx, hoarddbwire.OpPing, nil, nil); err != nil || reply.Op != hoarddbwire.OpPong {
		t.Fatalf("PING through the proxy: %v %v", reply.Op, err)
	}

	bad := dialProxy(t, ctx, proxy)
	if reply := login(bad, "not-the-password", ""); reply.Op != hoarddbwire.OpAuthFail {
		t.Fatalf("a wrong password answered %v", reply.Op)
	}
	_ = bad.Close()
	closed := proxy.AwaitClosed(t, 1, 5*time.Second)
	if closed.ClosedBy != "driver" {
		t.Errorf("the client closed connection 2, but the proxy says %q", closed.ClosedBy)
	}

	seen := proxy.Connections()
	if len(seen) != 2 {
		t.Fatalf("%s; want 2 connections", servertest.Describe(seen))
	}
	first, second := seen[0], seen[1]
	want := []servertest.AuthAttempt{
		{Method: "password", OnFreshConnection: true, Answer: hoarddbwire.OpAuthOK},
		{Method: "token", OnFreshConnection: false, Answer: hoarddbwire.OpAuthOK},
	}
	if len(first.Auths) != 2 || first.Auths[0] != want[0] || first.Auths[1] != want[1] || first.Frames != 3 {
		t.Errorf("connection 1: %s", servertest.Describe(seen[:1]))
	}
	wantFail := servertest.AuthAttempt{Method: "password", OnFreshConnection: true, Answer: hoarddbwire.OpAuthFail, Reason: hoarddbwire.AuthFailCredentials}
	if len(second.Auths) != 1 || second.Auths[0] != wantFail {
		t.Errorf("connection 2: %s", servertest.Describe(seen[1:]))
	}
	if most := servertest.PasswordAuthsPerConnection(seen); most != 1 {
		t.Errorf("PasswordAuthsPerConnection = %d, want 1", most)
	}

	proxy.DropAll()
	select {
	case <-good.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("DropAll left the client's connection open")
	}
	if closedBy := proxy.AwaitClosed(t, 0, 5*time.Second).ClosedBy; closedBy != "proxy" {
		t.Errorf("DropAll recorded %q as the closer", closedBy)
	}
}

// The conformance replay (leg 3) holds what the driver sent, and what the
// server answered, against the vectors: the proxy must hand over both frames
// of every data request, in order, and leave AUTH out.
func TestProxyRecordsEveryDataExchange(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	proxy := servertest.StartProxy(t, server)
	ctx := context30(t)

	conn := dialProxy(t, ctx, proxy)
	if reply, err := conn.Do(ctx, hoarddbwire.OpAuth, hoarddbwire.AuthRequest{
		Username: server.User, Password: server.Password, ProtocolVersion: 1, Capabilities: []string{}, Client: "hoarddb-go/proxy-test",
	}, nil); err != nil || reply.Op != hoarddbwire.OpAuthOK {
		t.Fatalf("AUTH: %v %v", reply.Op, err)
	}
	if got := proxy.Exchanges(); len(got) != 0 {
		t.Fatalf("an AUTH is not a data exchange, but the proxy recorded %+v", got)
	}
	if reply, err := conn.Do(ctx, hoarddbwire.OpPing, nil, nil); err != nil || reply.Op != hoarddbwire.OpPong {
		t.Fatalf("PING: %v %v", reply.Op, err)
	}
	var created hoarddbwire.AckResponse
	if reply, err := conn.Do(ctx, hoarddbwire.OpCreateDB, hoarddbwire.NameRequest{Name: "seen"}, &created); err != nil || reply.Op != hoarddbwire.OpCreateDB || !created.Success {
		t.Fatalf("CREATE_DB: %v %v %+v", reply.Op, err, created)
	}

	got := proxy.Exchanges()
	if len(got) != 2 {
		t.Fatalf("%d exchanges, want 2 (PING, CREATE_DB): %+v", len(got), got)
	}
	if got[0].Request.Op != hoarddbwire.OpPing || got[0].Response.Op != hoarddbwire.OpPong || !got[0].Answered {
		t.Errorf("first exchange = %+v; want PING answered by PONG", got[0])
	}
	second := got[1]
	if second.Request.Op != hoarddbwire.OpCreateDB || second.Response.Op != hoarddbwire.OpCreateDB || !second.Answered ||
		second.Response.ID != second.Request.ID || len(second.Request.Payload) == 0 || len(second.Response.Payload) == 0 {
		t.Errorf("second exchange = %+v; want CREATE_DB with both payloads and one request ID", second)
	}
}
