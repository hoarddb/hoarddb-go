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
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestLoginCarriesTheDriverName: the AUTH login names this driver, so a server log tells a
// request from the Go driver apart from one the internal ring client sent. The name is pinned
// here, not only through the constant, so renaming the constant does not silently rename the
// driver in server logs.
func TestLoginCarriesTheDriverName(t *testing.T) {
	logins := make(chan string, 1)
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	server.onAuth = func(frame hoarddbwire.Frame) {
		var login loginRequest
		if err := bson.Unmarshal(frame.Payload, &login); err != nil {
			t.Errorf("decoding the login frame: %v", err)
			return
		}
		logins <- login.Client
	}
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatalf("dialing the fake server: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	select {
	case got := <-logins:
		if got != "hoarddb-go" {
			t.Fatalf("the login names the client %q, want %q", got, "hoarddb-go")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the fake server saw no login frame")
	}
}
