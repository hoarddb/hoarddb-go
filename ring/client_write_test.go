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
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestAFrameOverTheSizeLimitIsRefusedBeforeTheWrite: a frame over the 32 MiB limit is refused
// before a byte is sent, so the connection stays usable: the fake server receives no frame for
// it and answers the next request on the same connection.
func TestAFrameOverTheSizeLimitIsRefusedBeforeTheWrite(t *testing.T) {
	var frames atomic.Int64
	var decl, schema atomic.Int64
	describe := describeHandler(t, &decl, &schema)
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		frames.Add(1)
		describe(conn, frame)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	oversize := hoarddbwire.Frame{Op: hoarddbwire.OpRingKeyed, ID: client.NextRequestID(),
		Payload: make([]byte, hoarddbwire.MaxPayload+1)}
	err = client.write(context.Background(), oversize)
	if err == nil || !errors.Is(err, hoarddbwire.ErrFrameTooLarge) {
		t.Fatalf("writing a frame of %d payload bytes returned %v, want the frame-too-large refusal", len(oversize.Payload), err)
	}
	if got := frames.Load(); got != 0 {
		t.Fatalf("%d frame(s) reached the server after the refusal, want 0", got)
	}
	if _, err := client.DescribeBucket(testContext(t), "db", "User"); err != nil {
		t.Fatalf("the connection must stay open after the refusal: %v", err)
	}
	if got := frames.Load(); got != 1 {
		t.Fatalf("%d frame(s) reached the server, want the one describe after the refusal", got)
	}
}

// TestARequestWithoutADeadlineGetsTheThirtySecondDefault: a call whose context carries no
// deadline is bounded by Options.RequestTimeout, 30 seconds when it is not set. The option is
// shortened here so the test does not wait out the default; the branch is the same one.
func TestARequestWithoutADeadlineGetsTheThirtySecondDefault(t *testing.T) {
	if defaultRequestTimeout != 30*time.Second {
		t.Fatalf("defaultRequestTimeout = %s, want the 30 s of the design", defaultRequestTimeout)
	}
	server := startFakeServer(t, func(*fakeConn, hoarddbwire.Frame) {})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	client.opts.RequestTimeout = 50 * time.Millisecond
	answered := make(chan error, 1)
	go func() {
		_, err := client.DescribeBucket(context.Background(), "db", "User")
		answered <- err
	}()
	select {
	case err := <-answered:
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "no answer") {
			t.Fatalf("a call with no context deadline returned %v, want the deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a call whose context had no deadline waited past the client's own timeout")
	}
}

// TestARefusalThatIsNotARingErrorSaysSo: the legacy branch of refusalError still returns an
// error, and it names the payload size so an operator can report it.
func TestARefusalThatIsNotARingErrorSaysSo(t *testing.T) {
	err := refusalError([]byte{0x01, 0x02, 0x03})
	if err == nil {
		t.Fatal("a payload that is not a ring error returned no error")
	}
	var refusal *Error
	if errors.As(err, &refusal) {
		t.Fatalf("a payload that is not a ring error returned %T, want a plain error", err)
	}
	for _, want := range []string{"not a ring error", "3 bytes"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("the refusal %q does not say %q", err, want)
		}
	}
}
