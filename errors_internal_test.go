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
	"bytes"
	"errors"
	"testing"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// TestAWireErrorCarriesTheOwner holds the owner a NOT_OWNER frame names to the
// error the caller gets, so the caller can send the request to that node.
func TestAWireErrorCarriesTheOwner(t *testing.T) {
	failure := wireError("get", "app/User", hoarddbwire.ErrorResponse{
		Code: hoarddbwire.CodeNotOwner, Message: "m",
		Detail: hoarddbwire.ErrorDetail{Owner: "node-b", OwnerAddr: "192.0.2.2:4433"},
	})
	if failure.Owner != "node-b" || failure.OwnerAddr != "192.0.2.2:4433" {
		t.Errorf("Owner, OwnerAddr = %q, %q; want node-b, 192.0.2.2:4433", failure.Owner, failure.OwnerAddr)
	}
	if !errors.Is(failure, ErrNotOwner) {
		t.Error("a NOT_OWNER frame must match ErrNotOwner")
	}
}

// TestBatchTooLargeErrorFrameKeepsTheConnectionAndIsNeverRetried builds the
// ERROR frame the server sends for a batch over its atomic bound, with both
// outcomes it stamps, and holds the driver's answer to the code: its own
// class, no automatic retry, the connection kept, and the stamp carried as
// given.
func TestBatchTooLargeErrorFrameKeepsTheConnectionAndIsNeverRetried(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome hoarddbwire.WriteOutcome
		want    WriteOutcome
	}{
		{name: "a standalone node refused it whole", outcome: hoarddbwire.WriteNotApplied, want: OutcomeNotApplied},
		{name: "a cluster owner cannot tell", outcome: hoarddbwire.WriteUnknown, want: OutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			message := "This batch exceeds the largest batch the store commits atomically, so none of it was written."
			payload, err := codec.Marshal(map[string]any{
				"code": int32(0x11), "message": message, "write_outcome": string(test.outcome),
			})
			if err != nil {
				t.Fatal(err)
			}
			var wire bytes.Buffer
			if err := hoarddbwire.WriteFrame(&wire, hoarddbwire.Frame{Op: hoarddbwire.OpError, Payload: payload}); err != nil {
				t.Fatalf("building the ERROR frame: %v", err)
			}
			frame, err := hoarddbwire.ReadFrame(&wire)
			if err != nil {
				t.Fatalf("reading the ERROR frame: %v", err)
			}
			if frame.Op != hoarddbwire.OpError {
				t.Fatalf("frame op = %s, want ERROR", frame.Op)
			}

			failure := wireError("insert many", "app/users", hoarddbwire.DecodeError(frame.Payload))
			if failure.Code != CodeBatchTooLarge {
				t.Errorf("Code = %v, want %v", failure.Code, CodeBatchTooLarge)
			}
			if !errors.Is(failure, ErrBatchTooLarge) {
				t.Error("the error must match ErrBatchTooLarge")
			}
			if errors.Is(failure, ErrInternal) {
				t.Error("BATCH_TOO_LARGE must not fold into ErrInternal")
			}
			if failure.Message != message {
				t.Errorf("Message = %q, want the server's sentence verbatim %q", failure.Message, message)
			}
			if failure.Outcome != test.want {
				t.Errorf("Outcome = %v, want %v", failure.Outcome, test.want)
			}
			if got := retryRule(answered, failure, false); got != giveUp {
				t.Errorf("retryRule on a write = %v, want give up", got)
			}
			if got := retryRule(answered, failure, true); got != giveUp {
				t.Errorf("retryRule on a read = %v, want give up", got)
			}
			if !keepsConnection(failure.Code) {
				t.Error("the connection must survive a BATCH_TOO_LARGE error frame")
			}
		})
	}
}
