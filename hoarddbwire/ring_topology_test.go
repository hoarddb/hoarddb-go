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

package hoarddbwire_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/hoarddb/hoarddb-go/conformance"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// ringTopologyRequestHex reads the request payload of a row of the frozen
// vector file, so a test compares the encoder with the server's bytes and
// never with the encoder's own output.
func ringTopologyRequestHex(t *testing.T, name string) []byte {
	t.Helper()
	file, err := conformance.Load(conformance.DriverVectorsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, vector := range file.Vectors {
		if vector.Name == name && vector.Request != nil {
			return unhex(t, vector.Request.PayloadHex)
		}
	}
	t.Fatalf("the vector file has no request row named %q", name)
	return nil
}

// TestATopologyRequestIsTwoInt64Members: the request is epoch then state_seq,
// both int64 and both written at zero, as the server's rows write them.
func TestATopologyRequestIsTwoInt64Members(t *testing.T) {
	rows := []struct {
		name    string
		request hoarddbwire.RingTopologyRequest
	}{
		{"ring topology: the first call after AUTH", hoarddbwire.RingTopologyRequest{Epoch: 0, StateSeq: 0}},
		{"ring topology: the view held is current", hoarddbwire.RingTopologyRequest{Epoch: 4, StateSeq: 9}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			got, err := row.request.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if want := ringTopologyRequestHex(t, row.name); !bytes.Equal(got, want) {
				t.Errorf("Encode wrote %x, the row's request payload is %x", got, want)
			}
		})
	}
}

// TestANegativeTopologyNumberIsRefused: the node holds both numbers to 0 or
// more, so the driver refuses a negative one at the call and names it.
func TestANegativeTopologyNumberIsRefused(t *testing.T) {
	rows := []struct {
		request hoarddbwire.RingTopologyRequest
		member  string
	}{
		{hoarddbwire.RingTopologyRequest{Epoch: -1, StateSeq: 0}, "Epoch"},
		{hoarddbwire.RingTopologyRequest{Epoch: 0, StateSeq: -1}, "StateSeq"},
	}
	for _, row := range rows {
		err := row.request.Validate()
		if !errors.Is(err, hoarddbwire.ErrRingRequest) {
			t.Errorf("Validate of %+v answered %v, want ErrRingRequest", row.request, err)
			continue
		}
		if !strings.Contains(err.Error(), row.member) {
			t.Errorf("Validate of %+v names %q, want the member %q", row.request, err, row.member)
		}
		if _, err := row.request.Encode(); !errors.Is(err, hoarddbwire.ErrRingRequest) {
			t.Errorf("Encode of %+v answered %v, want ErrRingRequest", row.request, err)
		}
	}
}
