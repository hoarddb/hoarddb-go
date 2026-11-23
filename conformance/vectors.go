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

// Package conformance is the driver's conformance harness: it replays the
// frozen vectors against the driver, one leg at a time (handbook/testing.md
// section 2). Leg 1, the codec, needs no server.
//
// The vectors and the handbook sit beside this module: testdata and handbook.
// The vector files are read-only here
// (handbook/README.md, "Changing this handbook"); this package reads them and
// never writes them.
package conformance

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// DriverVectorsPath is clients/testdata/driver-vectors/v1.json relative to a
// package directly under the module root (clients/go), where every test of
// this harness runs. The driver keeps no copy of its own.
const DriverVectorsPath = "../testdata/driver-vectors/v1.json"

// File is clients/testdata/driver-vectors/v1.json (handbook/testing.md section 1.1).
type File struct {
	Format        string            `json:"format"`
	ServerCommit  string            `json:"server_commit"`
	ServerVersion string            `json:"server_version"`
	ServerEnv     map[string]string `json:"server_env"`
	Vectors       []Vector          `json:"vectors"`
}

// Vector is one entry of the file. Its Kind says which of the other members
// are set: "frame", "exchange", "decode", "trust" or "refuse".
type Vector struct {
	Name   string `json:"name"`
	Group  string `json:"group"`
	Kind   string `json:"kind"`
	Status string `json:"status"`
	Notes  string `json:"notes"`

	// frame and decode vectors.
	FrameHex         string          `json:"frame_hex"`
	RequestHeaderHex string          `json:"request_header_hex"`
	Expect           json.RawMessage `json:"expect"`

	// exchange vectors.
	StoreType  string    `json:"store_type"`
	Topology   string    `json:"topology"`
	Connection string    `json:"connection"`
	Replay     bool      `json:"replay"`
	Request    *Request  `json:"request"`
	Response   *Response `json:"response"`
	// Error and AuthFail are what an ERROR or AUTH_FAIL answer must be
	// reported as. They belong to the vector, beside its response.
	Error     *ErrorExpect      `json:"error"`
	AuthFail  *AuthFailExpect   `json:"auth_fail"`
	Setup     []Setup           `json:"setup"`
	ServerEnv map[string]string `json:"server_env"`

	// refuse vectors: a payload the server's own decoder refuses, in the
	// request, reply or error direction. The payload is in Request or Response.
	Refuse *RefuseExpect `json:"refuse"`

	// trust vectors: a key with its certificate and fingerprint, or a
	// known_servers case.
	PublicKeyHex   string            `json:"public_key_hex"`
	CertificatePEM string            `json:"certificate_pem"`
	Fingerprint    string            `json:"fingerprint"`
	KnownServers   *KnownServersCase `json:"known_servers"`
}

// RefuseExpect says which payload of a refuse vector is refused (Direction is
// "request", "reply" or "error") and how the server reported it.
type RefuseExpect struct {
	Direction string `json:"direction"`
	Code      int    `json:"code"`
	Name      string `json:"name"`
}

// KnownServersCase is a known_servers file and what loading it must give:
// exactly Entries (host:port to the raw key's hex), or a refusal whose message
// contains ErrorContains.
type KnownServersCase struct {
	FileText      string            `json:"file_text"`
	Entries       map[string]string `json:"entries"`
	ErrorContains string            `json:"error_contains"`
}

// Setup is a request an exchange sends first, expecting it to succeed.
type Setup struct {
	Opcode      int             `json:"opcode"`
	OpcodeName  string          `json:"opcode_name"`
	PayloadJSON json.RawMessage `json:"payload_json"`
}

// Request is the request half of an exchange.
type Request struct {
	Opcode        int             `json:"opcode"`
	OpcodeName    string          `json:"opcode_name"`
	RequestIDHex  string          `json:"request_id_hex"`
	PayloadJSON   json.RawMessage `json:"payload_json"`
	PayloadHex    string          `json:"payload_hex"`
	AllowedFields []string        `json:"allowed_fields"`
}

// Response is the answer half of an exchange.
type Response struct {
	Opcode         int             `json:"opcode"`
	OpcodeName     string          `json:"opcode_name"`
	RequestIDHex   string          `json:"request_id_hex"`
	PayloadJSON    json.RawMessage `json:"payload_json"`
	PayloadHex     string          `json:"payload_hex"`
	Fields         []string        `json:"fields"`
	Vary           []string        `json:"vary"`
	AdditiveFields []string        `json:"additive_fields"`
	AbsentFields   []string        `json:"absent_fields"`
	Error          *ErrorExpect    `json:"error"`
	AuthFail       *AuthFailExpect `json:"auth_fail"`
}

// ErrorExpect is the error an ERROR answer must be reported as.
type ErrorExpect struct {
	Code            int      `json:"code"`
	Name            string   `json:"name"`
	WriteOutcome    string   `json:"write_outcome"`
	MessageContains []string `json:"message_contains"`
}

// AuthFailExpect is the refusal an AUTH_FAIL answer must be reported as.
type AuthFailExpect struct {
	Reason          int      `json:"reason"`
	Name            string   `json:"name"`
	MessageContains []string `json:"message_contains"`
}

// FrameExpect is what a frame vector's decoder must produce: either the
// decoded fields of an accepted frame or the reason it refuses.
type FrameExpect struct {
	Result        string `json:"result"`
	Version       int    `json:"version"`
	Length        int    `json:"length"`
	Opcode        int    `json:"opcode"`
	RequestIDHex  string `json:"request_id_hex"`
	PayloadHex    string `json:"payload_hex"`
	RefuseBecause string `json:"refuse_because"`
}

// DecodeExpect is what a decode vector's frame must be reported as.
type DecodeExpect struct {
	Result          string          `json:"result"`
	Code            int             `json:"code"`
	Name            string          `json:"name"`
	RequestIDZero   bool            `json:"request_id_zero"`
	ConnectionFatal bool            `json:"connection_fatal"`
	MessageContains []string        `json:"message_contains"`
	AuthFailReason  int             `json:"auth_fail_reason"`
	PayloadJSON     json.RawMessage `json:"payload_json"`
}

// Load reads and parses a driver vector file.
func Load(path string) (*File, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading the driver vectors: %w", err)
	}
	var file File
	if err := json.Unmarshal(contents, &file); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if file.Format != "hoarddb-driver-vectors/1" {
		return nil, fmt.Errorf("%s has format %q, this harness reads hoarddb-driver-vectors/1", path, file.Format)
	}
	return &file, nil
}

// Provisional reports whether the vector is informative only: its result is
// reported and never fails a run (handbook/README.md, "Precedence").
func (vector Vector) Provisional() bool { return strings.HasPrefix(vector.Status, "provisional") }

// Named returns the vector with the given name; names are unique in a file.
func (file *File) Named(name string) (Vector, bool) {
	for _, vector := range file.Vectors {
		if vector.Name == name {
			return vector, true
		}
	}
	return Vector{}, false
}
