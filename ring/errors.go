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
	"errors"
	"fmt"
	"time"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// ErrNotFound is returned (wrapped, so test it with errors.Is) when a single-value read
// finds nothing: a Get of a missing document, a Pop of an empty store, a Take of an absent
// key. List-shaped reads (Peek, StoreList, List) answer an empty result instead.
var ErrNotFound = errors.New("ring: not found")

// Error is a refusal from the server: the ring code and the message, which names the fix.
// Detail is set for the codes that define one (NOT_OWNER, STALE_EPOCH, OWNER_DOWN,
// NO_QUORUM, LEASE_LAPSED, RF_UNSUPPORTED, and NOT_FOUND of an expired document).
type Error struct {
	// Code is the ring code of the refusal.
	Code ringwire.Code
	// Message is the server's text.
	Message string
	// Detail is the structured detail of the code, or nil.
	Detail *ringwire.Detail
}

// Error returns the code and message.
func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Code, e.Message) }

// Is makes errors.Is(err, ErrNotFound) true for the server's NOT_FOUND.
func (e *Error) Is(target error) bool {
	return target == ErrNotFound && e.Code == ringwire.CodeNotFound
}

// refusalError decodes the payload of an OpError frame, ring or legacy shape.
func refusalError(payload []byte) error {
	if refusal, err := ringwire.DecodeRingError(payload); err == nil {
		return &Error{Code: refusal.Code, Message: refusal.Message, Detail: refusal.Detail}
	}
	return fmt.Errorf("the server refused the request with an error frame of %d bytes that is not a ring error", len(payload))
}

// frameError names an unexpected opcode.
func frameError(op hoarddbwire.Opcode, what string) error {
	return fmt.Errorf("%s was answered with opcode %s, want RING_REPLY", what, op)
}

// IsExpired reports whether err is the NOT_FOUND of a document that exists but is past its expire
// time. It reads the structured detail, never the message. An expired miss is also ErrNotFound.
func IsExpired(err error) bool {
	var refusal *Error
	return errors.As(err, &refusal) && refusal.Code == ringwire.CodeNotFound && refusal.Detail != nil && refusal.Detail.Expired
}

// ExpiresIn is the client's clock plus d, for filling the datetime field a bucket expires its documents at
// (`expire at <Field>`). The server never computes it: the expiry instant is the developer's.
func ExpiresIn(d time.Duration) time.Time { return time.Now().Add(d) }
