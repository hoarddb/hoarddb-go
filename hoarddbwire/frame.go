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
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// Frame layout (handbook/wire.md section 2):
//
//	Ver(1) | Length(4, little-endian) | Op(1) | RequestID(16) | Payload(N)
//
// Length counts everything after itself: 1 + 16 + len(Payload).
const (
	// ProtocolVersion is the only frame version this package speaks.
	ProtocolVersion byte = 1
	// HeaderSize is the bytes before the payload: version, length, opcode and
	// request ID.
	HeaderSize = 1 + 4 + 1 + RequestIDLen
	// RequestIDLen is the size of a request ID.
	RequestIDLen = 16
	// MaxFrameLength is the largest value of the Length field, 32 MiB. A frame
	// over it gets no answer, only a closed connection, so a sender must
	// enforce it first.
	MaxFrameLength = 32 << 20
	// MaxPayload is the largest payload that fits in one frame.
	MaxPayload = MaxFrameLength - 1 - RequestIDLen

	minFrameLength = 1 + RequestIDLen
)

// Errors a frame codec returns. Match them with errors.Is.
var (
	// ErrFrameTooLarge is a frame whose length exceeds MaxFrameLength.
	ErrFrameTooLarge = errors.New("frame exceeds the 32 MiB protocol limit")
	// ErrBadFrame is a frame header that breaks the protocol: wrong version or
	// a length too small to hold an opcode and request ID.
	ErrBadFrame = errors.New("malformed frame")
)

// RequestID correlates a response with its request. The client mints it and
// the server echoes it.
type RequestID [RequestIDLen]byte

// NewRequestID returns a random version 4 UUID.
func NewRequestID() (RequestID, error) {
	var id RequestID
	if _, err := rand.Read(id[:]); err != nil {
		return id, fmt.Errorf("generating a request ID: %w", err)
	}
	id[6] = id[6]&0x0F | 0x40
	id[8] = id[8]&0x3F | 0x80
	return id, nil
}

// IsZero reports whether the ID is all zero. Connection-level refusals carry a
// zero ID.
func (id RequestID) IsZero() bool { return id == RequestID{} }

// String is the hex form, which is also the server's audit-log identifier.
func (id RequestID) String() string { return fmt.Sprintf("%x", id[:]) }

// Frame is one protocol message.
type Frame struct {
	Op      Opcode
	ID      RequestID
	Payload []byte
}

// AppendTo appends the encoded frame to dst. It refuses a frame that would
// exceed MaxFrameLength before any byte is produced.
func (f Frame) AppendTo(dst []byte) ([]byte, error) {
	if len(f.Payload) > MaxPayload {
		return dst, fmt.Errorf("%s payload of %d bytes: %w (the most one frame carries is %d)",
			f.Op, len(f.Payload), ErrFrameTooLarge, MaxPayload)
	}
	length := minFrameLength + len(f.Payload)
	dst = append(dst, ProtocolVersion)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(length)) //nolint:gosec // bounded by MaxFrameLength above
	dst = append(dst, byte(f.Op))
	dst = append(dst, f.ID[:]...)
	return append(dst, f.Payload...), nil
}

// WriteFrame encodes f and writes it to w in a single Write, so a writer
// shared under a lock never interleaves two frames.
func WriteFrame(w io.Writer, f Frame) error {
	encoded, err := f.AppendTo(make([]byte, 0, HeaderSize+len(f.Payload)))
	if err != nil {
		return err
	}
	if _, err := w.Write(encoded); err != nil {
		return fmt.Errorf("writing %s frame: %w", f.Op, err)
	}
	return nil
}

// ReadFrame reads one frame from r. The version and length are checked before
// the body is allocated, so a hostile length cannot make the reader allocate.
func ReadFrame(r io.Reader) (Frame, error) {
	var header [1 + 4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, fmt.Errorf("reading frame header: %w", err)
	}
	if header[0] != ProtocolVersion {
		return Frame{}, fmt.Errorf("frame version %d, this driver speaks %d: %w", header[0], ProtocolVersion, ErrBadFrame)
	}
	length := binary.LittleEndian.Uint32(header[1:])
	if length < minFrameLength {
		return Frame{}, fmt.Errorf("frame length %d is below the %d bytes an opcode and request ID need: %w",
			length, minFrameLength, ErrBadFrame)
	}
	if length > MaxFrameLength {
		return Frame{}, fmt.Errorf("frame length %d: %w", length, ErrFrameTooLarge)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		return Frame{}, fmt.Errorf("reading %d-byte frame body: %w", length, err)
	}
	frame := Frame{Op: Opcode(body[0]), Payload: body[minFrameLength:]}
	copy(frame.ID[:], body[1:minFrameLength])
	return frame, nil
}
