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
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestFrameEncodingGolden(t *testing.T) {
	id := RequestID{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	tests := []struct {
		name  string
		frame Frame
		want  []byte
	}{
		{
			name:  "empty payload PING",
			frame: Frame{Op: OpPing, ID: id},
			// version 1, length 17 little-endian, opcode 0xA0, the ID.
			want: append([]byte{0x01, 0x11, 0x00, 0x00, 0x00, 0xA0}, id[:]...),
		},
		{
			name:  "three-byte payload",
			frame: Frame{Op: OpAuth, ID: id, Payload: []byte{0xAA, 0xBB, 0xCC}},
			want:  append(append([]byte{0x01, 0x14, 0x00, 0x00, 0x00, 0x01}, id[:]...), 0xAA, 0xBB, 0xCC),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := test.frame.AppendTo(nil)
			if err != nil {
				t.Fatalf("AppendTo: %v", err)
			}
			if !bytes.Equal(got, test.want) {
				t.Fatalf("encoded %x, want %x", got, test.want)
			}
			if len(got) != HeaderSize+len(test.frame.Payload) {
				t.Fatalf("encoded length %d, HeaderSize+payload is %d", len(got), HeaderSize+len(test.frame.Payload))
			}
			decoded, err := ReadFrame(bytes.NewReader(got))
			if err != nil {
				t.Fatalf("ReadFrame of our own encoding: %v", err)
			}
			if decoded.Op != test.frame.Op || decoded.ID != test.frame.ID || !bytes.Equal(decoded.Payload, test.frame.Payload) {
				t.Fatalf("round trip changed the frame: got %+v want %+v", decoded, test.frame)
			}
		})
	}
}

func TestHeaderSizeIsTwentyTwo(t *testing.T) {
	if HeaderSize != 22 {
		t.Fatalf("HeaderSize = %d; the protocol header is 22 bytes", HeaderSize)
	}
}

func TestMaxPayloadFitsExactlyInOneFrame(t *testing.T) {
	atLimit := Frame{Op: OpPut, Payload: make([]byte, MaxPayload)}
	encoded, err := atLimit.AppendTo(nil)
	if err != nil {
		t.Fatalf("a payload of exactly MaxPayload must encode: %v", err)
	}
	if got := binary.LittleEndian.Uint32(encoded[1:5]); got != MaxFrameLength {
		t.Fatalf("Length field = %d, want exactly %d (32 MiB)", got, MaxFrameLength)
	}
	overLimit := Frame{Op: OpPut, Payload: make([]byte, MaxPayload+1)}
	if _, err := overLimit.AppendTo(nil); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("one byte over the limit: err = %v, want ErrFrameTooLarge", err)
	}
}

func TestWriteFrameRefusesOversizedBeforeWriting(t *testing.T) {
	var sink bytes.Buffer
	err := WriteFrame(&sink, Frame{Op: OpPut, Payload: make([]byte, MaxPayload+1)})
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
	if sink.Len() != 0 {
		t.Fatalf("an oversized frame wrote %d bytes; the server would have closed the connection", sink.Len())
	}
}

func TestReadFrameRejectsBadHeaders(t *testing.T) {
	header := func(version byte, length uint32) []byte {
		raw := []byte{version, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(raw[1:], length)
		return raw
	}
	tests := []struct {
		name    string
		input   []byte
		wantErr error
	}{
		{"wrong version", header(2, 17), ErrBadFrame},
		{"version zero", header(0, 17), ErrBadFrame},
		{"length below opcode plus ID", header(1, 16), ErrBadFrame},
		{"length zero", header(1, 0), ErrBadFrame},
		// Only the header is supplied: the check must fire before the reader
		// tries to read, let alone allocate, a 4 GiB body.
		{"length over the limit, header only", header(1, MaxFrameLength+1), ErrFrameTooLarge},
		{"length 4 GiB, header only", header(1, 0xFFFFFFFF), ErrFrameTooLarge},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ReadFrame(bytes.NewReader(test.input))
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("err = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestReadFrameTruncated(t *testing.T) {
	whole, err := Frame{Op: OpGet, Payload: []byte("payload")}.AppendTo(nil)
	if err != nil {
		t.Fatal(err)
	}
	for cut := 0; cut < len(whole); cut++ {
		_, err := ReadFrame(bytes.NewReader(whole[:cut]))
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("frame cut at %d of %d bytes: err = %v, want an EOF", cut, len(whole), err)
		}
	}
}

func TestReadFrameConsumesBackToBackFrames(t *testing.T) {
	var wire bytes.Buffer
	for _, payload := range []string{"one", "", "three"} {
		if err := WriteFrame(&wire, Frame{Op: OpGet, Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{"one", "", "three"} {
		got, err := ReadFrame(&wire)
		if err != nil {
			t.Fatalf("ReadFrame: %v", err)
		}
		if string(got.Payload) != want {
			t.Fatalf("payload %q, want %q", got.Payload, want)
		}
	}
	if _, err := ReadFrame(&wire); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last frame: err = %v, want EOF", err)
	}
}

func TestNewRequestIDIsVersion4AndUnique(t *testing.T) {
	seen := map[RequestID]bool{}
	for range 1000 {
		id, err := NewRequestID()
		if err != nil {
			t.Fatal(err)
		}
		if id[6]>>4 != 4 {
			t.Fatalf("ID %s: version nibble %x, want 4", id, id[6]>>4)
		}
		if id[8]>>6 != 0b10 {
			t.Fatalf("ID %s: variant bits %b, want 10", id, id[8]>>6)
		}
		if id.IsZero() || seen[id] {
			t.Fatalf("ID %s is zero or repeated", id)
		}
		seen[id] = true
	}
}

func TestOpcodeValuesArePinned(t *testing.T) {
	// The numeric values are the protocol; a change here is a wire change.
	pinned := map[Opcode]uint8{
		OpAuth: 0x01, OpAuthOK: 0x02, OpAuthFail: 0x03, OpPing: 0xA0, OpPong: 0xA1, OpError: 0xA2,
		OpHealthCheck: 0xA3, OpListNodes: 0xA4, OpGetResponse: 0x12, OpGetFast: 0xC1,
	}
	for op, want := range pinned {
		if uint8(op) != want {
			t.Errorf("%s = 0x%02X, want 0x%02X", op, uint8(op), want)
		}
	}
	if got := Opcode(0x7F).String(); got != "0x7F" {
		t.Errorf("unknown opcode String = %q, want 0x7F", got)
	}
}

type countingReader struct {
	reader io.Reader
	read   int
}

func (counter *countingReader) Read(buffer []byte) (int, error) {
	count, err := counter.reader.Read(buffer)
	counter.read += count
	return count, err
}

// headerVerdict is the decoder's contract for a 5-byte prefix, stated
// independently of it: the error the prefix alone must produce, or nil when it
// is a legal header (or too short to judge).
func headerVerdict(input []byte) error {
	if len(input) < 5 {
		return nil
	}
	if input[0] != ProtocolVersion {
		return ErrBadFrame
	}
	switch length := binary.LittleEndian.Uint32(input[1:5]); {
	case length < 17:
		return ErrBadFrame
	case length > MaxFrameLength:
		return ErrFrameTooLarge
	default:
		return nil
	}
}

// FuzzReadFrame holds the decoder to its contract on any input: it never
// panics, a header it refuses is refused from the 5-byte prefix alone (the body
// is neither read nor allocated), and a frame it accepts fits the protocol's
// cap and encodes back to the bytes it was read from.
func FuzzReadFrame(f *testing.F) {
	id := RequestID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	for _, frame := range []Frame{{Op: OpPing, ID: id}, {Op: OpGet, ID: id, Payload: []byte{5, 0, 0, 0, 0}}} {
		encoded, err := frame.AppendTo(nil)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(encoded)
		f.Add(encoded[:len(encoded)-1]) // truncated
	}
	f.Add([]byte{2, 0x11, 0, 0, 0})                // a version the protocol does not have
	f.Add([]byte{1, 0x10, 0, 0, 0})                // below the 17 bytes an opcode and ID need
	f.Add([]byte{1, 0x01, 0, 0, 0x02})             // 32 MiB + 1
	f.Add([]byte{1, 0xFF, 0xFF, 0xFF, 0xFF, 0x10}) // a hostile length word
	f.Add([]byte{})                                // nothing at all
	f.Fuzz(func(t *testing.T, input []byte) {
		counter := &countingReader{reader: bytes.NewReader(input)}
		frame, err := ReadFrame(counter)
		if want := headerVerdict(input); want != nil && !errors.Is(err, want) {
			t.Fatalf("header %x was refused with %v, want %v", input[:min(len(input), 5)], err, want)
		}
		if err != nil {
			if (errors.Is(err, ErrBadFrame) || errors.Is(err, ErrFrameTooLarge)) && counter.read > 5 {
				t.Fatalf("refused %v after reading %d bytes; the 5-byte prefix decides it", err, counter.read)
			}
			return
		}
		if len(frame.Payload) > MaxPayload {
			t.Fatalf("accepted a %d-byte payload, over the %d the protocol allows", len(frame.Payload), MaxPayload)
		}
		encoded, err := frame.AppendTo(nil)
		if err != nil || !bytes.Equal(encoded, input[:counter.read]) {
			t.Fatalf("accepted frame encodes to %x (err %v), read from %x", encoded, err, input[:counter.read])
		}
	})
}
