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
	"encoding/binary"
	"errors"
	"fmt"
)

// GET_FAST is the one request whose payload is not BSON (handbook/wire.md
// section 5): 1-byte database length, database, 1-byte bucket length, bucket,
// 2-byte little-endian key length, key.

// Limits of a GET_FAST request: names are at most 255 bytes, a key 65,535.
const (
	maxGetFastName = 255
	maxGetFastKey  = 65535
)

// ErrGetFastLimit is a GET_FAST request whose names do not fit its compact
// form; the caller uses GET instead.
var ErrGetFastLimit = errors.New("GET_FAST name or key too long")

// GetFastRequest is the compact read.
type GetFastRequest struct {
	Database string
	Bucket   string
	Key      string
}

// MarshalBinary encodes the request. A name over 255 bytes or a key over
// 65,535 is ErrGetFastLimit, and nothing is sent.
func (request GetFastRequest) MarshalBinary() ([]byte, error) {
	if len(request.Database) > maxGetFastName || len(request.Bucket) > maxGetFastName || len(request.Key) > maxGetFastKey {
		return nil, fmt.Errorf("database %d bytes, bucket %d, key %d against the limits %d, %d and %d; use GET: %w",
			len(request.Database), len(request.Bucket), len(request.Key), maxGetFastName, maxGetFastName, maxGetFastKey, ErrGetFastLimit)
	}
	encoded := make([]byte, 0, 4+len(request.Database)+len(request.Bucket)+len(request.Key))
	encoded = append(encoded, byte(len(request.Database)))
	encoded = append(encoded, request.Database...)
	encoded = append(encoded, byte(len(request.Bucket)))
	encoded = append(encoded, request.Bucket...)
	encoded = binary.LittleEndian.AppendUint16(encoded, uint16(len(request.Key))) //nolint:gosec // bounded by maxGetFastKey above
	return append(encoded, request.Key...), nil
}

// UnmarshalBinary decodes a GET_FAST payload.
func (request *GetFastRequest) UnmarshalBinary(payload []byte) error {
	database, rest, err := lengthPrefixed(payload, 1, "database")
	if err != nil {
		return err
	}
	bucket, rest, err := lengthPrefixed(rest, 1, "bucket")
	if err != nil {
		return err
	}
	key, rest, err := lengthPrefixed(rest, 2, "key")
	if err != nil {
		return err
	}
	if len(rest) != 0 {
		return fmt.Errorf("GET_FAST payload has %d bytes after the key", len(rest))
	}
	*request = GetFastRequest{Database: string(database), Bucket: string(bucket), Key: string(key)}
	return nil
}

func lengthPrefixed(payload []byte, width int, what string) (value, rest []byte, err error) {
	if len(payload) < width {
		return nil, nil, fmt.Errorf("GET_FAST payload ends before the %s length", what)
	}
	length := int(payload[0])
	if width == 2 {
		length = int(binary.LittleEndian.Uint16(payload))
	}
	if len(payload) < width+length {
		return nil, nil, fmt.Errorf("GET_FAST payload is %d bytes, too short for a %d-byte %s", len(payload), length, what)
	}
	return payload[width : width+length], payload[width+length:], nil
}

// Status bytes of a GET_FAST answer.
const (
	getFastFound    = 0x00
	getFastNotFound = 0x01
)

// GetFastResponse is the answer to GET_FAST: a status byte, and the stored
// document when found. A miss is not an error.
type GetFastResponse struct {
	Found    bool
	Document Document
}

// MarshalBinary encodes the answer as the server writes it.
func (response GetFastResponse) MarshalBinary() ([]byte, error) {
	if !response.Found {
		return []byte{getFastNotFound}, nil
	}
	return append([]byte{getFastFound}, response.Document...), nil
}

// UnmarshalBinary decodes a GET_FAST_RESPONSE payload.
func (response *GetFastResponse) UnmarshalBinary(payload []byte) error {
	if len(payload) == 0 {
		return errors.New("GET_FAST answer is empty; it carries at least a status byte")
	}
	switch payload[0] {
	case getFastFound:
		*response = GetFastResponse{Found: true, Document: Document(append([]byte(nil), payload[1:]...))}
	case getFastNotFound:
		*response = GetFastResponse{}
	default:
		return fmt.Errorf("GET_FAST answer has status byte 0x%02X, expected 0x00 (found) or 0x01 (not found)", payload[0])
	}
	return nil
}
