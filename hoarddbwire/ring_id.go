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
	"fmt"
	"unicode/utf8"
)

// MaxRingIDBytes is the most an encoded string id may take on the wire,
// counting its tag, its escapes and its terminator.
const MaxRingIDBytes = 1024

// ringIDStringTag opens an encoded string id; A1 writes and reads no other tag.
const ringIDStringTag = 0x50

// EncodeStringID encodes a string id as entry-ring-conventions.md section 2
// has it: the tag 0x50, the bytes with each 0x00 written as 00 FF, then 00 00.
// An id that is not UTF-8 or that takes more than MaxRingIDBytes encoded is
// ErrRingRequest.
func EncodeStringID(id string) ([]byte, error) {
	if !utf8.ValidString(id) {
		return nil, ringRequestRefusal("id", "not valid UTF-8", "Send the id as UTF-8 text")
	}
	encoded := make([]byte, 0, len(id)+3)
	encoded = append(encoded, ringIDStringTag)
	for index := 0; index < len(id); index++ {
		if id[index] == 0x00 {
			encoded = append(encoded, 0x00, 0xFF)
			continue
		}
		encoded = append(encoded, id[index])
	}
	encoded = append(encoded, 0x00, 0x00)
	if len(encoded) > MaxRingIDBytes {
		return nil, ringRequestRefusal("id", fmt.Sprintf("takes %d bytes encoded, over the %d a ring id may take (each zero byte takes two)", len(encoded), MaxRingIDBytes),
			"Use a shorter id")
	}
	return encoded, nil
}

// DecodeStringID reads one string id from the front of encoded and returns it
// with the bytes it took; what follows is the next member's. Another tag, an
// escape that is not 00 FF, a missing terminator, an id over MaxRingIDBytes
// encoded or one that is not UTF-8 is ErrRingRequest.
func DecodeStringID(encoded []byte) (id string, width int, err error) {
	if len(encoded) == 0 {
		return "", 0, ringRequestRefusal("id", "missing", "Send the encoded id after the header: tag 0x50, the bytes, 00 00")
	}
	if encoded[0] != ringIDStringTag {
		return "", 0, ringRequestRefusal("id", fmt.Sprintf("the tag is 0x%02x, and only the string tag 0x50 is supported", encoded[0]), "Use a string id")
	}
	decoded := make([]byte, 0, len(encoded))
	for index := 1; index < len(encoded); index++ {
		if index+2 > MaxRingIDBytes { // a terminator here would end past the cap
			return "", 0, ringRequestRefusal("id", fmt.Sprintf("no terminator within the %d bytes a ring id may take", MaxRingIDBytes), "End the id with 00 00")
		}
		if encoded[index] != 0x00 {
			decoded = append(decoded, encoded[index])
			continue
		}
		if index+1 >= len(encoded) {
			return "", 0, ringRequestRefusal("id", "ends after a zero byte", "Write each zero byte as 00 FF and end the id with 00 00")
		}
		switch encoded[index+1] {
		case 0x00:
			if !utf8.Valid(decoded) {
				return "", 0, ringRequestRefusal("id", "not valid UTF-8", "Send the id as UTF-8 text")
			}
			return string(decoded), index + 2, nil
		case 0xFF:
			decoded = append(decoded, 0x00)
			index++
		default:
			return "", 0, ringRequestRefusal("id", fmt.Sprintf("a zero byte is followed by 0x%02x, not 00 or FF", encoded[index+1]), "Write each zero byte as 00 FF and end the id with 00 00")
		}
	}
	return "", 0, ringRequestRefusal("id", "no terminator", "End the id with 00 00")
}
