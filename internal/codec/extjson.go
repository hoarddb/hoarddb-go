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

package codec

import (
	"fmt"

	"github.com/hoarddb/hoarddb-bson/bson"
)

// Raw is one BSON document kept as bytes. Written, it is an embedded document;
// read, it is the document's bytes exactly as they arrived, so a stored
// document passes through without being re-encoded (handbook/wire.md section 3).
type Raw = bson.Raw

// RawValue is one BSON value of any type, kept with its type. It carries the
// few fields whose type the server does not pin.
type RawValue = bson.RawValue

// FromExtendedJSON encodes canonical Extended JSON (version 2) as one BSON
// document. The vectors state their payloads this way: integer widths, binary
// subtypes and datetimes are explicit in it.
func FromExtendedJSON(text []byte) ([]byte, error) {
	var document bson.Raw
	if err := bson.UnmarshalExtJSON(text, true, &document); err != nil {
		return nil, fmt.Errorf("reading Extended JSON: %w", err)
	}
	if len(document) == 0 {
		return nil, fmt.Errorf("reading Extended JSON: %q is not a document", text)
	}
	return document, nil
}

// ToExtendedJSON renders one BSON document as canonical Extended JSON.
func ToExtendedJSON(payload []byte) ([]byte, error) {
	rendered, err := bson.MarshalExtJSON(bson.Raw(payload), true, false)
	if err != nil {
		return nil, fmt.Errorf("writing Extended JSON: %w", err)
	}
	return rendered, nil
}

// ToRelaxedExtendedJSON renders one BSON document as relaxed Extended JSON,
// the readable form: integers and datetimes are plain, for a log line. A
// payload that is not a document is shown as its hex, because a String method
// has no way to fail.
func ToRelaxedExtendedJSON(payload []byte) string {
	rendered, err := bson.MarshalExtJSON(bson.Raw(payload), false, false)
	if err != nil {
		return fmt.Sprintf("<not a BSON document: %x>", payload)
	}
	return string(rendered)
}

// Validate reports whether payload is one well-formed BSON document.
func Validate(payload []byte) error {
	if err := bson.Raw(payload).Validate(); err != nil {
		return fmt.Errorf("validating BSON: %w", err)
	}
	return nil
}

// Lookup finds the value at path in a document, descending through embedded
// documents by field name. It reports false for a path that is not there.
func Lookup(document []byte, path ...string) (RawValue, bool) {
	value, err := bson.Raw(document).LookupErr(path...)
	if err != nil {
		return RawValue{}, false
	}
	return value, true
}

// ValueToRelaxedExtendedJSON renders one BSON value as relaxed Extended JSON.
func ValueToRelaxedExtendedJSON(value RawValue) string {
	wrapper, err := bson.Marshal(bson.D{{Key: "v", Value: value}})
	if err != nil {
		return fmt.Sprintf("<unprintable %s value>", value.Type)
	}
	rendered := ToRelaxedExtendedJSON(wrapper)
	const prefix, suffix = `{"v":`, `}`
	if len(rendered) >= len(prefix)+len(suffix) && rendered[:len(prefix)] == prefix && rendered[len(rendered)-1:] == suffix {
		return rendered[len(prefix) : len(rendered)-len(suffix)]
	}
	return rendered
}
