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
	"fmt"
	"time"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// Document is one stored document: raw BSON, decoded only when the caller
// asks. The server does not keep a document's field order, so do not rely on
// the order fields come back in. Its key is in the field "_id_".
type Document []byte

// Map is a literal document: field names to values, written as a map.
type Map = map[string]any

// Decode reads the document into into, a pointer to a struct with `bson:"..."`
// tags or to a [Map].
func (d Document) Decode(into any) error {
	return codec.Unmarshal(d, into)
}

// Lookup finds the value at path, descending through embedded documents by
// field name, and reports false when it is not there.
//
//	city, ok := doc.Lookup("Address", "City")
func (d Document) Lookup(path ...string) (Value, bool) {
	found, ok := codec.Lookup(d, path...)
	if !ok {
		return Value{}, false
	}
	return Value{raw: found}, true
}

// String is the document in relaxed Extended JSON, for logs and error
// messages. It is not a stable format: use [Document.Decode] to read a
// document in a program.
func (d Document) String() string { return codec.ToRelaxedExtendedJSON(d) }

// Value is one element of a [Document]. Read it with the accessor for the type
// you expect; each reports false when the element holds another type, so a
// wrong guess is never a silent zero.
type Value struct{ raw codec.RawValue }

// String is the value in relaxed Extended JSON, for logs.
func (v Value) String() string { return codec.ValueToRelaxedExtendedJSON(v.raw) }

// AsString returns the value when it is a string.
func (v Value) AsString() (string, bool) { return v.raw.StringValueOK() }

// Int64 returns the value when it is an integer of either width, which the
// server writes as a 32-bit or a 64-bit integer depending on its size.
func (v Value) Int64() (int64, bool) {
	if wide, ok := v.raw.Int64OK(); ok {
		return wide, true
	}
	if narrow, ok := v.raw.Int32OK(); ok {
		return int64(narrow), true
	}
	return 0, false
}

// Float64 returns the value when it is a floating-point number or an integer.
func (v Value) Float64() (float64, bool) {
	if number, ok := v.raw.DoubleOK(); ok {
		return number, true
	}
	if whole, ok := v.Int64(); ok {
		return float64(whole), true
	}
	return 0, false
}

// Bool returns the value when it is a boolean.
func (v Value) Bool() (bool, bool) { return v.raw.BooleanOK() }

// Time returns the value when it is a datetime, to the millisecond.
func (v Value) Time() (time.Time, bool) { return v.raw.TimeOK() }

// Decode reads the value into into, a pointer to the Go type it should become.
func (v Value) Decode(into any) error { return v.raw.Unmarshal(into) }

// encodeDocument is the one place a document the caller passes becomes BSON: a
// [Document] is sent as it is, anything else is written with the codec. The
// refusal names the fix, because the usual mistake is a value that is not a
// document (a string, a list, nil).
func encodeDocument(op, target string, doc any) (codec.Raw, error) {
	var encoded []byte
	switch typed := doc.(type) {
	case nil:
		return nil, refusal(op, target, "the document is nil, so nothing was sent. Pass a struct, a hoarddb.Map or a hoarddb.Document")
	case Document:
		encoded = typed
	case *Document:
		if typed == nil {
			return nil, refusal(op, target, "the document is a nil *hoarddb.Document, so nothing was sent")
		}
		encoded = *typed
	default:
		written, err := codec.Marshal(doc)
		if err != nil {
			return nil, refusal(op, target, fmt.Sprintf("a value of type %T is not a document (%v), so nothing was sent. "+
				"A document is a struct with bson tags, a hoarddb.Map, or a hoarddb.Document", doc, err))
		}
		encoded = written
	}
	if err := codec.Validate(encoded); err != nil {
		return nil, refusal(op, target, fmt.Sprintf("the document is not well-formed BSON (%v), so nothing was sent", err))
	}
	return encoded, nil
}
