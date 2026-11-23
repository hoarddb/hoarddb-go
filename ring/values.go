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
	"fmt"
	"strings"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// typedValue is a field value as the wire carries it: the BSON type byte, then the value's
// bytes.
func typedValue(value any) ([]byte, error) {
	raw, err := bson.Marshal(bson.D{{Key: "v", Value: value}})
	if err != nil {
		return nil, fmt.Errorf("encoding %T as a BSON value: %w", value, err)
	}
	member := bson.Raw(raw).Lookup("v")
	return append([]byte{byte(member.Type)}, member.Value...), nil
}

// elementBytes is the BSON document of a store element: bson.Raw and []byte pass through as
// they are, anything else is marshalled.
func elementBytes(element any) ([]byte, error) {
	switch typed := element.(type) {
	case bson.Raw:
		return typed, nil
	case []byte:
		return typed, nil
	}
	raw, err := bson.Marshal(element)
	if err != nil {
		return nil, fmt.Errorf("encoding the element (%T) as a BSON document: %w", element, err)
	}
	return raw, nil
}

// filterBytes is the BSON document of a list or count filter, or nil for none.
func filterBytes(filter any) ([]byte, error) {
	if filter == nil {
		return nil, nil
	}
	return elementBytes(filter)
}

// encodedID is the key-component encoding of a document id and its typed string value. The id
// must be valid UTF-8 and fit the id cap encoded.
func encodedID(id string) (encoded, typed []byte, err error) {
	encoded, err = hoarddbwire.EncodeStringID(id)
	if err != nil {
		return nil, nil, fmt.Errorf("encoding document id %q: %w", id, err)
	}
	typed, err = typedValue(id)
	if err != nil {
		return nil, nil, err
	}
	return encoded, typed, nil
}

// decodeID inverts the key-component encoding of a string id; any other encoding is shown in hex.
func decodeID(encoded []byte) string {
	if len(encoded) >= 3 && encoded[0] == ringwire.TagString {
		body := encoded[1 : len(encoded)-2]
		return strings.ReplaceAll(string(body), "\x00\xff", "\x00")
	}
	return fmt.Sprintf("%x", encoded)
}

func isStoreType(word string) bool { return strings.HasSuffix(word, "Store") }
