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
	"bytes"
	"strings"
	"testing"
)

func TestExtendedJSONRoundTrip(t *testing.T) {
	text := `{"Born": {"$numberInt": "1815"}, "Name": "Ada", "Tags": ["x", {"$numberLong": "7"}], "Nested": {"At": {"$date": {"$numberLong": "1767225600000"}}}}`
	encoded, err := FromExtendedJSON([]byte(text))
	if err != nil {
		t.Fatalf("FromExtendedJSON: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("FromExtendedJSON returned no BSON for a document")
	}
	back, err := ToExtendedJSON(encoded)
	if err != nil {
		t.Fatalf("ToExtendedJSON: %v", err)
	}
	for _, want := range []string{`"$numberInt":"1815"`, `"$numberLong":"7"`, `"$date"`, `"Name":"Ada"`} {
		if !strings.Contains(strings.ReplaceAll(string(back), " ", ""), want) {
			t.Errorf("canonical Extended JSON %s lacks %s: an integer width or type was lost", back, want)
		}
	}
	again, err := FromExtendedJSON(back)
	if err != nil || !bytes.Equal(again, encoded) {
		t.Fatalf("a second pass changed the document: %x then %x (err %v)", encoded, again, err)
	}
}

func TestExtendedJSONRefusesWhatIsNotADocument(t *testing.T) {
	for _, text := range []string{``, `[1,2]`, `{"a":`, `"text"`} {
		if _, err := FromExtendedJSON([]byte(text)); err == nil {
			t.Errorf("%q was accepted as a document", text)
		}
	}
	if _, err := ToExtendedJSON([]byte{1, 2, 3}); err == nil {
		t.Error("garbage bytes were accepted as BSON")
	}
}

func TestRawKeepsADocumentByteForByte(t *testing.T) {
	type holder struct {
		Doc  Raw   `bson:"doc,omitempty"`
		Docs []Raw `bson:"docs,omitempty"`
	}
	inner, err := Marshal(map[string]any{"b": int32(2), "a": "x"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Marshal(holder{Doc: inner, Docs: []Raw{inner, inner}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded holder
	if err := Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	var nested struct {
		Doc map[string]any `bson:"doc"`
	}
	if err := Unmarshal(encoded, &nested); err != nil || nested.Doc["a"] != "x" {
		t.Fatalf("Raw is not written as an embedded document: %+v (err %v)", nested, err)
	}
	if !bytes.Equal(decoded.Doc, inner) || len(decoded.Docs) != 2 || !bytes.Equal(decoded.Docs[1], inner) {
		t.Fatalf("a nested document was re-encoded: %x, want %x", decoded.Doc, inner)
	}
}
