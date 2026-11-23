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
	"testing"
)

type sample struct {
	Name    string   `bson:"name"`
	Count   int64    `bson:"count"`
	Tags    []string `bson:"tags"`
	Omitted string   `bson:"omitted,omitempty"`
}

func TestRoundTrip(t *testing.T) {
	encoded, err := Marshal(sample{Name: "n", Count: 7, Tags: []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded sample
	if err := Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Name != "n" || decoded.Count != 7 || len(decoded.Tags) != 2 {
		t.Fatalf("decoded %+v", decoded)
	}
	var fields map[string]any
	if err := Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if _, present := fields["omitted"]; present {
		t.Fatal("omitempty must keep the field off the wire")
	}
}

func TestIntegerWidthsDecodeIntoOneType(t *testing.T) {
	// Servers send int32 or int64 depending on magnitude; a driver decodes both.
	for _, document := range []map[string]any{{"count": int32(5)}, {"count": int64(5)}} {
		encoded, err := Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		var decoded sample
		if err := Unmarshal(encoded, &decoded); err != nil || decoded.Count != 5 {
			t.Fatalf("%T: count = %d, err = %v", document["count"], decoded.Count, err)
		}
	}
}

func TestEmptyPayloadLeavesTheTargetAlone(t *testing.T) {
	decoded := sample{Name: "kept"}
	if err := Unmarshal(nil, &decoded); err != nil || decoded.Name != "kept" {
		t.Fatalf("decoded %+v, err %v", decoded, err)
	}
}

func TestMalformedPayloadIsAnErrorNotAPanic(t *testing.T) {
	for _, payload := range [][]byte{{1}, {5, 0, 0, 0}, {0xFF, 0xFF, 0xFF, 0x7F, 0}} {
		var decoded sample
		if err := Unmarshal(payload, &decoded); err == nil {
			t.Errorf("payload %v decoded without error", payload)
		}
	}
	if _, err := Marshal(make(chan int)); err == nil {
		t.Error("an unencodable value must be an error")
	}
}
