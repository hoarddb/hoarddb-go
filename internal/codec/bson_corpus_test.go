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

package codec_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-bson/bson"
	"github.com/hoarddb/hoarddb-bson/bson/primitive"

	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// bsonCorpusPath is clients/testdata/bson-vectors/v1.json from this package
// directory (handbook/testing.md section 1.3). The driver keeps no copy; the
// file is data and is read, never written.
const bsonCorpusPath = "../../testdata/bson-vectors/v1.json"

// bsonCorpusDepth is limits.max_depth of the corpus: the root is level 1 and a
// decoder refuses level 101.
const bsonCorpusDepth = 100

// largeTestsEnv gates the 30 MiB size vectors, which hold the document, its
// bytes and a copy at once (a peak near 300 MiB with the 30 MiB cases).
const largeTestsEnv = "RINGCLI_LARGE_TESTS"

type bsonVector struct {
	Name                 string          `json:"name"`
	Kind                 string          `json:"kind"`
	Document             json.RawMessage `json:"document"`
	LastWinsDocument     json.RawMessage `json:"last_wins_document"`
	Hex                  string          `json:"hex"`
	ReencodesIdentically bool            `json:"reencodes_identically"`
	Refusal              string          `json:"refusal"`
	Instant              string          `json:"instant"`
	ExpectedMillis       string          `json:"expected_millis"`
	Recipe               struct {
		Key       string `json:"key"`
		Fill      string `json:"fill"`
		FillBytes int    `json:"fill_bytes"`
	} `json:"recipe"`
	DocumentBytes int    `json:"document_bytes"`
	SHA256        string `json:"sha256"`
}

type bsonCorpus struct {
	Format  string       `json:"format"`
	Vectors []bsonVector `json:"vectors"`
}

// loadBSONCorpus reads the corpus and fails the test, naming the path and the
// fix, when the file is missing or is not the format this replay was written
// for.
func loadBSONCorpus(t *testing.T, path string) []bsonVector {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("cannot read the BSON corpus %s: %v. It is testdata/bson-vectors/v1.json beside this module; run the test from a checkout that has testdata", path, err)
	}
	var corpus bsonCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatalf("BSON corpus %s is not JSON: %v", path, err)
	}
	if corpus.Format != "hoarddb-bson-vectors/1" {
		t.Fatalf("BSON corpus %s has format %q; this replay reads hoarddb-bson-vectors/1", path, corpus.Format)
	}
	return corpus.Vectors
}

// parseTyped decodes a typed JSON value without losing key order or numbers.
func parseTyped(t *testing.T, raw json.RawMessage) any {
	t.Helper()
	var typed any
	if err := json.Unmarshal(raw, &typed); err != nil {
		t.Fatalf("typed value %s: %v", raw, err)
	}
	return typed
}

// single returns the tag and body of a one-key typed object.
func single(t *testing.T, typed any) (string, any) {
	t.Helper()
	object, ok := typed.(map[string]any)
	if !ok || len(object) != 1 {
		t.Fatalf("typed value %v is not a one-key object", typed)
	}
	for tag, body := range object {
		return tag, body
	}
	return "", nil
}

func mustInt(t *testing.T, text string) int64 {
	t.Helper()
	number, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		t.Fatalf("integer %q: %v", text, err)
	}
	return number
}

func mustDouble(t *testing.T, text string) float64 {
	t.Helper()
	switch text {
	case "NaN":
		return math.Float64frombits(0x7ff8000000000000)
	case "Infinity":
		return math.Inf(1)
	case "-Infinity":
		return math.Inf(-1)
	case "-0":
		return math.Copysign(0, -1)
	}
	number, err := strconv.ParseFloat(text, 64)
	if err != nil {
		t.Fatalf("double %q: %v", text, err)
	}
	return number
}

func mustObjectID(t *testing.T, text string) primitive.ObjectID {
	t.Helper()
	id, err := primitive.ObjectIDFromHex(text)
	if err != nil {
		t.Fatalf("object id %q: %v", text, err)
	}
	return id
}

// toBSON builds the library value a typed JSON value stands for, from the
// forms in the corpus header (value_forms).
func toBSON(t *testing.T, typed any) any {
	t.Helper()
	switch value := typed.(type) {
	case nil:
		return nil
	case bool, string:
		return value
	case []any:
		array := make(bson.A, 0, len(value))
		for _, element := range value {
			array = append(array, toBSON(t, element))
		}
		return array
	}
	tag, body := single(t, typed)
	switch tag {
	case "$doc":
		return toDocument(t, body)
	case "$numberInt":
		return int32(mustInt(t, body.(string)))
	case "$numberLong":
		return mustInt(t, body.(string))
	case "$numberDouble":
		return mustDouble(t, body.(string))
	case "$numberDecimal":
		decimal, err := primitive.ParseDecimal128(body.(string))
		if err != nil {
			t.Fatalf("decimal %q: %v", body, err)
		}
		return decimal
	case "$date":
		return primitive.DateTime(mustInt(t, body.(map[string]any)["$numberLong"].(string)))
	case "$binary":
		fields := body.(map[string]any)
		subtype, err := strconv.ParseUint(fields["subType"].(string), 16, 8)
		if err != nil {
			t.Fatalf("binary subtype %v: %v", fields["subType"], err)
		}
		data, err := hex.DecodeString(fields["hex"].(string))
		if err != nil {
			t.Fatalf("binary hex %v: %v", fields["hex"], err)
		}
		return primitive.Binary{Subtype: byte(subtype), Data: data}
	case "$oid":
		return mustObjectID(t, body.(string))
	case "$timestamp":
		fields := body.(map[string]any)
		return primitive.Timestamp{T: uint32(fields["t"].(float64)), I: uint32(fields["i"].(float64))}
	case "$regularExpression":
		fields := body.(map[string]any)
		return primitive.Regex{Pattern: fields["pattern"].(string), Options: fields["options"].(string)}
	case "$code":
		return primitive.JavaScript(body.(string))
	case "$codeWithScope":
		fields := body.(map[string]any)
		return primitive.CodeWithScope{Code: primitive.JavaScript(fields["code"].(string)), Scope: toBSON(t, fields["scope"])}
	case "$symbol":
		return primitive.Symbol(body.(string))
	case "$undefined":
		return primitive.Undefined{}
	case "$minKey":
		return primitive.MinKey{}
	case "$maxKey":
		return primitive.MaxKey{}
	case "$dbPointer":
		fields := body.(map[string]any)
		return primitive.DBPointer{DB: fields["namespace"].(string), Pointer: mustObjectID(t, fields["oid"].(string))}
	}
	t.Fatalf("corpus uses the typed form %s, which this replay does not know", tag)
	return nil
}

// toDocument builds an ordered document; a repeated key stays repeated.
func toDocument(t *testing.T, body any) bson.D {
	t.Helper()
	pairs, ok := body.([]any)
	if !ok {
		t.Fatalf("$doc body %v is not a list of pairs", body)
	}
	document := make(bson.D, 0, len(pairs))
	for _, pair := range pairs {
		entry := pair.([]any)
		document = append(document, bson.E{Key: entry[0].(string), Value: toBSON(t, entry[1])})
	}
	return document
}

func decimalText(t *testing.T, decimal primitive.Decimal128) string {
	t.Helper()
	high, low := decimal.GetBytes()
	return fmt.Sprintf("%016x%016x", high, low)
}

// canonDouble renders a double by its bits, except that every NaN is one value:
// the corpus says a NaN with a payload, a sign or a signaling bit "reads as
// NaN" (decode/double-*-nan), so a decoder is not asked to keep the payload.
func canonDouble(number float64) string {
	if math.IsNaN(number) {
		return "dbl:NaN"
	}
	return fmt.Sprintf("dbl:%016x", math.Float64bits(number))
}

// canonTyped renders a typed JSON value as a canonical string. It is written
// apart from toBSON on purpose: a decode compares this rendering of the
// expectation with canonGo of what the library returned, so neither side
// routes the expectation through the library's encoder.
func canonTyped(t *testing.T, typed any) string {
	t.Helper()
	switch value := typed.(type) {
	case nil:
		return "null"
	case bool:
		return strconv.FormatBool(value)
	case string:
		return strconv.Quote(value)
	case []any:
		parts := make([]string, len(value))
		for index, element := range value {
			parts[index] = canonTyped(t, element)
		}
		return "[" + strings.Join(parts, ",") + "]"
	}
	tag, body := single(t, typed)
	switch tag {
	case "$doc":
		pairs := body.([]any)
		parts := make([]string, len(pairs))
		for index, pair := range pairs {
			entry := pair.([]any)
			parts[index] = strconv.Quote(entry[0].(string)) + ":" + canonTyped(t, entry[1])
		}
		return "{" + strings.Join(parts, ",") + "}"
	case "$numberInt":
		return "i32:" + strconv.FormatInt(mustInt(t, body.(string)), 10)
	case "$numberLong":
		return "i64:" + strconv.FormatInt(mustInt(t, body.(string)), 10)
	case "$numberDouble":
		return canonDouble(mustDouble(t, body.(string)))
	case "$numberDecimal":
		return "dec:" + decimalText(t, toBSON(t, typed).(primitive.Decimal128))
	case "$date":
		return "date:" + body.(map[string]any)["$numberLong"].(string)
	case "$binary":
		fields := body.(map[string]any)
		return fmt.Sprintf("bin:%s:%s", fields["subType"], fields["hex"])
	case "$oid":
		return "oid:" + body.(string)
	case "$timestamp":
		fields := body.(map[string]any)
		return fmt.Sprintf("ts:%d:%d", uint32(fields["t"].(float64)), uint32(fields["i"].(float64)))
	case "$regularExpression":
		fields := body.(map[string]any)
		return fmt.Sprintf("re:%q:%q", fields["pattern"], fields["options"])
	case "$code":
		return "js:" + strconv.Quote(body.(string))
	case "$codeWithScope":
		fields := body.(map[string]any)
		return fmt.Sprintf("jsscope:%q:%s", fields["code"], canonTyped(t, fields["scope"]))
	case "$symbol":
		return "sym:" + strconv.Quote(body.(string))
	case "$undefined":
		return "undefined"
	case "$minKey":
		return "minkey"
	case "$maxKey":
		return "maxkey"
	case "$dbPointer":
		fields := body.(map[string]any)
		return fmt.Sprintf("dbptr:%q:%s", fields["namespace"], fields["oid"])
	}
	t.Fatalf("corpus uses the typed form %s, which this replay does not know", tag)
	return ""
}

// canonGo renders what the library decoded in the same form as canonTyped.
func canonGo(t *testing.T, decoded any) string {
	t.Helper()
	switch value := decoded.(type) {
	case nil:
		return "null"
	case primitive.Null:
		return "null"
	case bool:
		return strconv.FormatBool(value)
	case string:
		return strconv.Quote(value)
	case bson.A:
		parts := make([]string, len(value))
		for index, element := range value {
			parts[index] = canonGo(t, element)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case bson.D:
		parts := make([]string, len(value))
		for index, entry := range value {
			parts[index] = strconv.Quote(entry.Key) + ":" + canonGo(t, entry.Value)
		}
		return "{" + strings.Join(parts, ",") + "}"
	case int32:
		return "i32:" + strconv.FormatInt(int64(value), 10)
	case int64:
		return "i64:" + strconv.FormatInt(value, 10)
	case float64:
		return canonDouble(value)
	case primitive.Decimal128:
		return "dec:" + decimalText(t, value)
	case primitive.DateTime:
		return "date:" + strconv.FormatInt(int64(value), 10)
	case primitive.Binary:
		return fmt.Sprintf("bin:%02x:%x", value.Subtype, value.Data)
	case primitive.ObjectID:
		return "oid:" + value.Hex()
	case primitive.Timestamp:
		return fmt.Sprintf("ts:%d:%d", value.T, value.I)
	case primitive.Regex:
		return fmt.Sprintf("re:%q:%q", value.Pattern, value.Options)
	case primitive.JavaScript:
		return "js:" + strconv.Quote(string(value))
	case primitive.CodeWithScope:
		return fmt.Sprintf("jsscope:%q:%s", string(value.Code), canonGo(t, value.Scope))
	case primitive.Symbol:
		return "sym:" + strconv.Quote(string(value))
	case primitive.Undefined:
		return "undefined"
	case primitive.MinKey:
		return "minkey"
	case primitive.MaxKey:
		return "maxkey"
	case primitive.DBPointer:
		return fmt.Sprintf("dbptr:%q:%s", value.DB, value.Pointer.Hex())
	}
	t.Fatalf("the library decoded a %T, which this replay does not know how to render", decoded)
	return ""
}

// readDocument decodes bytes the way the driver reads a wire payload: it
// checks the bytes and the depth first, then decodes into an ordered document.
func readDocument(payload []byte) (bson.D, error) {
	if _, err := bson.Validate(payload, bson.Limits{Depth: bsonCorpusDepth}); err != nil {
		return nil, fmt.Errorf("validating: %w", err)
	}
	var document bson.D
	if err := codec.Unmarshal(payload, &document); err != nil {
		return nil, err
	}
	return document, nil
}

func mustHex(t *testing.T, text string) []byte {
	t.Helper()
	payload, err := hex.DecodeString(text)
	if err != nil {
		t.Fatalf("hex %q: %v", text, err)
	}
	return payload
}

func replayEncode(t *testing.T, vector bsonVector) {
	typed := parseTyped(t, vector.Document)
	written, err := codec.Marshal(toBSON(t, typed))
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if got := hex.EncodeToString(written); got != vector.Hex {
		t.Fatalf("encoded bytes\n got %s\nwant %s", got, vector.Hex)
	}
	back, err := readDocument(mustHex(t, vector.Hex))
	if err != nil {
		t.Fatalf("reading the bytes back: %v", err)
	}
	if got, want := canonGo(t, back), canonTyped(t, typed); got != want {
		t.Fatalf("read back\n got %s\nwant %s", got, want)
	}
}

func replayDecode(t *testing.T, vector bsonVector) {
	payload := mustHex(t, vector.Hex)
	got, err := readDocument(payload)
	if err != nil {
		t.Fatalf("decoding: %v", err)
	}
	// An ordered document keeps a repeated key, so the full document is the
	// expectation; last_wins_document is the form for a mapping.
	if want := canonTyped(t, parseTyped(t, vector.Document)); canonGo(t, got) != want {
		t.Fatalf("decoded\n got %s\nwant %s", canonGo(t, got), want)
	}
	if len(vector.LastWinsDocument) > 0 {
		var mapped map[string]any
		if err := codec.Unmarshal(payload, &mapped); err != nil {
			t.Fatalf("decoding into a map: %v", err)
		}
		want := parseTyped(t, vector.LastWinsDocument).(map[string]any)["$doc"].([]any)
		if len(mapped) != len(want) {
			t.Fatalf("a map holds %d keys, last_wins_document has %d", len(mapped), len(want))
		}
		for _, pair := range want {
			entry := pair.([]any)
			if canonGo(t, mapped[entry[0].(string)]) != canonTyped(t, entry[1]) {
				t.Fatalf("a map's last-wins value of %q is %s, want %s", entry[0], canonGo(t, mapped[entry[0].(string)]), canonTyped(t, entry[1]))
			}
		}
	}
	if vector.ReencodesIdentically {
		again, err := codec.Marshal(got)
		if err != nil {
			t.Fatalf("re-encoding: %v", err)
		}
		if !bytes.Equal(again, payload) {
			t.Fatalf("re-encoded\n got %x\nwant %x", again, payload)
		}
	}
}

// knownDifferences are the reject vectors hoarddb-bson does not honour, by vector name
// and the reason. A listed vector must still disagree: when the library starts
// to agree its replay fails, so the table cannot go stale. No vector is
// edited or skipped; the corpus is the spec and the library is what differs.
var knownDifferences = map[string]string{
	"reject/string-invalid-utf8":        "the library decodes a string holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/string-overlong-utf8":       "the library decodes a string holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/string-truncated-utf8":      "the library decodes a string holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/string-lone-surrogate":      "the library decodes a string holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/string-above-max-codepoint": "the library decodes a string holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/key-invalid-utf8":           "the library decodes a key holding invalid UTF-8 without error and Validate does not check UTF-8",
	"reject/javascript-invalid-utf8":    "the library decodes JavaScript code holding invalid UTF-8 without error and Validate does not check UTF-8",
}

func replayReject(t *testing.T, vector bsonVector) {
	if vector.Refusal != "ProtocolError" {
		t.Fatalf("the corpus names refusal %q; this replay maps ProtocolError", vector.Refusal)
	}
	document, err := readDocument(mustHex(t, vector.Hex))
	reason, listed := knownDifferences[vector.Name]
	switch {
	case listed && err == nil:
		t.Logf("known difference: %s", reason)
	case listed:
		t.Fatalf("%s is listed as a known difference (%s) but is now refused (%v); delete the entry", vector.Name, reason, err)
	case err == nil:
		t.Fatalf("the bytes were accepted as %s", canonGo(t, document))
	}
}

func replayEncodeRefuse(t *testing.T, vector bsonVector) {
	typed := parseTyped(t, vector.Document)
	written, err := codec.Marshal(toBSON(t, typed))
	if err == nil {
		t.Fatalf("encoder wrote %x for a document it must refuse", written)
	}
	if errors.Unwrap(err) == nil {
		t.Fatalf("the refusal %q wraps no cause", err)
	}
}

func replayDatetimeFloor(t *testing.T, vector bsonVector) {
	instant, err := time.Parse(time.RFC3339Nano, vector.Instant)
	if err != nil {
		t.Fatalf("instant %q: %v", vector.Instant, err)
	}
	written, err := codec.Marshal(bson.D{{Key: "t", Value: instant}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if got := hex.EncodeToString(written); got != vector.Hex {
		t.Fatalf("encoded bytes\n got %s\nwant %s", got, vector.Hex)
	}
	back, err := readDocument(written)
	if err != nil {
		t.Fatalf("reading back: %v", err)
	}
	if got, want := canonGo(t, back), fmt.Sprintf(`{"t":date:%s}`, vector.ExpectedMillis); got != want {
		t.Fatalf("read back\n got %s\nwant %s", got, want)
	}
}

// largeSizeVector says whether a size vector is one of the 30 MiB and over
// documents that largeTestsEnv gates.
func largeSizeVector(vector bsonVector) bool {
	return vector.DocumentBytes > 28<<20
}

func replaySize(t *testing.T, vector bsonVector) {
	if largeSizeVector(vector) && os.Getenv(largeTestsEnv) == "" {
		t.Skipf("a %d byte document needs a few hundred MiB; set %s=1 to run it", vector.DocumentBytes, largeTestsEnv)
	}
	if len(vector.Recipe.Fill) != 1 {
		t.Fatalf("recipe fill %q is not one byte", vector.Recipe.Fill)
	}
	fill := strings.Repeat(vector.Recipe.Fill, vector.Recipe.FillBytes)
	written, err := codec.Marshal(bson.D{{Key: vector.Recipe.Key, Value: fill}})
	if err != nil {
		t.Fatalf("encoding: %v", err)
	}
	if len(written) != vector.DocumentBytes {
		t.Fatalf("document is %d bytes, want %d", len(written), vector.DocumentBytes)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256(written)); got != vector.SHA256 {
		t.Fatalf("sha256 %s, want %s", got, vector.SHA256)
	}
	if vector.Name == "size/document-24-mib" {
		back, err := readDocument(written)
		if err != nil {
			t.Fatalf("reading the 24 MiB document back: %v", err)
		}
		if len(back) != 1 || back[0].Key != vector.Recipe.Key || back[0].Value != fill {
			t.Fatalf("the 24 MiB document did not read back to its recipe")
		}
	}
}

// TestBSONCorpusReplay replays every vector of clients/testdata/bson-vectors/v1.json
// through the driver's codec, which is hoarddb-bson. The corpus is the server's
// own and the library is shared, so this is a drift guard: a library change
// that moves a byte fails here before it reaches a wire frame.
func TestBSONCorpusReplay(t *testing.T) {
	vectors := loadBSONCorpus(t, bsonCorpusPath)
	replays := map[string]func(*testing.T, bsonVector){
		"encode":         replayEncode,
		"decode":         replayDecode,
		"reject":         replayReject,
		"encode_refuse":  replayEncodeRefuse,
		"datetime_floor": replayDatetimeFloor,
		"size":           replaySize,
	}
	counts := map[string]int{}
	for _, vector := range vectors {
		replay, ok := replays[vector.Kind]
		if !ok {
			t.Errorf("vector %s has kind %q, which this replay does not know", vector.Name, vector.Kind)
			continue
		}
		counts[vector.Kind]++
		t.Run(vector.Name, func(t *testing.T) { replay(t, vector) })
	}
	want := map[string]int{"encode": 112, "decode": 13, "reject": 53, "encode_refuse": 1, "datetime_floor": 11, "size": 6}
	for kind, count := range want {
		if counts[kind] != count {
			t.Errorf("the corpus holds %d %s vectors, this replay was written for %d; read the new vectors and update the count", counts[kind], kind, count)
		}
	}
}
