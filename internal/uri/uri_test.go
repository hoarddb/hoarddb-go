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

package uri

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const vectorsRelPath = "testdata/uri-vectors/v1.json"

// findVectorsFile walks up from dir looking for the vector file, so the same
// test passes in the hoardDB repository, where testdata/ is above clients/go,
// and in the seeded driver repository, where testdata/ sits at the module root.
func findVectorsFile(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("cannot read the working directory: %v", err)
	}
	for {
		candidate := filepath.Join(dir, filepath.FromSlash(vectorsRelPath))
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("%s is not in any parent of the test's directory", vectorsRelPath)
			return ""
		}
		dir = parent
	}
}

type vectorFile struct {
	Defaults struct {
		Port          int `json:"port"`
		DialTimeoutMs int `json:"dialTimeoutMs"`
	} `json:"defaults"`
	Cases   []validCase   `json:"cases"`
	Invalid []invalidCase `json:"invalid"`
}

type validCase struct {
	Name   string `json:"name"`
	URI    string `json:"uri"`
	Expect struct {
		Hosts []struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		} `json:"hosts"`
		Username *string `json:"username"`
		Password *string `json:"password"`
		Database *string `json:"database"`
		Bucket   *string `json:"bucket"`
		Params   struct {
			Fingerprints  []string `json:"fingerprints"`
			AuthToken     *string  `json:"authToken"`
			ClientName    *string  `json:"clientName"`
			DialTimeoutMs int      `json:"dialTimeoutMs"`
		} `json:"params"`
		PinForHost map[string]string `json:"pin_for_host"`
	} `json:"expect"`
}

type invalidCase struct {
	Name          string   `json:"name"`
	URI           string   `json:"uri"`
	ErrorContains string   `json:"error_contains"`
	ErrorExcludes []string `json:"error_excludes"`
}

// errorChainText joins the message of err and of every error it wraps, so an
// exclusion holds for the whole chain and not only the outermost message.
func errorChainText(err error) string {
	var texts []string
	for ; err != nil; err = errors.Unwrap(err) {
		texts = append(texts, err.Error())
	}
	return strings.Join(texts, "\n")
}

func loadVectors(t *testing.T) vectorFile {
	t.Helper()
	raw, err := os.ReadFile(findVectorsFile(t))
	if err != nil {
		t.Fatalf("the frozen URI vectors are the contract and must be present: %v", err)
	}
	var vectors vectorFile
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func orEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func TestURIVectorsValid(t *testing.T) {
	vectors := loadVectors(t)
	if len(vectors.Cases) < 19 {
		t.Fatalf("only %d valid vectors loaded; the file carries 19", len(vectors.Cases))
	}
	for _, vector := range vectors.Cases {
		t.Run(vector.Name, func(t *testing.T) {
			got, err := Parse(vector.URI)
			if err != nil {
				t.Fatalf("Parse(%q): %v", vector.URI, err)
			}
			expect := vector.Expect
			if len(got.Hosts) != len(expect.Hosts) {
				t.Fatalf("hosts = %+v, want %+v", got.Hosts, expect.Hosts)
			}
			for index, host := range expect.Hosts {
				if got.Hosts[index].Name != host.Host || got.Hosts[index].Port != host.Port {
					t.Errorf("host %d = %+v, want %s:%d", index, got.Hosts[index], host.Host, host.Port)
				}
			}
			// Absent and empty are distinguished by the Has flags, not by "".
			if got.HasUsername != (expect.Username != nil) || got.Username != orEmpty(expect.Username) {
				t.Errorf("username = %q (has %v), want %v", got.Username, got.HasUsername, expect.Username)
			}
			if got.HasPassword != (expect.Password != nil) || got.Password != orEmpty(expect.Password) {
				t.Errorf("password differs from the vector")
			}
			if got.Database != orEmpty(expect.Database) || got.Bucket != orEmpty(expect.Bucket) {
				t.Errorf("database/bucket = %q/%q, want %q/%q", got.Database, got.Bucket, orEmpty(expect.Database), orEmpty(expect.Bucket))
			}
			wantFingerprints := expect.Params.Fingerprints
			if len(wantFingerprints) == 0 {
				wantFingerprints = nil
			}
			if !reflect.DeepEqual(got.Fingerprints, wantFingerprints) {
				t.Errorf("fingerprints = %v, want %v", got.Fingerprints, wantFingerprints)
			}
			if got.AuthToken != orEmpty(expect.Params.AuthToken) || got.ClientName != orEmpty(expect.Params.ClientName) {
				t.Errorf("authToken/clientName = %q/%q", got.AuthToken, got.ClientName)
			}
			if got.DialTimeoutMs != expect.Params.DialTimeoutMs {
				t.Errorf("dialTimeoutMs = %d, want %d", got.DialTimeoutMs, expect.Params.DialTimeoutMs)
			}
			for hostName, pin := range expect.PinForHost {
				for index, host := range got.Hosts {
					if host.Name == hostName && got.PinFor(index) != pin {
						t.Errorf("pin for %s = %q, want %q", hostName, got.PinFor(index), pin)
					}
				}
			}
		})
	}
}

func TestURIVectorsInvalid(t *testing.T) {
	vectors := loadVectors(t)
	if len(vectors.Invalid) < 29 {
		t.Fatalf("only %d invalid vectors loaded; the file carries 29", len(vectors.Invalid))
	}
	for _, vector := range vectors.Invalid {
		t.Run(vector.Name, func(t *testing.T) {
			parsed, err := Parse(vector.URI)
			if err == nil {
				t.Fatalf("Parse(%q) = %+v, want an error", vector.URI, parsed)
			}
			var parseErr *Error
			if !errors.As(err, &parseErr) {
				t.Fatalf("error %T is not *Error", err)
			}
			if !strings.Contains(err.Error(), vector.ErrorContains) {
				t.Fatalf("error %q does not contain %q", err.Error(), vector.ErrorContains)
			}
			chain := errorChainText(err)
			for _, excluded := range vector.ErrorExcludes {
				if strings.Contains(chain, excluded) {
					t.Fatalf("error chain %q repeats %q from the refused string", chain, excluded)
				}
			}
		})
	}
}

// The next tests are asserted independently of the vector file, so deleting a
// vector cannot delete the assertion (handbook/trust.md section 6).

func TestURIDefaultPortIs7433(t *testing.T) {
	parsed, err := Parse("hoarddb://example.com")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Hosts[0].Port != 7433 {
		t.Fatalf("default port = %d, want 7433 (4433 is the internode port)", parsed.Hosts[0].Port)
	}
	if parsed.DialTimeoutMs != 10000 {
		t.Fatalf("default dial timeout = %d, want 10000", parsed.DialTimeoutMs)
	}
	if got := parsed.Hosts[0].Address(); got != "example.com:7433" {
		t.Fatalf("Address = %q", got)
	}
}

func TestURIAddressBracketsIPv6(t *testing.T) {
	parsed, err := Parse("hoarddb://[2001:db8::1]:7500")
	if err != nil {
		t.Fatal(err)
	}
	if got := parsed.Hosts[0].Address(); got != "[2001:db8::1]:7500" {
		t.Fatalf("Address = %q", got)
	}
}

func TestURIFingerprintCanonicalised(t *testing.T) {
	parsed, err := Parse("hoarddb://h/?fingerprint=sha256:ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789")
	if err != nil {
		t.Fatal(err)
	}
	want := "SHA256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
	if len(parsed.Fingerprints) != 1 || parsed.Fingerprints[0] != want {
		t.Fatalf("fingerprints = %v, want [%s]", parsed.Fingerprints, want)
	}
}

func TestURIFingerprintCountMismatchNamesBothCounts(t *testing.T) {
	pin := "fingerprint=SHA256:" + strings.Repeat("ab", 32)
	_, err := Parse("hoarddb://a,b,c/app?" + pin + "&" + pin)
	if err == nil || !strings.Contains(err.Error(), "2 fingerprints") || !strings.Contains(err.Error(), "3 hosts") {
		t.Fatalf("err = %v, want one naming 2 fingerprints and 3 hosts", err)
	}
	// Exactly one applies to every host; exactly n is positional.
	for _, ok := range []string{"hoarddb://a,b,c/app?" + pin, "hoarddb://a,b/app?" + pin + "&" + pin} {
		if _, err := Parse(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
}

func TestURIUnknownParameterRefusedNamingTheAcceptedSet(t *testing.T) {
	_, err := Parse("hoarddb://h/app?madeUpOption=1")
	if err == nil {
		t.Fatal("an unknown parameter must be an error, not ignored")
	}
	for _, accepted := range []string{"fingerprint", "authToken", "clientName", "dialTimeoutMs"} {
		if !strings.Contains(err.Error(), accepted) {
			t.Errorf("error %q does not name the accepted parameter %q", err, accepted)
		}
	}
}

func TestURIRefusedParametersEachExplainThemselves(t *testing.T) {
	for _, name := range []string{"tls", "ssl", "insecure", "sslVerify", "srv", "replicaSet", "readPreference",
		"w", "journal", "authSource", "authMechanism", "retryWrites", "directConnection", "INSECURE"} {
		t.Run(name, func(t *testing.T) {
			_, err := Parse("hoarddb://h/app?" + name + "=1")
			if err == nil || !strings.Contains(err.Error(), "refused") || strings.Contains(err.Error(), "unknown parameter") {
				t.Fatalf("err = %v; a known-wrong parameter is refused by name with a reason, not called unknown", err)
			}
		})
	}
}

func TestURISchemeSrvRefusedByName(t *testing.T) {
	_, err := Parse("hoarddb+srv://cluster.example.com/app")
	if err == nil || !strings.Contains(err.Error(), "SRV") || strings.Contains(strings.ToLower(err.Error()), "unknown scheme") {
		t.Fatalf("err = %v, want a message about DNS SRV lookups", err)
	}
}

func TestURISchemeIsCaseInsensitiveButNothingElseIsAccepted(t *testing.T) {
	if _, err := Parse("HOARDDB://h"); err != nil {
		t.Fatal(err)
	}
	for _, text := range []string{"", "h", "localhost:7433", "http://h", "hoarddb:/h", "//h"} {
		if _, err := Parse(text); err == nil {
			t.Errorf("Parse(%q) succeeded", text)
		}
	}
}

func TestURIPasswordRedacted(t *testing.T) {
	parsed, err := Parse("hoarddb://root:p%40ssw0rd-secret@h1:7433,h2/app/Users?authToken=tok-secret&clientName=etl")
	if err != nil {
		t.Fatal(err)
	}
	redacted := parsed.Redacted()
	if strings.Contains(redacted, "secret") || strings.Contains(redacted, "ssw0rd") {
		t.Fatalf("Redacted leaked a secret: %s", redacted)
	}
	if want := "hoarddb://root:***@h1:7433,h2:7433/app/Users"; redacted != want {
		t.Fatalf("Redacted = %q, want %q", redacted, want)
	}
}

func TestURIErrorsNeverEchoThePassword(t *testing.T) {
	for _, text := range []string{
		"hoarddb://root:topsecret@ss@host/app",
		"hoarddb://root:topsecret@host:99999/app",
		"hoarddb://root:topsecret@host/a/b/c",
		"hoarddb://root:topsecret@host/app?bogus=1",
		"root:topsecret@host/app",
		"hoarddb://root:top%zzsecret@host/app",
	} {
		_, err := Parse(text)
		if err == nil {
			t.Errorf("Parse(%q) succeeded", text)
			continue
		}
		if strings.Contains(err.Error(), "topsecret") {
			t.Errorf("the error for %q echoed the password: %v", text, err)
		}
	}
}

func TestURIDetails(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		check   func(*URI) bool
		wantErr string
	}{
		{name: "empty password is present and empty", input: "hoarddb://root:@h", check: func(u *URI) bool { return u.HasPassword && u.Password == "" }},
		{name: "plus in a token stays a plus", input: "hoarddb://h?authToken=a+b/c=", check: func(u *URI) bool { return u.AuthToken == "a+b/c=" }},
		{name: "query without path", input: "hoarddb://h:1?clientName=x", check: func(u *URI) bool { return u.ClientName == "x" && u.Database == "" }},
		{name: "empty user name", input: "hoarddb://:pw@h", wantErr: "user name"},
		{name: "empty database", input: "hoarddb://h//bucket", wantErr: "database"},
		{name: "trailing slash after database", input: "hoarddb://h/app/", wantErr: "bucket"},
		{name: "port zero", input: "hoarddb://h:0", wantErr: "out of range"},
		{name: "colon without port", input: "hoarddb://h:", wantErr: "port"},
		{name: "unclosed bracket", input: "hoarddb://[::1:7433", wantErr: "']'"},
		{name: "bracketed non-IPv6", input: "hoarddb://[example.com]:1", wantErr: "IPv6"},
		{name: "parameter with no value", input: "hoarddb://h?clientName", wantErr: "no value"},
		{name: "empty value", input: "hoarddb://h?clientName=", wantErr: "no value"},
		{name: "repeated clientName", input: "hoarddb://h?clientName=a&clientName=b", wantErr: "twice"},
		{name: "dial timeout zero", input: "hoarddb://h?dialTimeoutMs=0", wantErr: "dialTimeoutMs"},
		{name: "bad percent escape", input: "hoarddb://h/%zz", wantErr: "percent"},
		{name: "path with three segments", input: "hoarddb://h/a/b/c", wantErr: "path"},
		{name: "percent-encoded slash survives as a name", input: "hoarddb://h/a%2Fb", check: func(u *URI) bool { return u.Database == "a/b" && u.Bucket == "" }},
		{name: "percent-encoded hash in a name", input: "hoarddb://h/a%23b", check: func(u *URI) bool { return u.Database == "a#b" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := Parse(test.input)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("Parse(%q) error = %v, want one containing %q", test.input, err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !test.check(parsed) {
				t.Fatalf("Parse(%q) = %+v", test.input, parsed)
			}
		})
	}
}

func TestPinForWithNoFingerprints(t *testing.T) {
	parsed, err := Parse("hoarddb://a,b")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.PinFor(0) != "" || parsed.PinFor(1) != "" {
		t.Fatal("no fingerprints means no pins")
	}
}

// leakedFragment reports the first run of three or more characters of secret
// that appears in text, or "".
func leakedFragment(text, secret string) string {
	for start := 0; start+3 <= len(secret); start++ {
		if fragment := secret[start : start+3]; strings.Contains(text, fragment) {
			return fragment
		}
	}
	return ""
}

// A password written without percent-encoding can contain '/', '?' or ':',
// which moves part of it into the host list, the path or the query. Whatever
// the parser refuses, the message names the problem and never repeats the
// connection string.
func TestParseErrorsNeverRepeatTheConnectionString(t *testing.T) {
	passwords := []string{
		"Zq7xK9/Wm3pL",
		"4817/Xv2jQ/Rt5nB",
		"4817/Xv2jQ/Rt5nB/Hy6cD",
		"4817?Xv2jQ=Rt5nB",
		"Zq7xK9:Wm3pL?Hy6cD",
		"Zq7xK9?Wm3pL/Hy6cD",
		"4817?Xv2jQ&Rt5nB=Hy6cD",
		"[Zq7xK9]/Wm3pL",
		"Zq7xK9,Wm3pL/Hy6cD",
		"Zq7xK9%zzWm3pL",
		"Zq7xK9#Wm3pL",
	}
	templates := []string{
		"hoarddb://alice:%s@db.example:7433/app",
		"hoarddb://alice:%s@db.example:notaport/app",
		"hoarddb://alice:%s@db.example:99999/app",
		"hoarddb://alice:%s@db.example/app?bogus=1",
		"hoarddb://alice:%s@db.example/app/b/c",
		"hoarddb://alice:%s@db.example/app?dialTimeoutMs=abc",
		"hoarddb://alice:%s@db.example/app?fingerprint=nothex",
		"hoarddb://alice:%s@db .example/app",
		"hoarddb://alice:%s@[nope]/app",
		"hoarddb://alice:%s@db.example:/app",
		"hoarddb://alice:%s@a:b:c/app",
		"hoarddb+tls://alice:%s@db.example/app",
		"https://alice:%s@db.example/app",
		"alice:%s@db.example/app",
		"%s@db.example",
		"hoarddb://alice:%s@@db.example",
	}
	for _, password := range passwords {
		for _, template := range templates {
			text := fmt.Sprintf(template, password)
			parsed, err := Parse(text)
			if err != nil {
				for _, message := range errorChain(err) {
					if fragment := leakedFragment(message, password); fragment != "" {
						t.Errorf("Parse(%q): the error repeats %q of the password: %s", text, fragment, message)
					}
					if strings.Contains(message, text) {
						t.Errorf("Parse(%q): the error repeats the connection string: %s", text, message)
					}
				}
				continue
			}
			if fragment := leakedFragment(parsed.Redacted(), password); fragment != "" {
				t.Errorf("Parse(%q): Redacted repeats %q of the password: %s", text, fragment, parsed.Redacted())
			}
		}
	}
}

func errorChain(err error) []string {
	var messages []string
	for ; err != nil; err = errors.Unwrap(err) {
		messages = append(messages, err.Error())
	}
	return messages
}

// A token that is not part of the grammar (a parameter name, a parameter value,
// a port, a host) is never repeated in the refusal, with or without a password
// in the string: nothing tells the parser the token was not a secret.
func TestParseErrorsNeverRepeatQueryOrHostText(t *testing.T) {
	const token = "Zq7xK9Wm3pL"
	texts := []string{
		"hoarddb://db.example/app?" + token + "=1",
		"hoarddb://db.example/app?" + token,
		"hoarddb://db.example/app?" + token + "=",
		"hoarddb://db.example/app?dialTimeoutMs=" + token,
		"hoarddb://db.example/app?fingerprint=" + token,
		"hoarddb://db.example/app?fingerprint=SHA256:" + token,
		"hoarddb://db.example/app?fingerprint=SHA256:" + strings.Repeat(token, 6)[:64],
		"hoarddb://db.example/app?authToken=a&authToken=" + token,
		"hoarddb://db.example/app?" + token + "%zz=1",
		"hoarddb://db.example/app?clientName=%zz" + token,
		"hoarddb://db.example:" + token + "/app",
		"hoarddb://db.example:" + token,
		"hoarddb://" + token + " x/app",
		"hoarddb://[" + token + "]/app",
		"hoarddb://[::1]" + token + "/app",
		"hoarddb://[" + token + "/app",
		"hoarddb://" + token + ":1:2/app",
		"hoarddb://:" + token + "/app",
		"hoarddb://db.example/" + token + "/b/c",
		"hoarddb://db.example/%zz" + token,
		token + "://db.example/app",
		token,
	}
	for _, text := range texts {
		_, err := Parse(text)
		if err == nil {
			t.Errorf("Parse(%q) succeeded", text)
			continue
		}
		if fragment := leakedFragment(err.Error(), token); fragment != "" {
			t.Errorf("Parse(%q): the error repeats %q: %s", text, fragment, err)
		}
	}
}

// A name that belongs to the fixed sets is the driver's own text, so the
// refusal may name it.
func TestParseErrorsNameKnownParameters(t *testing.T) {
	for text, want := range map[string]string{
		"hoarddb://h/app?dialTimeoutMs=soon":      "dialTimeoutMs",
		"hoarddb://h/app?authToken":               "authToken",
		"hoarddb://h/app?authToken=a&authtoken=b": "authToken",
		"hoarddb://h/app?replicaSet=rs0":          "replicaset",
		"hoarddb://h/app?nonsense=1":              "fingerprint, authToken, clientName, dialTimeoutMs",
	} {
		_, err := Parse(text)
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%q) error = %v, want one naming %q", text, err, want)
		}
	}
}

func TestParsePortRangeMessageMatchesNode(t *testing.T) {
	_, err := Parse("hoarddb://db.example:99999/app")
	want := "the port 99999 is out of range; ports are 1-65535 and the default is 7433"
	if err == nil || err.Error() != want {
		t.Fatalf("error = %v, want %q", err, want)
	}
}

// A second '@' before the query means the user name or password holds an
// unencoded '@' (possibly with a '/'), and the text after it would otherwise
// parse as a different host that receives the credentials.
func TestParseRefusesMoreThanOneAtSign(t *testing.T) {
	for _, text := range []string{
		"hoarddb://alice:ab@cd/ef@db.example:7433/app",
		"hoarddb://alice:ab@cd@db.example/app",
		"hoarddb://alice:ab@cd/ef@db.example",
		"hoarddb://alice@cd:ab@db.example/app",
	} {
		_, err := Parse(text)
		if err == nil {
			t.Errorf("Parse(%q) succeeded; the credentials would go to the wrong host", text)
			continue
		}
		if !strings.Contains(err.Error(), "%40") || !strings.Contains(err.Error(), "%2F") {
			t.Errorf("Parse(%q) error = %v, want the percent-encoding advice", text, err)
		}
	}
	if _, err := Parse("hoarddb://alice:pw@db.example/app?clientName=a@b"); err != nil {
		t.Errorf("an '@' in the query is allowed: %v", err)
	}
}

func TestURIRedactsUnderEveryFormatVerb(t *testing.T) {
	parsed, err := Parse("hoarddb://alice:Zq7xK9Wm3pL@db.example/app?authToken=Hy6cDRt5nB")
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{parsed, *parsed} {
		for _, verb := range []string{"%s", "%v", "%+v", "%#v", "%q", "%x", "%d"} {
			text := fmt.Sprintf(verb, value)
			if strings.Contains(text, "Zq7xK9Wm3pL") || strings.Contains(text, "Hy6cDRt5nB") {
				t.Errorf("%T formatted with %s repeats a credential: %s", value, verb, text)
			}
		}
	}
}

func TestParseEscapeAndLeftoverRefusals(t *testing.T) {
	for text, want := range map[string]string{
		"hoarddb://h/?Zq7xK9=%zz":    "the parameter value contains an invalid percent-escape; write a literal '%' as %25",
		"hoarddb://h/?authToken=%zz": "the parameter authToken contains an invalid percent-escape; write a literal '%' as %25",
	} {
		if _, err := Parse(text); err == nil || err.Error() != want {
			t.Errorf("Parse(%q) error = %v, want %q", text, err, want)
		}
	}
	// No '@' before the first '/', one after it: the userinfo was cut in half.
	_, err := Parse("hoarddb://alice:123/456@db.example/app")
	if err == nil || !strings.Contains(err.Error(), "%40") || !strings.Contains(err.Error(), "%2F") {
		t.Errorf("error = %v, want the percent-encoding advice", err)
	}
}
