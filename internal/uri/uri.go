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

// Package uri parses hoardDB connection strings:
//
//	hoarddb://[user[:password]@]host[:port][,host[:port]...][/database[/bucket]][?params]
//
// The grammar is handbook/trust.md section 6, checked against
// clients/testdata/uri-vectors/v1.json. A URI never reaches the wire: it is
// parsed entirely on the client, so the parser is strict. A parameter it does
// not know is an error naming the ones it does, never ignored, because a
// silently ignored "?w=majority" is a developer believing in a setting that
// does not exist.
package uri

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

// DefaultDialTimeoutMs is the dial, handshake and AUTH deadline per host.
const DefaultDialTimeoutMs = 10000

// Host is one entry of the host list.
type Host struct {
	Name string
	Port int
}

// Address is "host:port", with an IPv6 name bracketed.
func (host Host) Address() string {
	return net.JoinHostPort(host.Name, strconv.Itoa(host.Port))
}

// URI is a parsed connection string with every default filled in.
type URI struct {
	Hosts []Host

	Username    string
	HasUsername bool
	Password    string
	HasPassword bool

	// Database and Bucket are empty when the path names none.
	Database string
	Bucket   string

	// Fingerprints are canonical (see trust.ParseFingerprint). There are none,
	// one for every host, or one per host in written order.
	Fingerprints  []string
	AuthToken     string
	ClientName    string
	DialTimeoutMs int
}

// Error is a connection string the parser refused. Its message says what is
// wrong and what to write instead.
type Error struct {
	Message string
}

func (err *Error) Error() string { return err.Message }

func refuse(format string, args ...any) error {
	return &Error{Message: fmt.Sprintf(format, args...)}
}

// unencodedCredentialAdvice is the refusal for a user name or password that
// holds an '@', '/' or '?' written without percent-encoding.
const unencodedCredentialAdvice = "the user name or password contains an unencoded '@', '/' or '?'; " +
	"percent-encode '@' as %40, '/' as %2F and '?' as %3F (and ':' as %3A) in the user name and password"

// acceptedParameters is quoted in the error for an unknown parameter.
const acceptedParameters = "fingerprint, authToken, clientName, dialTimeoutMs"

// refusedParameters maps a lower-cased name a developer is likely to try, for
// something hoardDB does not have, to the sentence that explains why.
var refusedParameters = map[string]string{
	"tls":              "TLS 1.3 is always on and cannot be switched off",
	"ssl":              "TLS 1.3 is always on and cannot be switched off",
	"insecure":         "trust bypass is never read from a URI, because a URI is copied between environments; use the --insecure flag of the command line tool, or hoarddb.WithInsecureSkipVerify in this driver",
	"sslverify":        "trust is decided by a fingerprint, not by a certificate authority; use fingerprint=SHA256:<64 hex characters>",
	"srv":              "DNS SRV lookups are not supported; list the hosts instead",
	"replicaset":       "hoardDB has no replica sets; list the nodes as host1,host2,host3 and the first reachable one is used",
	"readpreference":   "hoardDB has no read preferences; a read is served by the node that answers",
	"w":                "write concern is not part of the URI; set it per client or per call in the driver's options",
	"journal":          "hoardDB has no journal setting; write concern is set in the driver's options",
	"authsource":       "hoardDB has one user store; there is no authentication source to choose",
	"authmechanism":    "hoardDB has one authentication mechanism, a password or a token (authToken=...)",
	"retrywrites":      "the driver never retries a write on its own, because a retried push or increment applies twice",
	"directconnection": "there is no topology discovery to switch off; the host list is tried in order",
}

// Parse parses text. It never contacts the network.
func Parse(text string) (*URI, error) {
	if strings.Contains(text, "#") {
		return nil, refuse("fragments (a '#' and what follows it) are not part of a hoardDB connection string; remove it " +
			"(a literal '#' inside a name must be written %%23)")
	}
	scheme, rest, found := strings.Cut(text, "://")
	if !found {
		return nil, refuse("the text is not a connection string; it must start with hoarddb://, for example hoarddb://localhost:7433/app")
	}
	if err := checkScheme(scheme); err != nil {
		return nil, err
	}
	// An '@' that is not the one ending the userinfo means the user name or
	// password holds an unencoded '@', '/' or '?'. The authority ends at the
	// first '/' or '?', so left alone the text after that character would parse
	// as a host or a path, and the credentials would be sent to the wrong host.
	if beforeQuery, _, _ := strings.Cut(rest, "?"); strings.Count(beforeQuery, "@") > 1 || leftoverAt(rest) {
		return nil, refuse("%s", unencodedCredentialAdvice)
	}
	rest, query, _ := strings.Cut(rest, "?")
	authority, path, _ := strings.Cut(rest, "/")
	parsed := &URI{DialTimeoutMs: DefaultDialTimeoutMs}
	if err := parsed.parseAuthority(authority); err != nil {
		return nil, err
	}
	if err := parsed.parsePath(path); err != nil {
		return nil, err
	}
	if err := parsed.parseQuery(query); err != nil {
		return nil, err
	}
	if count := len(parsed.Fingerprints); count > 1 && count != len(parsed.Hosts) {
		return nil, refuse("%d fingerprints were given for %d hosts; give one fingerprint for all hosts, or exactly one per host in the order the hosts are written",
			count, len(parsed.Hosts))
	}
	return parsed, nil
}

// leftoverAt reports an '@' after the first '/' or '?' when none came before
// it: the userinfo was cut in half by an unencoded '/' or '?'.
func leftoverAt(rest string) bool {
	end := strings.IndexAny(rest, "/?")
	return end >= 0 && !strings.Contains(rest[:end], "@") && strings.Contains(rest[end:], "@")
}

func checkScheme(scheme string) error {
	switch lower := strings.ToLower(scheme); lower {
	case "hoarddb":
		return nil
	case "hoarddb+srv":
		return refuse("DNS SRV lookups are not supported; list the hosts instead: hoarddb://host1:7433,host2:7433/")
	case "hoarddb+tls":
		return refuse("the scheme hoarddb+tls is not a hoardDB scheme: TLS 1.3 is always on, so there is no separate TLS scheme; use hoarddb://")
	default:
		return refuse("the scheme is not a hoardDB scheme; use hoarddb://")
	}
}

func (parsed *URI) parseAuthority(authority string) error {
	hostList := authority
	if at := strings.LastIndex(authority, "@"); at >= 0 {
		userinfo := authority[:at]
		hostList = authority[at+1:]
		if strings.Contains(userinfo, "@") {
			return refuse("%s", unencodedCredentialAdvice)
		}
		rawName, rawPassword, hasPassword := strings.Cut(userinfo, ":")
		name, err := unescape("user name", rawName)
		if err != nil {
			return err
		}
		if name == "" {
			return refuse("the user name before '@' is empty; write hoarddb://user@host or leave the '@' out")
		}
		parsed.Username, parsed.HasUsername = name, true
		if hasPassword {
			password, err := unescape("password", rawPassword)
			if err != nil {
				return err
			}
			parsed.Password, parsed.HasPassword = password, true
		}
	}
	if hostList == "" {
		return refuse("no host given; write hoarddb://host or hoarddb://host:7433")
	}
	for _, entry := range strings.Split(hostList, ",") {
		host, err := parseHost(entry)
		if err != nil {
			return err
		}
		parsed.Hosts = append(parsed.Hosts, host)
	}
	return nil
}

func parseHost(entry string) (Host, error) {
	if entry == "" {
		return Host{}, refuse("the host list has an empty host (a stray comma?); write hoarddb://host1:7433,host2:7433")
	}
	name, port := entry, ""
	if strings.HasPrefix(entry, "[") {
		closing := strings.Index(entry, "]")
		if closing < 0 {
			return Host{}, refuse("the host list has an opening '[' with no closing ']'; an IPv6 literal is written [::1]:7433")
		}
		name = entry[1:closing]
		remainder := entry[closing+1:]
		switch {
		case remainder == "":
		case strings.HasPrefix(remainder, ":"):
			port = remainder[1:]
		default:
			return Host{}, refuse("a host has text after the closing ']' that is not a :port; write [::1]:7433")
		}
		if net.ParseIP(name) == nil {
			return Host{}, refuse("a bracketed host is not an IPv6 address; brackets are only for IPv6 literals such as [::1]")
		}
	} else {
		switch colons := strings.Count(entry, ":"); {
		case colons > 1:
			return Host{}, refuse("a host has more than one ':' and looks like an IPv6 literal; an IPv6 literal must be bracketed with a port after the bracket, like [::1]:7433")
		case colons == 1:
			name, port, _ = strings.Cut(entry, ":")
		}
	}
	if name == "" {
		return Host{}, refuse("a host entry has no host name before the port; write host:7433")
	}
	if strings.ContainsAny(name, " \t/\\@[]") {
		return Host{}, refuse("a host name contains a character that is not allowed in a host name (a space, '/', '\\', '@', '[' or ']')")
	}
	if port == "" && strings.HasSuffix(entry, ":") {
		return Host{}, refuse("a host has a ':' with no port after it; write host:7433 or just host")
	}
	if port == "" {
		return Host{Name: name, Port: hoarddbwire.DefaultPort}, nil
	}
	number, err := strconv.Atoi(port)
	if err != nil {
		return Host{}, refuse("the port in the connection string is not a number; ports are 1-65535 and the default is %d", hoarddbwire.DefaultPort)
	}
	if number < 1 || number > 65535 {
		return Host{}, refuse("the port %d is out of range; ports are 1-65535 and the default is %d", number, hoarddbwire.DefaultPort)
	}
	return Host{Name: name, Port: number}, nil
}

func (parsed *URI) parsePath(path string) error {
	if path == "" {
		return nil
	}
	segments := strings.Split(path, "/")
	if len(segments) > 2 {
		return refuse("the path has more than a database and a bucket; write /database or /database/bucket " +
			"(a '/' inside a name must be written %%2F)")
	}
	database, err := unescape("database name", segments[0])
	if err != nil {
		return err
	}
	if database == "" {
		return refuse("the path has an empty database name; write /database or /database/bucket")
	}
	parsed.Database = database
	if len(segments) == 2 {
		bucket, err := unescape("bucket name", segments[1])
		if err != nil {
			return err
		}
		if bucket == "" {
			return refuse("the path has an empty bucket name; write /database/bucket or drop the trailing '/'")
		}
		parsed.Bucket = bucket
	}
	return nil
}

func (parsed *URI) parseQuery(query string) error {
	if query == "" {
		return nil
	}
	seen := map[string]bool{}
	for _, pair := range strings.Split(query, "&") {
		rawName, rawValue, hasValue := strings.Cut(pair, "=")
		name, err := unescape("parameter name", rawName)
		if err != nil {
			return err
		}
		key := strings.ToLower(name)
		shown := parameterForMessage(key)
		if !hasValue || rawValue == "" {
			if shown == "" {
				return refuse("a parameter has no value; write name=<value>, and the accepted parameters are %s", acceptedParameters)
			}
			return refuse("parameter %q has no value; write %s=<value>", shown, shown)
		}
		valueOf := "parameter value"
		if shown != "" {
			valueOf = "parameter " + shown
		}
		value, err := unescape(valueOf, rawValue)
		if err != nil {
			return err
		}
		if why, refused := refusedParameters[key]; refused {
			return refuse("parameter %q is refused: %s", shown, why)
		}
		switch key {
		case "fingerprint":
			canonical, err := trust.ParseFingerprint(value)
			if err != nil {
				return refuse("parameter fingerprint: %v", err)
			}
			parsed.Fingerprints = append(parsed.Fingerprints, canonical)
			continue
		case "authtoken", "clientname", "dialtimeoutms":
			if seen[key] {
				return refuse("parameter %q is given twice; only fingerprint may repeat", shown)
			}
			seen[key] = true
		default:
			return refuse("an unknown parameter was given; the accepted parameters are %s", acceptedParameters)
		}
		switch key {
		case "authtoken":
			parsed.AuthToken = value
		case "clientname":
			parsed.ClientName = value
		case "dialtimeoutms":
			milliseconds, err := strconv.Atoi(value)
			if err != nil || milliseconds < 1 {
				return refuse("the dialTimeoutMs value must be a whole number of milliseconds greater than zero, for example dialTimeoutMs=3000")
			}
			parsed.DialTimeoutMs = milliseconds
		}
	}
	return nil
}

// parameterForMessage is the spelling an error may use for a parameter name:
// the driver's own spelling for a name in the accepted or refused sets, and ""
// for anything else, which is the user's text and is never repeated.
func parameterForMessage(key string) string {
	switch key {
	case "fingerprint":
		return "fingerprint"
	case "authtoken":
		return "authToken"
	case "clientname":
		return "clientName"
	case "dialtimeoutms":
		return "dialTimeoutMs"
	}
	if _, refused := refusedParameters[key]; refused {
		return key
	}
	return ""
}

// PinFor is the fingerprint that applies to the host at index, or "" when the
// URI pins nothing. One fingerprint covers every host.
func (parsed *URI) PinFor(index int) string {
	switch len(parsed.Fingerprints) {
	case 0:
		return ""
	case 1:
		return parsed.Fingerprints[0]
	default:
		return parsed.Fingerprints[index]
	}
}

// String is Redacted, so a URI printed with %s or %v never shows the password.
func (parsed URI) String() string { return parsed.Redacted() }

// GoString is Redacted, so %#v does not print the fields.
func (parsed URI) GoString() string { return parsed.Redacted() }

// Format is Redacted under every verb: %d, %x or %+v would otherwise print
// the Password and AuthToken fields. A short write to the fmt.State has no one
// to report to, so its error is not checked.
func (parsed URI) Format(state fmt.State, _ rune) { _, _ = fmt.Fprint(state, parsed.Redacted()) }

// Redacted renders the URI for a log line or an error message with the
// password and token replaced, so a connection string is safe to print.
func (parsed *URI) Redacted() string {
	var out strings.Builder
	out.WriteString("hoarddb://")
	if parsed.HasUsername {
		out.WriteString(url.PathEscape(parsed.Username))
		if parsed.HasPassword {
			out.WriteString(":***")
		}
		out.WriteString("@")
	}
	for index, host := range parsed.Hosts {
		if index > 0 {
			out.WriteString(",")
		}
		out.WriteString(host.Address())
	}
	if parsed.Database != "" {
		out.WriteString("/" + url.PathEscape(parsed.Database))
		if parsed.Bucket != "" {
			out.WriteString("/" + url.PathEscape(parsed.Bucket))
		}
	}
	return out.String()
}

// unescape percent-decodes one component. A '+' is a plus: tokens are base64
// and this is not a form encoding.
func unescape(what, text string) (string, error) {
	decoded, err := url.PathUnescape(text)
	if err != nil {
		return "", refuse("the %s contains an invalid percent-escape; write a literal '%%' as %%25", what)
	}
	return decoded, nil
}
