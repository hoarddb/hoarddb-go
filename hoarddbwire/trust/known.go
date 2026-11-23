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

package trust

import (
	"bufio"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// KnownServers is a file of servers a person has already decided to trust.
//
// The format is one server per line: "host:port" and the lowercase hex of the
// server's 32-byte ed25519 public key, separated by a space. The file is
// created with mode 0600 in a directory of mode 0700. Blank lines and lines
// starting with "#" are ignored and preserved. The hoardDB command line tool
// reads and writes the same file, so a server trusted once there is trusted
// here.
type KnownServers struct {
	// OnLockUnsupported, if set, is called once with the file's path the first
	// time the file system turns out not to support advisory locks (or the
	// platform has none: windows, solaris, aix). Writers in separate processes
	// are then not serialised. Writers in this process still are, even across
	// several KnownServers on the same path, by a package-level mutex per path.
	OnLockUnsupported func(path string)

	path            string
	mu              sync.Mutex
	unsupportedOnce sync.Once
}

// pathLocks serialises Add across every KnownServers in this process that
// names the same file. A KnownServers' own mutex covers only that instance, and
// without file locks two instances would each record a first-seen key.
var pathLocks = struct {
	sync.Mutex
	byPath map[string]*sync.Mutex
}{byPath: map[string]*sync.Mutex{}}

// lockPath takes the process-wide mutex for the cleaned absolute form of path
// and returns the function that releases it. One small mutex is kept per
// distinct path for the life of the process.
func lockPath(path string) (unlock func()) {
	key := filepath.Clean(path)
	if absolute, err := filepath.Abs(key); err == nil {
		key = absolute
	}
	pathLocks.Lock()
	mutex, ok := pathLocks.byPath[key]
	if !ok {
		mutex = new(sync.Mutex)
		pathLocks.byPath[key] = mutex
	}
	pathLocks.Unlock()
	mutex.Lock()
	return mutex.Unlock
}

var (
	// errLockUnsupported: the platform or file system has no advisory locks.
	errLockUnsupported = errors.New("advisory file locks are not supported here")
	// ErrLockTimeout: another process kept the known-servers lock past the wait bound.
	ErrLockTimeout = errors.New("timed out waiting for the file lock")
)

// NewKnownServers returns a store backed by the file at path. The file need not
// exist yet.
func NewKnownServers(path string) *KnownServers {
	return &KnownServers{path: path}
}

// DefaultKnownServersPath is ~/.hoarddb/known_servers.
func DefaultKnownServersPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("finding the home directory for ~/.hoarddb/known_servers: %w", err)
	}
	return filepath.Join(home, ".hoarddb", "known_servers"), nil
}

// Path is the file this store reads and writes.
func (known *KnownServers) Path() string { return known.path }

// ConflictingEntriesError is a known-servers file that records two different
// keys for one host. Choosing one would be a guess about which is the real
// server, so the store refuses.
type ConflictingEntriesError struct {
	Address string
	First   string
	Second  string
	Path    string
}

func (err *ConflictingEntriesError) Error() string {
	return fmt.Sprintf("%s records two different keys for %s (%s and %s)", err.Path, err.Address, err.First, err.Second)
}

// UnsafeFileError is a known-servers file, or its directory, that group or
// other users can write to: anyone who can write it can make a hostile server
// trusted.
type UnsafeFileError struct {
	Path string
	Mode os.FileMode
	// Directory is true when the directory, not the file, is the problem.
	Directory bool
}

func (err *UnsafeFileError) Error() string {
	what, fix := "file", "chmod 600 "+err.Path
	if err.Directory {
		what, fix = "directory", "chmod 700 "+err.Path
		if err.Mode&os.ModeSticky != 0 {
			// A shared directory such as /tmp is not ours to chmod.
			fix = "move the file to a private directory (for example ~/.hoarddb/known_servers) and point WithKnownServers at it"
		}
	}
	return fmt.Sprintf("the known-servers %s %s has mode %04o, so other users can change which servers you trust; fix it with: %s",
		what, err.Path, err.Mode.Perm(), fix)
}

// checkSafe refuses a file or directory that exists and is writable by group or
// other. A missing one is fine.
func (known *KnownServers) checkSafe() error {
	for _, candidate := range []struct {
		path      string
		directory bool
	}{{filepath.Dir(known.path), true}, {known.path, false}} {
		info, err := os.Stat(candidate.path)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("checking %s: %w", candidate.path, err)
		}
		if info.Mode().Perm()&0o022 != 0 {
			return &UnsafeFileError{Path: candidate.path, Mode: info.Mode(), Directory: candidate.directory}
		}
	}
	return nil
}

// parse reads every entry in a known-servers file (handbook/trust.md section
// 4). A line is "host:port", whitespace and the hex of a raw 32-byte ed25519
// key; text after the key is ignored. Blank lines, lines starting with "#" and
// malformed lines are skipped, so a line this reader does not understand never
// stops the others from being read. Two different keys for one host is a
// [ConflictingEntriesError]: the file is refused, not chosen from.
func (known *KnownServers) parse(r io.Reader) (map[string]ed25519.PublicKey, error) {
	entries := map[string]ed25519.PublicKey{}
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || strings.HasPrefix(fields[0], "#") {
			continue
		}
		address := canonicalAddress(fields[0])
		key, err := hex.DecodeString(fields[1])
		if err != nil || len(key) != ed25519.PublicKeySize {
			continue
		}
		previous, seen := entries[address]
		switch {
		case !seen:
			entries[address] = key
		case !previous.Equal(ed25519.PublicKey(key)):
			return nil, &ConflictingEntriesError{Address: address, First: Fingerprint(previous), Second: Fingerprint(key), Path: known.path}
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("reading known servers file %s: %w", known.path, err)
	}
	return entries, nil
}

// Load returns every server the file records, by "host:port". A missing file
// is an empty store, not an error. A file others can write, or one holding two
// different keys for any host, is an error.
func (known *KnownServers) Load() (map[string]ed25519.PublicKey, error) {
	known.mu.Lock()
	defer known.mu.Unlock()
	if err := known.checkSafe(); err != nil {
		return nil, err
	}
	file, err := os.Open(known.path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]ed25519.PublicKey{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("opening known servers file: %w", err)
	}
	defer func() { _ = file.Close() }()
	return known.parse(file)
}

// canonicalAddress returns the form the hoardDB command line tool writes for a
// host:port (see [CanonicalServerAddress]). An address the tool refuses is
// returned unchanged, so a malformed line misses the lookup or is skipped
// rather than being turned into a different server.
func canonicalAddress(address string) string {
	canonical, err := CanonicalServerAddress(address)
	if err != nil {
		return address
	}
	return canonical
}

// CanonicalServerAddress returns the one form of a server address that
// known_servers records and looks up: a registered name lowercased with one
// trailing dot stripped, an IPv4 address in dotted decimal, an IPv6 address
// in net/netip's compressed form in brackets, and the port always written
// (hoarddbwire.DefaultPort when the address has none) without leading zeros.
// No name is resolved, so 127.0.0.1 and localhost stay different names.
//
// This ports hoardDB/transport's CanonicalServerAddress (the function the
// command line tool writes the file through), with the label loop spelled
// strings.Split instead of strings.SplitSeq because this module's go line is
// 1.23. clients/ is Apache-2.0 and must not import hoardDB/, so the rule lives
// here too; the tables in known_canonical_test.go and hoardDB/ringclienttest
// keep the two equal.
func CanonicalServerAddress(address string) (string, error) {
	host, portText, err := splitServerAddress(address)
	if err != nil {
		return "", err
	}
	port, err := canonicalPort(address, portText)
	if err != nil {
		return "", err
	}
	canonicalHost, err := canonicalServerHost(address, host)
	if err != nil {
		return "", err
	}
	return canonicalHost + ":" + port, nil
}

// splitServerAddress separates host and port. portText is empty when the
// address has no port.
func splitServerAddress(address string) (host, portText string, err error) {
	if address == "" {
		return "", "", errors.New("the server address is empty; write it as host:port, for example db.internal:7433")
	}
	if address[0] == '[' {
		end := strings.IndexByte(address, ']')
		if end < 0 {
			return "", "", fmt.Errorf("server address %q opens a bracket but never closes it; write an IPv6 address as [::1]:7433", address)
		}
		rest := address[end+1:]
		switch {
		case rest == "":
			return address[:end+1], "", nil
		case rest == ":":
			return "", "", fmt.Errorf("server address %q ends with ':' but has no port; write [address]:port, for example [::1]:7433, or leave out the ':' for the default port", address)
		case rest[0] == ':':
			return address[:end+1], rest[1:], nil
		default:
			return "", "", fmt.Errorf("server address %q has %q after the bracketed address; write it as [address]:port", address, rest)
		}
	}
	switch strings.Count(address, ":") {
	case 0:
		return address, "", nil
	case 1:
		host, portText, _ = strings.Cut(address, ":")
		if portText == "" {
			return "", "", fmt.Errorf("server address %q ends with ':' but has no port; write host:port, for example db.internal:7433", address)
		}
		return host, portText, nil
	default:
		return "", "", fmt.Errorf("server address %q has more than one ':'; an IPv6 address must be written in brackets, for example [::1]:7433", address)
	}
}

// canonicalPort writes the port in decimal without leading zeros, or the
// default port when there is none.
func canonicalPort(address, portText string) (string, error) {
	if portText == "" {
		return strconv.Itoa(hoarddbwire.DefaultPort), nil
	}
	refusal := fmt.Errorf("the port of server address %q is %q, which is not a number from 1 to 65535 (at most five digits)", address, portText)
	if len(portText) > 5 || strings.Trim(portText, "0123456789") != "" {
		return "", refusal
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", refusal
	}
	return strconv.Itoa(port), nil
}

// canonicalServerHost canonicalises the host part (brackets included for
// IPv6).
func canonicalServerHost(address, host string) (string, error) {
	if strings.HasPrefix(host, "[") {
		literal := host[1 : len(host)-1]
		ip, err := netip.ParseAddr(literal)
		if err != nil || !ip.Is6() {
			return "", fmt.Errorf("server address %q holds %q in brackets, which is not an IPv6 address; brackets are for IPv6 only (write an IPv4 address as 127.0.0.1:7433)", address, literal)
		}
		if zone := ip.Zone(); zone != "" && !isHostNameText(zone) {
			return "", fmt.Errorf("server address %q has the zone %q, which may only hold letters, digits, '-', '_' and '.'", address, zone)
		}
		return "[" + ip.String() + "]", nil
	}
	if host == "" {
		return "", fmt.Errorf("server address %q has no host; write host:port, for example db.internal:7433", address)
	}
	if strings.Trim(host, "0123456789.") == "" {
		ip, err := netip.ParseAddr(host)
		if err != nil || !ip.Is4() {
			return "", fmt.Errorf("server address %q has the host %q, which is made of digits and dots but is not an IPv4 address (four numbers from 0 to 255 without leading zeros, like 127.0.0.1)", address, host)
		}
		return ip.String(), nil
	}
	// The character check runs on the host as written, before any case
	// folding: Unicode lower-casing maps U+0130 to "i" and U+212A (Kelvin) to
	// "k", which would make a non-ASCII name canonicalise to an ASCII one.
	if !isHostNameText(host) {
		return "", fmt.Errorf("server address %q has the host %q; a host name may only hold ASCII letters, digits, '-', '_' and '.'", address, host)
	}
	name := asciiLower(strings.TrimSuffix(host, "."))
	for _, label := range strings.Split(name, ".") {
		if label == "" {
			return "", fmt.Errorf("server address %q has the host %q, which has an empty label (two dots together, or a leading dot)", address, host)
		}
	}
	return name, nil
}

// asciiLower lowercases ASCII letters only, so the comparison follows the
// command line tool's rule and no Unicode mapping can fold two names together.
func asciiLower(text string) string {
	lowered := []byte(text)
	for index, character := range lowered {
		if character >= 'A' && character <= 'Z' {
			lowered[index] = character + ('a' - 'A')
		}
	}
	return string(lowered)
}

// isHostNameText reports whether text holds only ASCII letters, digits, '-',
// '_' and '.', so a canonical address can never hold a field separator.
func isHostNameText(text string) bool {
	for index := 0; index < len(text); index++ {
		character := text[index]
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z',
			character >= '0' && character <= '9', character == '-', character == '_', character == '.':
		default:
			return false
		}
	}
	return true
}

// Lookup returns the fingerprint recorded for address ("host:port"), and
// whether one is recorded. The address is compared in the canonical form the
// hoardDB command line tool writes (see [canonicalAddress]), so any spelling of
// the same host name and port finds the line. It loads the whole file, so every
// error [Load] returns is returned here too.
func (known *KnownServers) Lookup(address string) (string, bool, error) {
	entries, err := known.Load()
	if err != nil {
		return "", false, err
	}
	key, found := entries[canonicalAddress(address)]
	if !found {
		return "", false, nil
	}
	return Fingerprint(key), true, nil
}

// Add records key as the trusted key of address, in the canonical form (see
// [canonicalAddress]), atomically: under a
// process-wide mutex for the path, this store's mutex and an advisory file lock it re-reads the file, and if a
// different key is now recorded for the host it returns a [KeyChangedError]
// instead of adding a second one. That closes the window between Lookup, a
// prompt and Add in which two connections to a host showing a different key each
// time could both be trusted, in this process or another. Recording the same key
// again changes nothing. Only one line is ever appended.
func (known *KnownServers) Add(address string, key ed25519.PublicKey) error {
	address = canonicalAddress(address)
	defer lockPath(known.path)()
	known.mu.Lock()
	defer known.mu.Unlock()
	if err := known.checkSafe(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(known.path), 0o700); err != nil {
		return fmt.Errorf("creating the directory for %s: %w", known.path, err)
	}
	file, err := os.OpenFile(known.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("opening known servers file for append: %w", err)
	}
	defer func() { _ = file.Close() }()
	switch err := lockFile(file); {
	case err == nil:
		defer unlockFile(file)
	case errors.Is(err, errLockUnsupported):
		// No file locks here (some network and FUSE mounts): the mutex and the
		// re-read below still cover this process, so carry on and say so once.
		known.unsupportedOnce.Do(func() {
			if known.OnLockUnsupported != nil {
				known.OnLockUnsupported(known.path)
			}
		})
	default:
		return fmt.Errorf("locking %s: %w", known.path, err)
	}

	entries, err := known.parse(file)
	if err != nil {
		return err
	}
	if recorded, found := entries[address]; found {
		if !recorded.Equal(key) {
			return &KeyChangedError{Address: address, Recorded: Fingerprint(recorded), Presented: Fingerprint(key), Path: known.path}
		}
		return nil
	}
	line := address + " " + hex.EncodeToString(key) + "\n"
	if existing, statErr := file.Stat(); statErr == nil && existing.Size() > 0 && !endsWithNewline(file, existing.Size()) {
		line = "\n" + line
	}
	if _, err := file.WriteString(line); err != nil {
		return fmt.Errorf("recording %s in %s: %w", address, known.path, err)
	}
	return nil
}

func endsWithNewline(file *os.File, size int64) bool {
	last := make([]byte, 1)
	if _, err := file.ReadAt(last, size-1); err != nil {
		return true
	}
	return last[0] == '\n'
}
