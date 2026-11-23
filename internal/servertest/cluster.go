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

package servertest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/hoarddbwire/trust"
)

// Cluster is several servers forming one cluster on loopback, each its own
// process. Every node has the same root user and password.
type Cluster struct {
	nodes []*Server
	// token, seeds and pins are what every node was started with, kept for a
	// node started beside the cluster.
	token       string
	seeds, pins []string
}

// StartCluster runs n nodes (handbook/testing.md section 3.2) and returns when
// every node's HEALTH_CHECK answers "membership: confirmed". The nodes are
// stopped when the test ends.
func StartCluster(t testing.TB, n int, opts Options) *Cluster {
	t.Helper()
	if n < 1 {
		t.Fatalf("a cluster needs at least one node, not %d", n)
	}
	taken := make(map[int]bool)
	ringPorts := make([]int, n)
	for i := range ringPorts {
		ringPorts[i] = reserveRingPort(t, taken)
	}
	otherPorts := reservePorts(t, 2*n)

	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		t.Fatal(err)
	}
	clusterToken := hex.EncodeToString(token)

	root := t.TempDir()
	cluster := &Cluster{nodes: make([]*Server, n), token: clusterToken}
	seeds := make([]string, n)
	pins := make([]string, n)
	for i := range cluster.nodes {
		name := fmt.Sprintf("node%d", i+1)
		directory := filepath.Join(root, name)
		fingerprint := writeIdentity(t, filepath.Join(directory, "data", "keys"), name)
		ring := fmt.Sprintf("127.0.0.1:%d", ringPorts[i])
		seeds[i] = name + "@" + ring
		pins[i] = ring + "=" + fingerprint
		cluster.nodes[i] = &Server{
			Address:     net.JoinHostPort("127.0.0.1", fmt.Sprint(otherPorts[2*i])),
			RingAddress: ring,
			Fingerprint: fingerprint,
			User:        rootUser,
			Password:    rootPassword,
		}
	}
	cluster.seeds, cluster.pins = seeds, pins
	for i, server := range cluster.nodes {
		server.env = []string{
			"HOARDDB_NODE_ID=" + fmt.Sprintf("node%d", i+1),
			"HOARDDB_LISTEN=" + fmt.Sprintf("127.0.0.1:%d", ringPorts[i]),
			"HOARDDB_TCP_LISTEN=" + server.Address,
			fmt.Sprintf("HOARDDB_METRICS_LISTEN=127.0.0.1:%d", otherPorts[2*i+1]),
			"HOARDDB_SEED_NODES=" + strings.Join(seeds, ","),
			"HOARDDB_SEED_FINGERPRINTS=" + strings.Join(pins, ","),
			"HOARDDB_CLUSTER_TOKEN=" + clusterToken,
			fmt.Sprintf("HOARDDB_REPLICATION_FACTOR=%d", min(n, 3)),
		}
		server.prepare(t, filepath.Join(root, fmt.Sprintf("node%d", i+1)), opts)
	}
	for _, server := range cluster.nodes {
		server.run(t)
	}
	for i := range cluster.nodes {
		cluster.awaitConfirmed(t, i)
	}
	cluster.awaitReplication(t, min(n, 3))
	return cluster
}

// StartUnjoined runs one node configured as the first of a three-node cluster
// whose other two members never start (handbook/testing.md section 3.2, with
// only node1 running). No member ever confirms it, so it stays a node that has
// not been added to its cluster: it answers PING, AUTH and HEALTH_CHECK, and
// refuses anything that names a database or bucket with UNAVAILABLE
// (handbook/errors.md section 5). It is stopped when the test ends.
func StartUnjoined(t testing.TB, opts Options) *Server {
	t.Helper()
	taken := make(map[int]bool)
	ringPorts := []int{reserveRingPort(t, taken), reserveRingPort(t, taken), reserveRingPort(t, taken)}
	otherPorts := reservePorts(t, 2)
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	seeds := make([]string, len(ringPorts))
	pins := make([]string, len(ringPorts))
	var fingerprint string
	for i, port := range ringPorts {
		name := fmt.Sprintf("node%d", i+1)
		// Every member gets a real identity, so the seed settings are the ones a
		// formed cluster would have; only node1's is used.
		memberFingerprint := writeIdentity(t, filepath.Join(root, name, "data", "keys"), name)
		if i == 0 {
			fingerprint = memberFingerprint
		}
		ring := fmt.Sprintf("127.0.0.1:%d", port)
		seeds[i] = name + "@" + ring
		pins[i] = ring + "=" + memberFingerprint
	}
	server := &Server{
		Address:     net.JoinHostPort("127.0.0.1", fmt.Sprint(otherPorts[0])),
		Fingerprint: fingerprint,
		User:        rootUser,
		Password:    rootPassword,
	}
	server.env = []string{
		"HOARDDB_NODE_ID=node1",
		fmt.Sprintf("HOARDDB_LISTEN=127.0.0.1:%d", ringPorts[0]),
		"HOARDDB_TCP_LISTEN=" + server.Address,
		fmt.Sprintf("HOARDDB_METRICS_LISTEN=127.0.0.1:%d", otherPorts[1]),
		"HOARDDB_SEED_NODES=" + strings.Join(seeds, ","),
		"HOARDDB_SEED_FINGERPRINTS=" + strings.Join(pins, ","),
		"HOARDDB_CLUSTER_TOKEN=" + hex.EncodeToString(token),
		"HOARDDB_REPLICATION_FACTOR=3",
	}
	server.prepare(t, filepath.Join(root, "node1"), opts)
	server.run(t)
	return server
}

// StartNewcomer runs a fourth node, "newcomer", beside the running cluster: its
// seed list is the cluster's nodes and itself, and no member adds it with
// "join" (handbook/testing.md section 3.2). It answers PING, AUTH and
// HEALTH_CHECK ("not added yet") and refuses anything that names a database or
// bucket with UNAVAILABLE. Its RingAddress is the address its own sentences
// name. It is stopped when the test ends.
func (cluster *Cluster) StartNewcomer(t testing.TB, opts Options) *Server {
	t.Helper()
	taken := make(map[int]bool)
	ringPort := reserveRingPort(t, taken)
	otherPorts := reservePorts(t, 2)
	root := t.TempDir()
	fingerprint := writeIdentity(t, filepath.Join(root, "newcomer", "data", "keys"), "newcomer")
	ring := fmt.Sprintf("127.0.0.1:%d", ringPort)
	server := &Server{
		Address:     net.JoinHostPort("127.0.0.1", fmt.Sprint(otherPorts[0])),
		RingAddress: ring,
		Fingerprint: fingerprint,
		User:        rootUser,
		Password:    rootPassword,
	}
	server.env = []string{
		"HOARDDB_NODE_ID=newcomer",
		"HOARDDB_LISTEN=" + ring,
		"HOARDDB_TCP_LISTEN=" + server.Address,
		fmt.Sprintf("HOARDDB_METRICS_LISTEN=127.0.0.1:%d", otherPorts[1]),
		"HOARDDB_SEED_NODES=" + strings.Join(append(slices.Clone(cluster.seeds), "newcomer@"+ring), ","),
		"HOARDDB_SEED_FINGERPRINTS=" + strings.Join(append(slices.Clone(cluster.pins), ring+"="+fingerprint), ","),
		"HOARDDB_CLUSTER_TOKEN=" + cluster.token,
		fmt.Sprintf("HOARDDB_REPLICATION_FACTOR=%d", len(cluster.nodes)),
	}
	server.prepare(t, filepath.Join(root, "newcomer"), opts)
	server.run(t)
	return server
}

// writeIdentity writes an ed25519 key (PKCS#8 PEM, mode 0600) and a
// self-signed certificate for it into keysDirectory, and returns the node's
// fingerprint.
func writeIdentity(t testing.TB, keysDirectory, name string) string {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost", name},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(keysDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	files := []struct {
		name  string
		block *pem.Block
		mode  os.FileMode
	}{
		{"server.key", &pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}, 0o600},
		{"server.crt", &pem.Block{Type: "CERTIFICATE", Bytes: certDER}, 0o644},
	}
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(keysDirectory, file.name), pem.EncodeToMemory(file.block), file.mode); err != nil {
			t.Fatal(err)
		}
	}
	return trust.Fingerprint(public)
}

// awaitConfirmed waits until node i reports "membership: confirmed".
func (cluster *Cluster) awaitConfirmed(t testing.TB, i int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	var last string
	for ctx.Err() == nil {
		membership, err := cluster.nodes[i].membership(ctx)
		if err == nil && membership == "confirmed" {
			return
		}
		last = fmt.Sprintf("membership %q, error %v", membership, err)
		time.Sleep(100 * time.Millisecond)
	}
	log, _ := os.ReadFile(cluster.nodes[i].LogPath)
	t.Fatalf("node %d did not become confirmed within %v (%s):\n%s", i+1, startTimeout, last, log)
}

// probeDatabase and probeBucket are where awaitReplication writes; the names
// are the helper's own, so they cannot collide with a test's.
const (
	probeDatabase = "servertest_ready"
	probeBucket   = "probe"
)

// awaitReplication returns once a write with concern "all" through node 1 is
// confirmed by want copies. Every node being "confirmed" does not mean every
// peer link is up: right after formation a first write can reach fewer
// replicas (handbook/testing.md section 3.2 item 5). On timeout it fails with
// the last copy count seen.
func (cluster *Cluster) awaitReplication(t testing.TB, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	conn, err := cluster.nodes[0].Dial(ctx)
	if err != nil {
		t.Fatalf("probing replication: %v", err)
	}
	defer func() { _ = conn.Close() }()

	type probeRequest struct {
		Database string `bson:"database"`
		Name     string `bson:"name"`
		Type     string `bson:"type"`
	}
	type probeWrite struct {
		Database     string         `bson:"database"`
		Bucket       string         `bson:"bucket"`
		Key          string         `bson:"key"`
		Data         map[string]any `bson:"data"`
		WriteConcern string         `bson:"writeconcern"`
	}
	type probeReply struct {
		Success         bool `bson:"success"`
		CopiesConfirmed int  `bson:"copies_confirmed"`
	}
	if reply, err := conn.Do(ctx, hoarddbwire.OpCreateBucket, probeRequest{Database: probeDatabase, Name: probeBucket, Type: "hash"}, nil); err != nil || reply.Op == hoarddbwire.OpError {
		t.Fatalf("creating the replication probe bucket: op %v, error %v", reply.Op, err)
	}
	last := "no write was answered"
	for attempt := 0; ctx.Err() == nil; attempt++ {
		var answer probeReply
		reply, err := conn.Do(ctx, hoarddbwire.OpPut, probeWrite{
			Database: probeDatabase, Bucket: probeBucket, Key: fmt.Sprintf("probe-%d", attempt),
			Data: map[string]any{"attempt": attempt}, WriteConcern: "all",
		}, &answer)
		switch {
		case err != nil:
			last = fmt.Sprintf("error %v", err)
		case reply.Op == hoarddbwire.OpError:
			last = "the server refused the probe write"
		case answer.Success && answer.CopiesConfirmed >= want:
			return
		default:
			last = fmt.Sprintf("copies_confirmed %d, want %d", answer.CopiesConfirmed, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("the cluster did not replicate to %d nodes within %v; last seen: %s", want, startTimeout, last)
}

// membership asks the node for its HEALTH_CHECK and returns the membership it
// reports.
func (server *Server) membership(ctx context.Context) (string, error) {
	conn, err := server.Dial(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = conn.Close() }()
	var health struct {
		Membership string `bson:"membership"`
	}
	reply, err := conn.Do(ctx, hoarddbwire.OpHealthCheck, struct{}{}, &health)
	if err != nil {
		return "", err
	}
	if reply.Op == hoarddbwire.OpError {
		return "", fmt.Errorf("HEALTH_CHECK refused")
	}
	return health.Membership, nil
}

// Node returns the i-th node (from 0). Its Dial opens a connection to that node.
func (cluster *Cluster) Node(i int) *Server { return cluster.nodes[i] }

// Stop stops node i with SIGTERM, escalating to SIGKILL after 15 seconds.
func (cluster *Cluster) Stop(i int) { cluster.nodes[i].Stop() }

// Restart starts node i again with the same data directory and variables, and
// waits until it is confirmed. A node still running is stopped first.
func (cluster *Cluster) Restart(t testing.TB, i int) {
	t.Helper()
	cluster.nodes[i].Stop()
	cluster.nodes[i].run(t)
	cluster.awaitConfirmed(t, i)
}

// Addresses returns each node's driver address.
func (cluster *Cluster) Addresses() []string {
	addresses := make([]string, len(cluster.nodes))
	for i, node := range cluster.nodes {
		addresses[i] = node.Address
	}
	return addresses
}

// Fingerprints returns each node's key fingerprint, in the order of Addresses.
func (cluster *Cluster) Fingerprints() []string {
	fingerprints := make([]string, len(cluster.nodes))
	for i, node := range cluster.nodes {
		fingerprints[i] = node.Fingerprint
	}
	return fingerprints
}

// RingAddresses returns each node's cluster address, in the order of Addresses.
func (cluster *Cluster) RingAddresses() []string {
	addresses := make([]string, len(cluster.nodes))
	for i, node := range cluster.nodes {
		addresses[i] = node.RingAddress
	}
	return addresses
}

// DropProbe drops the bucket StartCluster wrote its replication probe into, so
// the cluster holds no bucket at all, as a cluster does before its first
// CREATE_BUCKET (a HEALTH_CHECK's bucketcount is then 0).
func (cluster *Cluster) DropProbe(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), startTimeout)
	defer cancel()
	conn, err := cluster.nodes[0].Dial(ctx)
	if err != nil {
		t.Fatalf("dropping the probe bucket: %v", err)
	}
	defer func() { _ = conn.Close() }()
	reply, err := conn.Do(ctx, hoarddbwire.OpDeleteBucket, hoarddbwire.DropBucketRequest{Database: probeDatabase, Name: probeBucket}, nil)
	if err != nil || reply.Op == hoarddbwire.OpError {
		t.Fatalf("dropping the probe bucket: op %v, error %v", reply.Op, err)
	}
}
