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
	"testing"

	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// TestCreateDatabaseWithStorageSendsAndReturnsTheWord: the storage word goes out in the
// create_database request, and the storage the server answers comes back in DatabaseInfo.
func TestCreateDatabaseWithStorageSendsAndReturnsTheWord(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		if request.Cmd != ringwire.AdminCreateDatabase || request.Name != "cache" || request.Storage != "memory" || request.RF == nil || *request.RF != 1 {
			t.Errorf("the create_database request lost its storage: %+v", request)
		}
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, DatabaseID: 7, RF: 1, Storage: "memory"})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	info, err := client.CreateDatabaseWithStorage(testContext(t), "cache", 1, "memory")
	if err != nil {
		t.Fatal(err)
	}
	if info != (DatabaseInfo{ID: 7, RF: 1, Storage: "memory"}) {
		t.Fatalf("CreateDatabaseWithStorage gave %+v", info)
	}
}

// TestDescribeDatabaseReturnsStorageAndMemoryFigures: the storage word and the two memory
// figures of a memory database come back in DatabaseDescription.
func TestDescribeDatabaseReturnsStorageAndMemoryFigures(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, Name: "cache", DatabaseID: 7, RF: 1, Storage: "memory",
			MemoryBytes: 4096, MemoryCapBytes: 8192, Buckets: []ringwire.RingAdminListed{}})
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	described, err := client.DescribeDatabase(testContext(t), "cache", PageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if described.Storage != "memory" || described.MemoryBytes != 4096 || described.MemoryCapBytes != 8192 || described.ID != 7 {
		t.Fatalf("DescribeDatabase gave %+v", described)
	}
}
