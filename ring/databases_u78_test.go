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

// TestU78DatabaseListingsReachTheCaller: the limit and cursor go out as sent, and the rows,
// More and Cursor of the reply come back as the typed pages.
func TestU78DatabaseListingsReachTheCaller(t *testing.T) {
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		request, err := ringwire.DecodeRingAdmin(frame.Payload)
		if err != nil {
			t.Error(err)
			return
		}
		if request.Limit == nil || *request.Limit != 2 || string(request.Cursor) != "after" {
			t.Errorf("the request lost its page: %+v", request)
		}
		switch request.Cmd {
		case ringwire.AdminShowDatabases:
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, Databases: []ringwire.RingAdminDatabase{{Name: "app", DatabaseID: 4, RF: 1}}, More: true, Cursor: []byte("app")})
		default:
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, Name: request.Database, DatabaseID: 4, RF: 1, Storage: "disk",
				Buckets: []ringwire.RingAdminListed{{Name: "User", BucketID: 9, State: "active"}}})
		}
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	page, err := client.ShowDatabases(testContext(t), PageRequest{Limit: 2, Cursor: []byte("after")})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Databases) != 1 || page.Databases[0] != (DatabaseSummary{Name: "app", ID: 4, RF: 1}) || !page.More || string(page.Cursor) != "app" {
		t.Fatalf("ShowDatabases gave %+v", page)
	}
	described, err := client.DescribeDatabase(testContext(t), "app", PageRequest{Limit: 2, Cursor: []byte("after")})
	if err != nil {
		t.Fatal(err)
	}
	if described.Name != "app" || described.ID != 4 || described.RF != 1 || len(described.Buckets) != 1 || described.Buckets[0] != (BucketSummary{Name: "User", ID: 9, State: "active"}) {
		t.Fatalf("DescribeDatabase gave %+v", described)
	}
}
