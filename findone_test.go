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

package hoarddb_test

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/hoarddbwire"
	"github.com/hoarddb/hoarddb-go/internal/codec"
)

// FindOne asks for one document with a SCAN of limit 1, whatever page size or
// limit the caller passed, and keeps Skip, SortBy and Projection.
func TestFindOneSendsAScanOfLimitOneAndReturnsTheDocument(t *testing.T) {
	var scans []hoarddbwire.ScanRequest
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		var scan hoarddbwire.ScanRequest
		if err := codec.Unmarshal(request.Payload, &scan); err != nil {
			t.Error(err)
			return false
		}
		scans = append(scans, scan)
		doc, err := marshal(hoarddb.Map{"Name": "Ada", "_id_": "ada"})
		if err != nil {
			t.Error(err)
			return false
		}
		c.send(request, hoarddbwire.OpScan, hoarddbwire.ScanResponse{Found: true, Documents: []hoarddbwire.Document{hoarddbwire.Document(doc)}, Total: 7})
		return true
	}))
	client := mustConnect(t, server.uri())
	doc, err := client.Database("app").Bucket("people").FindOne(testContext(t), hoarddb.Map{"Name": "Ada"},
		hoarddb.Limit(50), hoarddb.PageSize(20), hoarddb.Skip(2), hoarddb.SortBy("Born", hoarddb.Descending))
	if err != nil || stringField(t, doc, "Name") != "Ada" {
		t.Fatalf("FindOne = %s, %v", doc, err)
	}
	if len(scans) != 1 || scans[0].Limit != 1 || scans[0].Skip != 2 || scans[0].Sort == nil || scans[0].Filter == nil {
		t.Fatalf("SCANs %+v, want one with limit 1, skip 2, a sort and a filter", scans)
	}
}

func TestFindOneMissIsErrNotFoundWithTheFix(t *testing.T) {
	var scans atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		scans.Add(1)
		c.send(request, hoarddbwire.OpScan, hoarddbwire.ScanResponse{Found: true, Total: 0})
		return true
	}))
	client := mustConnect(t, server.uri())
	doc, err := client.Database("app").Bucket("people").FindOne(testContext(t), hoarddb.Map{"Name": "Nobody"})
	if !errors.Is(err, hoarddb.ErrNotFound) || doc != nil {
		t.Fatalf("FindOne = %s, %v; want ErrNotFound and no document", doc, err)
	}
	var failure *hoarddb.Error
	if !errors.As(err, &failure) || failure.Op != "findOne" || !strings.Contains(failure.Message, "app/people") || !strings.Contains(failure.Message, "Find") {
		t.Fatalf("error %#v should name the bucket and point at Find", err)
	}
	if scans.Load() != 1 {
		t.Fatalf("%d SCANs, want 1", scans.Load())
	}
}

func TestFindOneRefusesABadOptionBeforeSending(t *testing.T) {
	var frames atomic.Int32
	server := startFake(t, serveFrames(func(c *fakeConn, request hoarddbwire.Frame, _ int) bool {
		frames.Add(1)
		return false
	}))
	client := mustConnect(t, server.uri())
	_, err := client.Database("app").Bucket("people").FindOne(testContext(t), nil, hoarddb.Skip(-1))
	if err == nil || errors.Is(err, hoarddb.ErrNotFound) || frames.Load() != 0 {
		t.Fatalf("FindOne(Skip(-1)) = %v after %d frames; want a refusal and nothing sent", err, frames.Load())
	}
}
