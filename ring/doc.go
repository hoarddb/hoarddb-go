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

// Package ring is the Go client for a hoardDB ring cluster: the TLS connection, AUTH, and the
// RING_ADMIN, RING_KEYED, RING_BATCH, RING_GET_MANY, RING_LIST, RING_COUNT and RING_TOPOLOGY
// requests.
//
// A bucket holds documents (entities), and a document carries its own stores. Everything
// keyed is a method of a [Bucket]:
//
//	client, err := ring.Dial(ctx, ring.Options{
//		Addr: "127.0.0.1:7433", User: "root", Password: pw, Insecure: true, // local development only
//	})
//	if err != nil { ... }
//	defer client.Close()
//
//	_, err = client.CreateDatabase(ctx, "app", 0)
//	_, err = client.CreateBucket(ctx, "app", "User", []ring.FieldDecl{
//		{Name: "Name", Type: "string"}, {Name: "Purchases", Type: "fifoStore"},
//	})
//	users := client.Bucket("app", "User")
//	_, err = users.Insert(ctx, "ada", ring.Doc{"Name": "Ada"})
//	_, err = users.Push(ctx, "ada", "Purchases", bson.D{{Key: "item", Value: "coffee"}})
//	doc, err := users.Get(ctx, "ada")
//
// A Client is safe for concurrent use. It keeps one connection and pipelines requests on
// it: every request carries a 16-byte request id and its reply is matched by that id, so
// replies may arrive in any order. A retry of a write passes the same id with
// [WithRequestID], which makes the owner answer from its dedupe window instead of applying
// the write twice.
//
// The header of a keyed request (bucket id, declaration version, schema version seen) is
// learned from the RING_ADMIN reply and cached per bucket. A SCHEMA_NOT_CURRENT refusal
// refreshes the cached header and sends the request once more under the same request id,
// unless [Options.NoResend] is set: then one frame is sent and the refusal comes back as it
// is.
//
// The request and reply codecs are the module github.com/hoarddb/hoarddb-wire, which the
// server uses too, so the two cannot disagree on a byte. Documents and values are
// github.com/hoarddb/hoarddb-bson types. The package imports nothing from the server.
package ring
