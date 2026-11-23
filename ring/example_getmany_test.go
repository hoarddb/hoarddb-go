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

package ring_test

import (
	"context"
	"fmt"
	"log"

	"github.com/hoarddb/hoarddb-go/ring"
)

// ExampleBucket_GetMany reads a user's feed the way an application does: one page of the user's own
// feed store (post ids), then every post of the page in one frame instead of one Get each. It needs
// a server that holds the social layout (a User bucket whose documents carry a Feed store, and a
// Post bucket), so it is compiled by go test and not run.
func ExampleBucket_GetMany() {
	ctx := context.Background()
	client, err := ring.Dial(ctx, ring.Options{
		Addr: "localhost:7433", User: "root", Password: "the root password", Insecure: true, // local development only
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("closing the client: %v", err)
		}
	}()

	page, err := client.Bucket("scenarios_social", "User").StoreList(ctx, "u7", "Feed", 20, nil)
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, len(page.Elements))
	for i, entry := range page.Elements {
		ids[i] = entry.Lookup("post").StringValue()
	}

	answers, err := client.Bucket("scenarios_social", "Post").GetMany(ctx, ids, nil)
	if err != nil {
		log.Fatal(err) // the whole call failed: the connection, or a refusal that names no key
	}
	for i, answer := range answers { // one answer per id, in the order of ids
		switch {
		case answer.Err != nil:
			fmt.Println(ids[i], "was refused:", answer.Err) // that key only; the others are answered
		case !answer.Found:
			fmt.Println(ids[i], "is gone") // a missing document is data, not an error
		default:
			fmt.Println(ids[i], answer.Doc.Lookup("Text").StringValue())
		}
	}
}
