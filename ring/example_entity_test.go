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

	"github.com/hoarddb/hoarddb-bson/bson"

	"github.com/hoarddb/hoarddb-go/ring"
)

// ExampleBucket_Entity keeps a user's data with the user: the document holds the scalars, and the
// stores the bucket declares on it (here Purchases, a fifoStore, and Friends, a hashStore) are that
// user's own. It needs a server with
//
//	create bucket User in app { Name string, Purchases fifoStore, Friends hashStore }
//
// so it is compiled by go test and not run.
func ExampleBucket_Entity() {
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

	ada := client.Bucket("app", "User").Entity("ada") // a handle: nothing is sent yet
	if _, err := ada.Upsert(ctx, ring.Doc{"Name": "Ada"}); err != nil {
		log.Fatal(err)
	}

	purchases := ada.Store("Purchases") // ada's own queue, not a shared one
	if _, err := purchases.Push(ctx, bson.D{{Key: "sku", Value: "book"}, {Key: "cents", Value: int64(1250)}}); err != nil {
		log.Fatal(err)
	}
	waiting, err := purchases.Count(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("ada has", waiting, "purchases")

	if _, err := ada.Store("Friends").Put(ctx, "grace", bson.D{{Key: "since", Value: int64(2019)}}); err != nil {
		log.Fatal(err)
	}
	profile, err := ada.Get(ctx) // the scalars; the stores are read through their handles
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(profile.Lookup("Name").StringValue())
}

// ExampleBucket_Field reads one page of a user's friends with the shorthand for
// Entity(id).Store(field). Like ExampleBucket_Entity it needs a server, so it is compiled and not run.
func ExampleBucket_Field() {
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

	friends := client.Bucket("app", "User").Field("ada", "Friends")
	page, err := friends.List(ctx, 20, nil)
	if err != nil {
		log.Fatal(err)
	}
	for i, friend := range page.Elements {
		fmt.Println(page.Keys[i], friend.Lookup("since").Int64())
	}
	if page.More {
		fmt.Println("more friends follow: pass page.Cursor as after")
	}
}
