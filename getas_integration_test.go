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
	"testing"

	hoarddb "github.com/hoarddb/hoarddb-go"
)

// Real-server tests of GetAs (handbook/api-go.md section 4).

type accountDocument struct {
	Name string `bson:"Name"`
	Age  int64  `bson:"Age"`
}

func accountsBucket(t *testing.T) *hoarddb.Bucket {
	t.Helper()
	client := connectTo(t, startServer(t))
	accounts, err := client.Database("app").CreateBucket(testContext(t), "accounts", hoarddb.Hash)
	if err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return accounts
}

func TestGetAsDecodesIntoAStruct(t *testing.T) {
	ctx := testContext(t)
	accounts := accountsBucket(t)
	if _, err := accounts.Put(ctx, "ada", accountDocument{Name: "Ada", Age: 36}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	account, err := hoarddb.GetAs[accountDocument](ctx, accounts, "ada")
	if err != nil || account.Name != "Ada" || account.Age != 36 {
		t.Fatalf("GetAs = %+v, %v; want Ada, 36", account, err)
	}
	literal, err := hoarddb.GetAs[hoarddb.Map](ctx, accounts, "ada")
	if err != nil || literal["Name"] != "Ada" {
		t.Fatalf("GetAs[Map] = %v, %v; want Name Ada", literal, err)
	}
}

func TestGetAsMissIsNotFoundWithTheZeroValue(t *testing.T) {
	ctx := testContext(t)
	accounts := accountsBucket(t)
	account, err := hoarddb.GetAs[accountDocument](ctx, accounts, "nobody")
	if !errors.Is(err, hoarddb.ErrNotFound) {
		t.Fatalf("GetAs of a missing key: %+v, %v; want ErrNotFound", account, err)
	}
	if account != (accountDocument{}) {
		t.Fatalf("a miss returned %+v; want the zero value", account)
	}
}

func TestGetAsUndecodableIsInvalidArgument(t *testing.T) {
	ctx := testContext(t)
	accounts := accountsBucket(t)
	if _, err := accounts.Put(ctx, "ada", hoarddb.Map{"Name": 7, "Age": "old"}); err != nil {
		t.Fatalf("Put: %v", err)
	}
	account, err := hoarddb.GetAs[accountDocument](ctx, accounts, "ada")
	if !errors.Is(err, hoarddb.ErrInvalidArgument) {
		t.Fatalf("GetAs of a document of the wrong shape: %+v, %v; want ErrInvalidArgument", account, err)
	}
	if !strings.Contains(err.Error(), "ada") {
		t.Fatalf("the error should name the key: %v", err)
	}
	var decodeFailure *hoarddb.Error
	if !errors.As(err, &decodeFailure) || decodeFailure.Err == nil {
		t.Fatalf("the error should carry the decoder's complaint as its cause: %#v", err)
	}
}
