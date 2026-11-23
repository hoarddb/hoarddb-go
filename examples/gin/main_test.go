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

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	hoarddb "github.com/hoarddb/hoarddb-go"
	"github.com/hoarddb/hoarddb-go/internal/servertest"
)

func send(t *testing.T, router *gin.Engine, method, path, body string) (int, map[string]any) {
	t.Helper()
	request := httptest.NewRequest(method, path, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)
	var answer map[string]any
	if recorder.Body.Len() > 0 {
		if err := json.Unmarshal(recorder.Body.Bytes(), &answer); err != nil {
			answer = map[string]any{"body": recorder.Body.String()}
		}
	}
	return recorder.Code, answer
}

func TestTheUsersAPIRunsAgainstAServer(t *testing.T) {
	server := servertest.Start(t, servertest.Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client, err := hoarddb.Connect(ctx, "hoarddb://"+server.User+":"+server.Password+"@"+server.Address+"/app?fingerprint="+server.Fingerprint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	users, err := setUp(ctx, client)
	if err != nil {
		t.Fatal(err)
	}
	router := newRouter(users)

	status, created := send(t, router, "POST", "/users", `{"name":"Ada","email":"ada@example.com","age":36}`)
	id, _ := created["id"].(string)
	if status != http.StatusCreated || id == "" {
		t.Fatalf("POST /users = %d %v; want 201 and the new id", status, created)
	}
	status, found := send(t, router, "GET", "/users/"+id, "")
	if status != http.StatusOK || found["name"] != "Ada" || found["email"] != "ada@example.com" || found["age"] != float64(36) {
		t.Fatalf("GET /users/%s = %d %v; want the user as stored", id, status, found)
	}
	if status, missing := send(t, router, "GET", "/users/nobody", ""); status != http.StatusNotFound {
		t.Fatalf("GET of an unknown user = %d %v; want 404", status, missing)
	}
	if status, _ := send(t, router, "POST", "/users/"+id+"/purchases", `{"item":"tea"}`); status != http.StatusCreated {
		t.Fatalf("POST purchase = %d; want 201", status)
	}
	if waiting, err := users.Field(id, "Purchases").Len(ctx); err != nil || waiting != 1 {
		t.Fatalf("the user's Purchases queue holds %d, %v; want 1", waiting, err)
	}
	if status, _ := send(t, router, "POST", "/users/nobody/purchases", `{"item":"tea"}`); status != http.StatusNotFound {
		t.Fatalf("POST purchase for an unknown user = %d; want 404", status)
	}
	if status, refused := send(t, router, "POST", "/users", `{"name":"Bob","email":"not-an-email"}`); status != http.StatusBadRequest {
		t.Fatalf("POST of an invalid user = %d %v; want 400", status, refused)
	}
}
