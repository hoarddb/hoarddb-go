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

// Command gin is a users API on the Gin web framework, backed by hoardDB. Each
// user carries a Purchases queue of its own.
//
//	export HOARDDB_URI='hoarddb://admin:PASSWORD@localhost:7433/app?fingerprint=SHA256:...'
//	go run .
//	curl -d '{"name":"Ada","email":"ada@example.com","age":36}' -H 'Content-Type: application/json' localhost:8080/users
package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	hoarddb "github.com/hoarddb/hoarddb-go"
)

// User is the request, the response and the stored document: json tags face
// the web, binding tags validate, bson tags face the store.
type User struct {
	Name  string `json:"name" binding:"required" bson:"Name"`
	Email string `json:"email" binding:"required,email" bson:"Email"`
	Age   int    `json:"age" binding:"gte=0,lte=150" bson:"Age"`
}

// Purchase is one entry of a user's own Purchases queue.
type Purchase struct {
	Item string `json:"item" binding:"required" bson:"Item"`
}

func setUp(ctx context.Context, client *hoarddb.Client) (*hoarddb.Bucket, error) {
	database, err := client.DefaultDatabase() // the database the URI names; creates User, with its queue, if absent
	if err != nil {
		return nil, err
	}
	return database.CreateBucket(ctx, "User", hoarddb.Hash, hoarddb.WithIfNotExists(),
		hoarddb.WithStores(hoarddb.StoreSpec{Name: "Purchases", Type: hoarddb.FIFO}))
}

func newRouter(users *hoarddb.Bucket) *gin.Engine {
	router := gin.Default() // logs each request and recovers from a panic
	router.POST("/users", func(c *gin.Context) {
		var user User
		if err := c.ShouldBindJSON(&user); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		receipt, err := users.Insert(c.Request.Context(), user) // the server assigns the key
		if err != nil {
			refuse(c, err)
			return
		}
		c.JSON(http.StatusCreated, gin.H{"id": receipt.ID})
	})
	router.GET("/users/:id", func(c *gin.Context) {
		user, err := hoarddb.GetAs[User](c.Request.Context(), users, c.Param("id"))
		if err != nil {
			refuse(c, err)
			return
		}
		c.JSON(http.StatusOK, user)
	})
	router.POST("/users/:id/purchases", func(c *gin.Context) {
		var purchase Purchase
		if err := c.ShouldBindJSON(&purchase); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		// Field is no I/O: it names this user's own queue, found by the user's key.
		if _, err := users.Field(c.Param("id"), "Purchases").Push(c.Request.Context(), purchase); err != nil {
			refuse(c, err)
			return
		}
		c.Status(http.StatusCreated)
	})
	return router
}

// refuse answers 404 for what is not there and 500 for the rest, with the driver's message.
func refuse(c *gin.Context, err error) {
	if errors.Is(err, hoarddb.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second) // bounds the setup, not the server
	defer cancel()
	client, err := hoarddb.Connect(ctx, "") // an empty string reads HOARDDB_URI
	if err != nil {
		log.Fatalf("connecting: %v", err)
	}
	defer func() { _ = client.Close() }()
	users, err := setUp(ctx, client)
	if err != nil {
		log.Fatalf("creating the User bucket: %v", err)
	}
	server := &http.Server{Addr: ":8080", Handler: newRouter(users), ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(server.ListenAndServe())
}
