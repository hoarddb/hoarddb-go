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
	"bytes"
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/hoarddb/hoarddb-bson/bson"
	ringwire "github.com/hoarddb/hoarddb-wire"

	"github.com/hoarddb/hoarddb-go/hoarddbwire"
)

// frameLog is every frame the fake server was sent, in order, describes included.
type frameLog struct {
	lock   sync.Mutex
	frames []hoarddbwire.Frame
}

func (l *frameLog) add(frame hoarddbwire.Frame) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.frames = append(l.frames, frame)
}

// drain returns the keyed frames logged since the last drain and how many frames of any kind there were.
func (l *frameLog) drain() (keyed []hoarddbwire.Frame, all int) {
	l.lock.Lock()
	defer l.lock.Unlock()
	for _, frame := range l.frames {
		if frame.Op == hoarddbwire.OpRingKeyed {
			keyed = append(keyed, frame)
		}
	}
	all, l.frames = len(l.frames), nil
	return keyed, all
}

// entityReply is an answer every flat method accepts for the request it sent, so a row compares
// two calls that both succeeded and not two refusals.
func entityReply(t *testing.T, request ringwire.RingKeyedRequest) ringwire.RingReply {
	t.Helper()
	found, count, scanned, skipped := true, int64(3), int32(1), int32(0)
	element := mustMarshal(t, bson.D{{Key: "sku", Value: "book"}, {Key: "Cents", Value: int64(1250)}})
	if request.Read == nil {
		reply := ringwire.RingReply{WriteOutcome: ringwire.OutcomeApplied, Found: &found, Elements: [][]byte{element}}
		if request.Op.Class == ringwire.OpClaim {
			reply.Lease = bytes.Repeat([]byte{0x5A}, 16)
		}
		return reply
	}
	switch request.Read.Class {
	case ringwire.RingClassStoreCount:
		return ringwire.RingReply{Count: &count}
	case ringwire.RingClassNearest:
		return ringwire.RingReply{Elements: [][]byte{element}, Scores: []float64{0.5}, Scanned: &scanned, Skipped: &skipped}
	case ringwire.RingClassAggregate:
		return ringwire.RingReply{Aggregate: mustMarshal(t, bson.D{{Key: "sum", Value: int64(5)}})}
	case ringwire.RingClassPeek, ringwire.RingClassStoreList:
		return ringwire.RingReply{Elements: [][]byte{element}}
	default: // get, get_fields, store_get, lease_get
		return ringwire.RingReply{Found: &found, Elements: [][]byte{element}}
	}
}

// entityClient dials a fake server for a bucket User { Name, Age, Employer, Purchases fifoStore, Friends
// hashStore } that logs every frame and answers every keyed request.
func entityClient(t *testing.T) (*Client, *frameLog) {
	t.Helper()
	log := &frameLog{}
	server := startFakeServer(t, func(conn *fakeConn, frame hoarddbwire.Frame) {
		log.add(frame)
		if frame.Op == hoarddbwire.OpRingAdmin {
			request, err := ringwire.DecodeRingAdmin(frame.Payload)
			if err != nil {
				t.Error(err)
				return
			}
			adminReplyFrame(t, conn, frame, ringwire.RingAdminReply{Cmd: request.Cmd, BucketID: 41, State: "active", DeclVersion: 2, SchemaVersionSeen: 2,
				Fields: []ringwire.RingAdminReplyField{{ID: 1, Name: "_id_", Type: "string"}, {ID: 2, Name: "Name", Type: "string"}, {ID: 3, Name: "Age", Type: "int"},
					{ID: 4, Name: "Employer", Type: "string"}, {ID: 5, Name: "Purchases", Type: "fifoStore"}, {ID: 6, Name: "Friends", Type: "hashStore"}}})
			return
		}
		request, err := ringwire.DecodeRingKeyed(frame.Payload)
		if err != nil {
			conn.refuse(frame, ringwire.CodeInvalidArgument, err.Error())
			return
		}
		payload, err := ringwire.EncodeRingReply(entityReply(t, request))
		if err != nil {
			conn.refuse(frame, ringwire.CodeInvalidArgument, err.Error())
			return
		}
		conn.reply(frame, payload)
	})
	client, err := Dial(testContext(t), server.options())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, log
}

// oneAnswer and twoAnswers box a call's results so a row can compare what two calls returned.
func oneAnswer[T any](answer T, err error) ([]any, error) { return []any{answer}, err }

func twoAnswers[T, U any](first T, second U, err error) ([]any, error) {
	return []any{first, second}, err
}

// handleRow is one method of the entity layer and the flat call it must equal. on and method name
// the handle method; write rows are given a request id and a write deadline to pass on.
type handleRow struct {
	on, method   string
	write        bool
	flat, handle func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error)
}

// entityRows is the table of design section 1: 2 methods on Bucket, 8 on Entity, 24 on Store.
func entityRows() []handleRow {
	const ada = "ada"
	lease := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	person := Doc{"Name": "Ada", "Age": int64(36)}
	older := Update{Set: bson.D{{Key: "Name", Value: "Ada L."}}, Inc: bson.D{{Key: "Age", Value: int64(1)}}}
	purchase := bson.D{{Key: "sku", Value: "book"}, {Key: "Cents", Value: int64(1250)}}
	friend := bson.D{{Key: "since", Value: int64(2019)}}
	visits := map[string]any{"visits": 1, "d.14": int64(2)}
	employers := []ExpandRequest{{Field: "Employer", Database: "db", Bucket: "Company", Fields: []string{"Name"}}}
	friendDocs := &ExpandRequest{Database: "db", Bucket: "User", Fields: []string{"Name"}}
	vector := NearestQuery{Vector: []byte{0, 0, 128, 63, 0, 0, 0, 64}, K: 3, Metric: "l2", Field: "Embedding"}
	spend := AggregateQuery{Kind: "sum by", Field: "Cents", By: "sku", Limit: 7, After: []byte{9}}
	cursor := []byte{0xC0, 0x01}
	return []handleRow{
		{on: "Bucket", method: "Entity",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Get(ctx, "grace"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity("grace").Get(ctx))
			}},
		{on: "Bucket", method: "Field",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreCount(ctx, "grace", "Friends"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field("grace", "Friends").Count(ctx))
			}},

		{on: "Entity", method: "Store",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreCount(ctx, "alan", "Purchases"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity("alan").Store("Purchases").Count(ctx))
			}},
		{on: "Entity", method: "Get",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Get(ctx, ada))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Get(ctx))
			}},
		{on: "Entity", method: "GetFields",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.GetFields(ctx, ada, []string{"Name", "Age"}))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).GetFields(ctx, []string{"Name", "Age"}))
			}},
		{on: "Entity", method: "GetExpanded",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return twoAnswers(users.GetExpanded(ctx, ada, []string{"Employer"}, employers, true))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return twoAnswers(users.Entity(ada).GetExpanded(ctx, []string{"Employer"}, employers, true))
			}},
		{on: "Entity", method: "Insert", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Insert(ctx, ada, person, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Insert(ctx, person, options...))
			}},
		{on: "Entity", method: "Upsert", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Upsert(ctx, ada, person, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Upsert(ctx, person, options...))
			}},
		{on: "Entity", method: "Update", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Update(ctx, ada, older, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Update(ctx, older, options...))
			}},
		{on: "Entity", method: "Delete", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Delete(ctx, ada, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Delete(ctx, options...))
			}},

		{on: "Store", method: "Push", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Push(ctx, ada, "Purchases", purchase, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").Push(ctx, purchase, options...))
			}},
		{on: "Store", method: "Put", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Put(ctx, ada, "Friends", "grace", friend, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Friends").Put(ctx, "grace", friend, options...))
			}},
		{on: "Store", method: "Inc", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Inc(ctx, ada, "Friends", "grace", visits, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Friends").Inc(ctx, "grace", visits, options...))
			}},
		{on: "Store", method: "IncAfter", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return twoAnswers(users.IncAfter(ctx, ada, "Friends", "grace", visits, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return twoAnswers(users.Field(ada, "Friends").IncAfter(ctx, "grace", visits, options...))
			}},
		{on: "Store", method: "Pop", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Pop(ctx, ada, "Purchases", options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").Pop(ctx, options...))
			}},
		{on: "Store", method: "PopN", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.PopN(ctx, ada, "Purchases", 4, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").PopN(ctx, 4, options...))
			}},
		{on: "Store", method: "Take", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Take(ctx, ada, "Friends", "grace", options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Friends").Take(ctx, "grace", options...))
			}},
		{on: "Store", method: "Peek",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Peek(ctx, ada, "Purchases", 5))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").Peek(ctx, 5))
			}},
		{on: "Store", method: "PeekKeyed",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.PeekKeyed(ctx, ada, "Purchases", 6))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").PeekKeyed(ctx, 6))
			}},
		{on: "Store", method: "Nearest",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Nearest(ctx, ada, "Friends", vector))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Friends").Nearest(ctx, vector))
			}},
		{on: "Store", method: "Claim", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Claim(ctx, ada, "Purchases", 8, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").Claim(ctx, 8, options...))
			}},
		{on: "Store", method: "Ack", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Ack(ctx, ada, "Purchases", lease, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").Ack(ctx, lease, options...))
			}},
		{on: "Store", method: "Release", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Release(ctx, ada, "Purchases", lease, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").Release(ctx, lease, options...))
			}},
		{on: "Store", method: "Extend", write: true,
			flat: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Extend(ctx, ada, "Purchases", lease, 45000, options...))
			},
			handle: func(ctx context.Context, users *Bucket, options []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").Extend(ctx, lease, 45000, options...))
			}},
		{on: "Store", method: "LeaseGet",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.LeaseGet(ctx, ada, "Purchases", lease))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").LeaseGet(ctx, lease))
			}},
		{on: "Store", method: "Aggregate",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Aggregate(ctx, ada, "Purchases", spend))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").Aggregate(ctx, spend))
			}},
		{on: "Store", method: "Sum",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Sum(ctx, ada, "Purchases", "Cents"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").Sum(ctx, "Cents"))
			}},
		{on: "Store", method: "SumBy",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.SumBy(ctx, ada, "Purchases", "Cents", "sku", 11, cursor))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").SumBy(ctx, "Cents", "sku", 11, cursor))
			}},
		{on: "Store", method: "CountBy",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.CountBy(ctx, ada, "Purchases", "sku", 12, cursor))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Purchases").CountBy(ctx, "sku", 12, cursor))
			}},
		{on: "Store", method: "Get",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreGet(ctx, ada, "Friends", "grace"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Friends").Get(ctx, "grace"))
			}},
		{on: "Store", method: "List",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreList(ctx, ada, "Friends", 13, cursor))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Friends").List(ctx, 13, cursor))
			}},
		{on: "Store", method: "ListExpanded",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreListExpanded(ctx, ada, "Friends", 14, cursor, friendDocs, true))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Friends").ListExpanded(ctx, 14, cursor, friendDocs, true))
			}},
		{on: "Store", method: "Count",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.StoreCount(ctx, ada, "Friends"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Entity(ada).Store("Friends").Count(ctx))
			}},
		{on: "Store", method: "Describe",
			flat: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.DescribeStore(ctx, ada, "Purchases"))
			},
			handle: func(ctx context.Context, users *Bucket, _ []CallOption) ([]any, error) {
				return oneAnswer(users.Field(ada, "Purchases").Describe(ctx))
			}},
	}
}

// TestEntityStoreSendsTheSameFrameAsTheFlatCall: [GUARD: a handle that sends another request than the
// flat method it stands for, or drops an argument, an option or an answer]. Every method of the entity
// layer sends the opcode and payload bytes the flat Bucket call sends for the same document and field,
// under the request id it was given, and returns what the flat call returns for the same reply.
func TestEntityStoreSendsTheSameFrameAsTheFlatCall(t *testing.T) {
	client, log := entityClient(t)
	users := client.Bucket("db", "User")
	if _, err := users.Info(testContext(t)); err != nil { // learn the header once, so each call below is one frame
		t.Fatal(err)
	}
	log.drain()
	rows := entityRows()
	if len(rows) != 34 {
		t.Fatalf("the table has %d rows and the entity layer has 34 methods: 2 on Bucket, 8 on Entity, 24 on Store", len(rows))
	}
	requestID := [16]byte{0xE1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0x20}
	failed := 0
	for _, row := range rows {
		passed := t.Run(row.on+"."+row.method, func(t *testing.T) {
			var options []CallOption
			if row.write {
				options = []CallOption{WithRequestID(requestID), WithWriteDeadline(1500)}
			}
			flatAnswers, flatErr := row.flat(testContext(t), users, options)
			flatFrames, _ := log.drain()
			if flatErr != nil || len(flatFrames) != 1 {
				t.Fatalf("the flat call sent %d keyed frames and returned %v, want one frame and no error", len(flatFrames), flatErr)
			}
			handleAnswers, handleErr := row.handle(testContext(t), users, options)
			handleFrames, _ := log.drain()
			if handleErr != nil {
				t.Fatalf("the handle call returned %v and the flat call returned no error", handleErr)
			}
			if len(handleFrames) != 1 {
				t.Fatalf("the handle call sent %d keyed frames, want 1", len(handleFrames))
			}
			flatFrame, handleFrame := flatFrames[0], handleFrames[0]
			if handleFrame.Op != flatFrame.Op || !bytes.Equal(handleFrame.Payload, flatFrame.Payload) {
				t.Fatalf("the handle call sent\n  op 0x%02X payload %x\nand the flat call sent\n  op 0x%02X payload %x", handleFrame.Op, handleFrame.Payload, flatFrame.Op, flatFrame.Payload)
			}
			if row.write && (flatFrame.ID != requestID || handleFrame.ID != requestID) {
				t.Fatalf("the request id sent is %x by the handle and %x by the flat call, want the WithRequestID id %x", handleFrame.ID, flatFrame.ID, requestID)
			}
			if !reflect.DeepEqual(handleAnswers, flatAnswers) {
				t.Fatalf("the handle call returned %+v and the flat call returned %+v for the same reply", handleAnswers, flatAnswers)
			}
		})
		if !passed {
			failed++
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d rows failed", failed, len(rows))
	}
}

// TestEntityTableNamesEveryHandleMethod: [GUARD: a handle method added or renamed without a row]. The
// rows name exactly the exported methods of Entity and Store, and two methods of Bucket.
func TestEntityTableNamesEveryHandleMethod(t *testing.T) {
	named := map[string]map[string]bool{"Bucket": {}, "Entity": {}, "Store": {}}
	for _, row := range entityRows() {
		if named[row.on] == nil || named[row.on][row.method] {
			t.Fatalf("row %s.%s is on an unknown type or is there twice", row.on, row.method)
		}
		named[row.on][row.method] = true
	}
	if len(named["Bucket"]) != 2 || len(named["Entity"]) != 8 || len(named["Store"]) != 24 {
		t.Fatalf("the table has %d Bucket, %d Entity and %d Store rows, want 2, 8 and 24", len(named["Bucket"]), len(named["Entity"]), len(named["Store"]))
	}
	for on, handle := range map[string]reflect.Type{"Bucket": reflect.TypeOf(&Bucket{}), "Entity": reflect.TypeOf(&Entity{}), "Store": reflect.TypeOf(&Store{})} {
		for method := range named[on] {
			if _, ok := handle.MethodByName(method); !ok {
				t.Errorf("the table has a row %s.%s and *%s has no such method", on, method, on)
			}
		}
		if on != "Bucket" && handle.NumMethod() != len(named[on]) {
			t.Errorf("*%s has %d exported methods and the table has %d rows for it: add the row with the method", on, handle.NumMethod(), len(named[on]))
		}
	}
}

// TestEntityHandlesSendNothingUntilCalled: making a handle is free, so an application may make one
// per document per request: no describe and no request leaves the client.
func TestEntityHandlesSendNothingUntilCalled(t *testing.T) {
	client, log := entityClient(t)
	users := client.Bucket("db", "User")
	entity := users.Entity("ada")
	if entity == nil || entity.Store("Purchases") == nil || users.Field("ada", "Friends") == nil {
		t.Fatal("a handle constructor returned nil")
	}
	// A round trip after the handles were made: whatever they had sent would be logged before it.
	if _, err := client.DescribeBucket(testContext(t), "db", "User"); err != nil {
		t.Fatal(err)
	}
	if _, all := log.drain(); all != 1 {
		t.Fatalf("the server saw %d frames, want only the describe this test sent: a handle constructor sent something", all)
	}
}

// TestEntityHandleOfABatchBucketQueues: [GUARD: a handle that forgets the batch of its bucket and sends
// the write alone]. A handle made from Bucket.In queues its writes on the batch, as the flat methods do.
func TestEntityHandleOfABatchBucketQueues(t *testing.T) {
	client, log := entityClient(t)
	batch := client.NewBatch()
	ada := client.Bucket("db", "User").In(batch).Entity("ada")
	if _, err := ada.Upsert(testContext(t), Doc{"Name": "Ada"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ada.Store("Purchases").Push(testContext(t), bson.D{{Key: "sku", Value: "book"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Bucket("db", "User").In(batch).Field("ada", "Friends").Put(testContext(t), "grace", bson.D{{Key: "since", Value: int64(2019)}}); err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 3 {
		t.Fatalf("the batch holds %d operations, want the 3 queued through handles", batch.Len())
	}
	if keyed, _ := log.drain(); len(keyed) != 0 {
		t.Fatalf("%d keyed requests were sent for writes queued on a batch", len(keyed))
	}
}
