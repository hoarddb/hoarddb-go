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
	"strings"
	"testing"
)

// TestGetManyRefusesABadFieldsListOnItsOwnKey: Fields the server's codec refuses (an empty non-nil
// list, 65 names, a name twice) is that key's Err, never the failure of the call; the other keys
// of the frame are sent and answered, and the bad key is not sent.
func TestGetManyRefusesABadFieldsListOnItsOwnKey(t *testing.T) {
	sixtyFive := make([]string, 65)
	for i := range sixtyFive {
		sixtyFive[i] = strings.Repeat("f", 3) + string(rune('A'+i/26)) + string(rune('a'+i%26))
	}
	for name, bad := range map[string][]string{"empty": {}, "65 names": sixtyFive, "duplicate": {"Name", "Name"}} {
		t.Run(name, func(t *testing.T) {
			fake, server := newGetManyFake(t, "User")
			client := dialFake(t, server)
			bucket := client.Bucket("app", "User")
			answers, err := client.GetMany(testContext(t), []Key{
				{Bucket: bucket, ID: "ada"},
				{Bucket: bucket, ID: "bob", Fields: bad},
				{Bucket: bucket, ID: "cyd", Fields: []string{"Name"}},
			})
			if err != nil {
				t.Fatalf("one bad key failed the call: %v", err)
			}
			if docID(t, answers[0]) != "ada" || docID(t, answers[2]) != "cyd" {
				t.Fatalf("the good keys were not answered: %+v", answers)
			}
			if answers[1].Err == nil || answers[1].Found {
				t.Fatalf("the bad key has no Err: %+v", answers[1])
			}
			fake.lock.Lock()
			defer fake.lock.Unlock()
			if len(fake.requests) != 1 || len(fake.requests[0].Reads) != 2 {
				t.Fatalf("frames %+v, want one frame of the two good keys", fake.requests)
			}
		})
	}
}

// TestBucketGetManyWithEmptyFieldsAnswersEveryKeyWithTheRefusal: Bucket.GetMany(ids, []string{})
// refuses each key by name and returns the answers, with no frame sent.
func TestBucketGetManyWithEmptyFieldsAnswersEveryKeyWithTheRefusal(t *testing.T) {
	fake, server := newGetManyFake(t, "User")
	client := dialFake(t, server)
	answers, err := client.Bucket("app", "User").GetMany(testContext(t), []string{"ada", "bob"}, []string{})
	if err != nil {
		t.Fatalf("an empty Fields failed the call: %v", err)
	}
	if len(answers) != 2 || answers[0].Err == nil || answers[1].Err == nil {
		t.Fatalf("answers %+v, want both refused", answers)
	}
	if fake.frames.Load() != 0 {
		t.Fatal("a frame was sent for keys that were all refused")
	}
}
