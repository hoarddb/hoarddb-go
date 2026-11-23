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

package hoarddbwire

import (
	"encoding/binary"
	"math/bits"
)

// ringSlotShift takes the top 13 bits of a hash: the 8192 slots of a table.
const ringSlotShift = 51

// The five XXH64 primes (the published algorithm, seed 0).
const (
	xxhPrime1 uint64 = 11400714785074694791
	xxhPrime2 uint64 = 14029467366897019727
	xxhPrime3 uint64 = 1609587929392839161
	xxhPrime4 uint64 = 9650029242287828579
	xxhPrime5 uint64 = 2870177450012600261
)

// RingPlacementKey is the placement key of one keyed document: the database
// name and the bucket name, each with a uvarint length in front, then the
// encoded id. The prefixes keep ("ab", "c") and ("a", "bc") apart, as the
// server's storage/keys.go:PlacementKey does.
func RingPlacementKey(database, bucket string, encodedID []byte) []byte {
	key := make([]byte, 0, len(database)+len(bucket)+len(encodedID)+2*binary.MaxVarintLen64)
	key = binary.AppendUvarint(key, uint64(len(database)))
	key = append(key, database...)
	key = binary.AppendUvarint(key, uint64(len(bucket)))
	key = append(key, bucket...)
	return append(key, encodedID...)
}

// RingSlot is the slot of a keyed document in a table of 8192 slots: the top 13
// bits of the xxHash64 of its placement key. A wrong slot reads and writes the
// wrong owner with no error, so the function is frozen to the server's.
func RingSlot(database, bucket string, encodedID []byte) uint16 {
	return uint16(ringXXH64(RingPlacementKey(database, bucket, encodedID)) >> ringSlotShift)
}

// ringXXH64 is xxHash64 with seed 0, written here because the dependency gate
// allows no module but this one and the BSON codec.
func ringXXH64(input []byte) uint64 {
	length := len(input)
	var hash uint64
	if length >= 32 {
		acc1 := xxhPrime1
		acc1 += xxhPrime2
		acc2 := xxhPrime2
		acc3 := uint64(0)
		acc4 := uint64(0)
		acc4 -= xxhPrime1
		for ; len(input) >= 32; input = input[32:] {
			acc1 = xxhRound(acc1, binary.LittleEndian.Uint64(input[0:8]))
			acc2 = xxhRound(acc2, binary.LittleEndian.Uint64(input[8:16]))
			acc3 = xxhRound(acc3, binary.LittleEndian.Uint64(input[16:24]))
			acc4 = xxhRound(acc4, binary.LittleEndian.Uint64(input[24:32]))
		}
		hash = bits.RotateLeft64(acc1, 1) + bits.RotateLeft64(acc2, 7) +
			bits.RotateLeft64(acc3, 12) + bits.RotateLeft64(acc4, 18)
		hash = xxhMergeRound(hash, acc1)
		hash = xxhMergeRound(hash, acc2)
		hash = xxhMergeRound(hash, acc3)
		hash = xxhMergeRound(hash, acc4)
	} else {
		hash = xxhPrime5
	}
	hash += uint64(length)
	for ; len(input) >= 8; input = input[8:] {
		hash ^= xxhRound(0, binary.LittleEndian.Uint64(input[0:8]))
		hash = bits.RotateLeft64(hash, 27)*xxhPrime1 + xxhPrime4
	}
	if len(input) >= 4 {
		hash ^= uint64(binary.LittleEndian.Uint32(input[0:4])) * xxhPrime1
		hash = bits.RotateLeft64(hash, 23)*xxhPrime2 + xxhPrime3
		input = input[4:]
	}
	for _, octet := range input {
		hash ^= uint64(octet) * xxhPrime5
		hash = bits.RotateLeft64(hash, 11) * xxhPrime1
	}
	hash ^= hash >> 33
	hash *= xxhPrime2
	hash ^= hash >> 29
	hash *= xxhPrime3
	hash ^= hash >> 32
	return hash
}

// xxhRound folds one 8-byte lane into an accumulator.
func xxhRound(acc, lane uint64) uint64 {
	acc += lane * xxhPrime2
	acc = bits.RotateLeft64(acc, 31)
	return acc * xxhPrime1
}

// xxhMergeRound folds one accumulator into the hash of a 32-byte-or-longer input.
func xxhMergeRound(hash, acc uint64) uint64 {
	hash ^= xxhRound(0, acc)
	return hash*xxhPrime1 + xxhPrime4
}
