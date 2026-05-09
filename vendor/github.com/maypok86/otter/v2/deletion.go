// Copyright (c) 2024 Alexey Mayshev and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otter

// DeletionCause the cause why a cached entry was deleted.
type DeletionCause int

const (
	// CauseInvalidation means that the entry was manually deleted by the user.
	CauseInvalidation DeletionCause = iota + 1
	// CauseReplacement means that the entry itself was not actually deleted, but its value was replaced by the user.
	CauseReplacement
	// CauseOverflow means that the entry was evicted due to size constraints.
	CauseOverflow
	// CauseExpiration means that the entry's expiration timestamp has passed.
	CauseExpiration
)

const causeUnknown DeletionCause = 0

var deletionCauseStrings = []string{
	"Invalidation",
	"Replacement",
	"Overflow",
	"Expiration",
}

// String implements [fmt.Stringer] interface.
func (dc DeletionCause) String() string {
	if dc >= 1 && int(dc) <= len(deletionCauseStrings) {
		return deletionCauseStrings[dc-1]
	}
	return "<unknown otter.DeletionCause>"
}

// IsEviction returns true if there was an automatic deletion due to eviction
// (the cause is neither [CauseInvalidation] nor [CauseReplacement]).
func (dc DeletionCause) IsEviction() bool {
	return !(dc == CauseInvalidation || dc == CauseReplacement)
}

// DeletionEvent is an event of the deletion of a single entry.
type DeletionEvent[K comparable, V any] struct {
	// Key is the key corresponding to the deleted entry.
	Key K
	// Value is the value corresponding to the deleted entry.
	Value V
	// Cause is the cause for which entry was deleted.
	Cause DeletionCause
}

// WasEvicted returns true if there was an automatic deletion due to eviction (the cause is neither
// [CauseInvalidation] nor [CauseReplacement]).
func (de DeletionEvent[K, V]) WasEvicted() bool {
	return de.Cause.IsEviction()
}
