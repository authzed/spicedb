// Copyright (c) 2025 Alexey Mayshev and contributors. All rights reserved.
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

import (
	"time"
)

// Entry is a key-value pair that may include policy metadata for the cached entry.
//
// It is an immutable snapshot of the cached data at the time of this entry's creation, and it will not
// reflect changes afterward.
type Entry[K comparable, V any] struct {
	// Key is the entry's key.
	Key K
	// Value is the entry's value.
	Value V
	// Weight returns the entry's weight.
	//
	// If the cache was not configured with a weight then this value is always 1.
	Weight uint32
	// ExpiresAtNano is the entry's expiration time as a unix time,
	// the number of nanoseconds elapsed since January 1, 1970 UTC.
	//
	// If the cache was not configured with an expiration policy then this value is always math.MaxInt64.
	ExpiresAtNano int64
	// RefreshableAtNano is the time after which the entry will be reloaded as a unix time,
	// the number of nanoseconds elapsed since January 1, 1970 UTC.
	//
	// If the cache was not configured with a refresh policy then this value is always math.MaxInt64.
	RefreshableAtNano int64
	// SnapshotAtNano is the time when this snapshot of the entry was taken as a unix time,
	// the number of nanoseconds elapsed since January 1, 1970 UTC.
	//
	// If the cache was not configured with a time-based policy then this value is always 0.
	SnapshotAtNano int64
}

// ExpiresAt returns the entry's expiration time.
//
// If the cache was not configured with an expiration policy then this value is roughly [math.MaxInt64]
// nanoseconds away from the SnapshotAt.
func (e Entry[K, V]) ExpiresAt() time.Time {
	return time.Unix(0, e.ExpiresAtNano)
}

// ExpiresAfter returns the fixed duration used to determine if an entry should be automatically removed due
// to elapsing this time bound. An entry is considered fresh if its age is less than this
// duration, and stale otherwise. The expiration policy determines when the entry's age is reset.
//
// If the cache was not configured with an expiration policy then this value is always [math.MaxInt64].
func (e Entry[K, V]) ExpiresAfter() time.Duration {
	return time.Duration(e.ExpiresAtNano - e.SnapshotAtNano)
}

// HasExpired returns true if the entry has expired.
func (e Entry[K, V]) HasExpired() bool {
	return e.ExpiresAtNano < e.SnapshotAtNano
}

// RefreshableAt is the time after which the entry will be reloaded.
//
// If the cache was not configured with a refresh policy then this value is roughly [math.MaxInt64]
// nanoseconds away from the SnapshotAt.
func (e Entry[K, V]) RefreshableAt() time.Time {
	return time.Unix(0, e.RefreshableAtNano)
}

// RefreshableAfter returns the fixed duration used to determine if an entry should be eligible for reloading due
// to elapsing this time bound. An entry is considered fresh if its age is less than this
// duration, and stale otherwise. The refresh policy determines when the entry's age is reset.
//
// If the cache was not configured with a refresh policy then this value is always [math.MaxInt64].
func (e Entry[K, V]) RefreshableAfter() time.Duration {
	return time.Duration(e.RefreshableAtNano - e.SnapshotAtNano)
}

// SnapshotAt is the time when this snapshot of the entry was taken.
//
// If the cache was not configured with a time-based policy then this value is always 1970-01-01 00:00:00 UTC.
func (e Entry[K, V]) SnapshotAt() time.Time {
	return time.Unix(0, e.SnapshotAtNano)
}
