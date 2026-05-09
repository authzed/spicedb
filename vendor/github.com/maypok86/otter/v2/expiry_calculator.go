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

// ExpiryCalculator calculates when cache entries expire. A single expiration time is retained so that the lifetime
// of an entry may be extended or reduced by subsequent evaluations.
type ExpiryCalculator[K comparable, V any] interface {
	// ExpireAfterCreate specifies that the entry should be automatically removed from the cache once the duration has
	// elapsed after the entry's creation. To indicate no expiration, an entry may be given an
	// excessively long period.
	//
	// NOTE: ExpiresAtNano and RefreshableAtNano are not initialized at this stage.
	ExpireAfterCreate(entry Entry[K, V]) time.Duration
	// ExpireAfterUpdate specifies that the entry should be automatically removed from the cache once the duration has
	// elapsed after the replacement of its value. To indicate no expiration, an entry may be given an
	// excessively long period. The entry.ExpiresAfter() may be returned to not modify the expiration time.
	ExpireAfterUpdate(entry Entry[K, V], oldValue V) time.Duration
	// ExpireAfterRead specifies that the entry should be automatically removed from the cache once the duration has
	// elapsed after its last read. To indicate no expiration, an entry may be given an excessively
	// long period. The entry.ExpiresAfter() may be returned to not modify the expiration time.
	ExpireAfterRead(entry Entry[K, V]) time.Duration
}

type varExpiryCreating[K comparable, V any] struct {
	f func(entry Entry[K, V]) time.Duration
}

func (c *varExpiryCreating[K, V]) ExpireAfterCreate(entry Entry[K, V]) time.Duration {
	return c.f(entry)
}

func (c *varExpiryCreating[K, V]) ExpireAfterUpdate(entry Entry[K, V], oldValue V) time.Duration {
	return entry.ExpiresAfter()
}

func (c *varExpiryCreating[K, V]) ExpireAfterRead(entry Entry[K, V]) time.Duration {
	return entry.ExpiresAfter()
}

// ExpiryCreating returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation. The expiration time is
// not modified when the entry is updated or read.
func ExpiryCreating[K comparable, V any](duration time.Duration) ExpiryCalculator[K, V] {
	return ExpiryCreatingFunc(func(entry Entry[K, V]) time.Duration {
		return duration
	})
}

// ExpiryCreatingFunc returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation. The expiration time is
// not modified when the entry is updated or read.
func ExpiryCreatingFunc[K comparable, V any](f func(entry Entry[K, V]) time.Duration) ExpiryCalculator[K, V] {
	return &varExpiryCreating[K, V]{
		f: f,
	}
}

type varExpiryWriting[K comparable, V any] struct {
	f func(entry Entry[K, V]) time.Duration
}

func (w *varExpiryWriting[K, V]) ExpireAfterCreate(entry Entry[K, V]) time.Duration {
	return w.f(entry)
}

func (w *varExpiryWriting[K, V]) ExpireAfterUpdate(entry Entry[K, V], oldValue V) time.Duration {
	return w.f(entry)
}

func (w *varExpiryWriting[K, V]) ExpireAfterRead(entry Entry[K, V]) time.Duration {
	return entry.ExpiresAfter()
}

// ExpiryWriting returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation or replacement of its value.
// The expiration time is not modified when the entry is read.
func ExpiryWriting[K comparable, V any](duration time.Duration) ExpiryCalculator[K, V] {
	return ExpiryWritingFunc(func(entry Entry[K, V]) time.Duration {
		return duration
	})
}

// ExpiryWritingFunc returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation or replacement of its value.
// The expiration time is not modified when the entry is read.
func ExpiryWritingFunc[K comparable, V any](f func(entry Entry[K, V]) time.Duration) ExpiryCalculator[K, V] {
	return &varExpiryWriting[K, V]{
		f: f,
	}
}

type varExpiryAccessing[K comparable, V any] struct {
	f func(entry Entry[K, V]) time.Duration
}

func (a *varExpiryAccessing[K, V]) ExpireAfterCreate(entry Entry[K, V]) time.Duration {
	return a.f(entry)
}

func (a *varExpiryAccessing[K, V]) ExpireAfterUpdate(entry Entry[K, V], oldValue V) time.Duration {
	return a.f(entry)
}

func (a *varExpiryAccessing[K, V]) ExpireAfterRead(entry Entry[K, V]) time.Duration {
	return a.f(entry)
}

// ExpiryAccessing returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation, replacement of its value,
// or after it was last read.
func ExpiryAccessing[K comparable, V any](duration time.Duration) ExpiryCalculator[K, V] {
	return ExpiryAccessingFunc(func(entry Entry[K, V]) time.Duration {
		return duration
	})
}

// ExpiryAccessingFunc returns an [ExpiryCalculator] that specifies that the entry should be automatically deleted from
// the cache once the duration has elapsed after the entry's creation, replacement of its value,
// or after it was last read.
func ExpiryAccessingFunc[K comparable, V any](f func(entry Entry[K, V]) time.Duration) ExpiryCalculator[K, V] {
	return &varExpiryAccessing[K, V]{
		f: f,
	}
}
