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

// RefreshCalculator calculates when cache entries will be reloaded. A single refresh time is retained so that the lifetime
// of an entry may be extended or reduced by subsequent evaluations.
type RefreshCalculator[K comparable, V any] interface {
	// RefreshAfterCreate returns the duration after which the entry is eligible for an automatic refresh after the
	// entry's creation. To indicate no refresh, an entry may be given an excessively long period.
	RefreshAfterCreate(entry Entry[K, V]) time.Duration
	// RefreshAfterUpdate returns the duration after which the entry is eligible for an automatic refresh after the
	// replacement of the entry's value due to an explicit update.
	// The entry.RefreshableAfter() may be returned to not modify the refresh time.
	RefreshAfterUpdate(entry Entry[K, V], oldValue V) time.Duration
	// RefreshAfterReload returns the duration after which the entry is eligible for an automatic refresh after the
	// replacement of the entry's value due to a reload.
	// The entry.RefreshableAfter() may be returned to not modify the refresh time.
	RefreshAfterReload(entry Entry[K, V], oldValue V) time.Duration
	// RefreshAfterReloadFailure returns the duration after which the entry is eligible for an automatic refresh after the
	// value failed to be reloaded.
	// The entry.RefreshableAfter() may be returned to not modify the refresh time.
	RefreshAfterReloadFailure(entry Entry[K, V], err error) time.Duration
}

type varRefreshCreating[K comparable, V any] struct {
	f func(entry Entry[K, V]) time.Duration
}

func (c *varRefreshCreating[K, V]) RefreshAfterCreate(entry Entry[K, V]) time.Duration {
	return c.f(entry)
}

func (c *varRefreshCreating[K, V]) RefreshAfterUpdate(entry Entry[K, V], oldValue V) time.Duration {
	return entry.RefreshableAfter()
}

func (c *varRefreshCreating[K, V]) RefreshAfterReload(entry Entry[K, V], oldValue V) time.Duration {
	return entry.RefreshableAfter()
}

func (c *varRefreshCreating[K, V]) RefreshAfterReloadFailure(entry Entry[K, V], err error) time.Duration {
	return entry.RefreshableAfter()
}

// RefreshCreating returns a [RefreshCalculator] that specifies that the entry should be automatically reloaded
// once the duration has elapsed after the entry's creation.
// The refresh time is not modified when the entry is updated or reloaded.
func RefreshCreating[K comparable, V any](duration time.Duration) RefreshCalculator[K, V] {
	return RefreshCreatingFunc(func(entry Entry[K, V]) time.Duration {
		return duration
	})
}

// RefreshCreatingFunc returns a [RefreshCalculator] that specifies that the entry should be automatically reloaded
// once the duration has elapsed after the entry's creation.
// The refresh time is not modified when the entry is updated or reloaded.
func RefreshCreatingFunc[K comparable, V any](f func(entry Entry[K, V]) time.Duration) RefreshCalculator[K, V] {
	return &varRefreshCreating[K, V]{
		f: f,
	}
}

type varRefreshWriting[K comparable, V any] struct {
	f func(entry Entry[K, V]) time.Duration
}

func (w *varRefreshWriting[K, V]) RefreshAfterCreate(entry Entry[K, V]) time.Duration {
	return w.f(entry)
}

func (w *varRefreshWriting[K, V]) RefreshAfterUpdate(entry Entry[K, V], oldValue V) time.Duration {
	return w.f(entry)
}

func (w *varRefreshWriting[K, V]) RefreshAfterReload(entry Entry[K, V], oldValue V) time.Duration {
	return w.f(entry)
}

func (w *varRefreshWriting[K, V]) RefreshAfterReloadFailure(entry Entry[K, V], err error) time.Duration {
	return entry.RefreshableAfter()
}

// RefreshWriting returns a [RefreshCalculator] that specifies that the entry should be automatically reloaded
// once the duration has elapsed after the entry's creation or the most recent replacement of its value.
// The refresh time is not modified when the reload fails.
func RefreshWriting[K comparable, V any](duration time.Duration) RefreshCalculator[K, V] {
	return RefreshWritingFunc(func(entry Entry[K, V]) time.Duration {
		return duration
	})
}

// RefreshWritingFunc returns a [RefreshCalculator] that specifies that the entry should be automatically reloaded
// once the duration has elapsed after the entry's creation or the most recent replacement of its value.
// The refresh time is not modified when the reload fails.
func RefreshWritingFunc[K comparable, V any](f func(entry Entry[K, V]) time.Duration) RefreshCalculator[K, V] {
	return &varRefreshWriting[K, V]{
		f: f,
	}
}
