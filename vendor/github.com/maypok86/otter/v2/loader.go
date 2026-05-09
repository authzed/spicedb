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

import "context"

// Loader computes or retrieves values, based on a key, for use in populating a [Cache].
type Loader[K comparable, V any] interface {
	// Load computes or retrieves the value corresponding to key.
	//
	// WARNING: loading must not attempt to update any mappings of this cache directly.
	//
	// NOTE: The Loader implementation should always return ErrNotFound
	// if the entry was not found in the data source.
	Load(ctx context.Context, key K) (V, error)
	// Reload computes or retrieves a replacement value corresponding to an already-cached key.
	// If the replacement value is not found, then the mapping will be removed if ErrNotFound is returned.
	// This method is called when an existing cache entry is refreshed by Cache.Get, or through a call to Cache.Refresh.
	//
	// WARNING: loading must not attempt to update any mappings of this cache directly
	// or block waiting for other cache operations to complete.
	//
	// NOTE: all errors returned by this method will be logged (using Logger) and then swallowed.
	//
	// NOTE: The Loader implementation should always return ErrNotFound
	// if the entry was not found in the data source.
	Reload(ctx context.Context, key K, oldValue V) (V, error)
}

// LoaderFunc is an adapter to allow the use of ordinary functions as loaders.
// If f is a function with the appropriate signature, LoaderFunc(f) is a [Loader] that calls f.
type LoaderFunc[K comparable, V any] func(ctx context.Context, key K) (V, error)

// Load calls f(ctx, key).
func (lf LoaderFunc[K, V]) Load(ctx context.Context, key K) (V, error) {
	return lf(ctx, key)
}

// Reload calls f(ctx, key).
func (lf LoaderFunc[K, V]) Reload(ctx context.Context, key K, oldValue V) (V, error) {
	return lf(ctx, key)
}

// BulkLoader computes or retrieves values, based on the keys, for use in populating a [Cache].
type BulkLoader[K comparable, V any] interface {
	// BulkLoad computes or retrieves the values corresponding to keys.
	// This method is called by Cache.BulkGet.
	//
	// If the returned map doesn't contain all requested keys, then the entries it does
	// contain will be cached, and Cache.BulkGet will return the partial results. If the returned map
	// contains extra keys not present in keys then all returned entries will be cached, but
	// only the entries for keys, will be returned from Cache.BulkGet.
	//
	// WARNING: loading must not attempt to update any mappings of this cache directly.
	BulkLoad(ctx context.Context, keys []K) (map[K]V, error)
	// BulkReload computes or retrieves replacement values corresponding to already-cached keys.
	// If the replacement value is not found, then the mapping will be removed.
	// This method is called when an existing cache entry is refreshed by Cache.BulkGet, or through a call to Cache.BulkRefresh.
	//
	// If the returned map doesn't contain all requested keys, then the entries it does
	// contain will be cached. If the returned map
	// contains extra keys not present in keys then all returned entries will be cached.
	//
	// WARNING: loading must not attempt to update any mappings of this cache directly
	// or block waiting for other cache operations to complete.
	//
	// NOTE: all errors returned by this method will be logged (using Logger) and then swallowed.
	BulkReload(ctx context.Context, keys []K, oldValues []V) (map[K]V, error)
}

// BulkLoaderFunc is an adapter to allow the use of ordinary functions as loaders.
// If f is a function with the appropriate signature, BulkLoaderFunc(f) is a [BulkLoader] that calls f.
type BulkLoaderFunc[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// BulkLoad calls f(ctx, keys).
func (blf BulkLoaderFunc[K, V]) BulkLoad(ctx context.Context, keys []K) (map[K]V, error) {
	return blf(ctx, keys)
}

// BulkReload calls f(ctx, keys).
func (blf BulkLoaderFunc[K, V]) BulkReload(ctx context.Context, keys []K, oldValues []V) (map[K]V, error) {
	return blf(ctx, keys)
}

// RefreshResult holds the results of [Cache.Refresh]/[Cache.BulkRefresh], so they can be passed
// on a channel.
type RefreshResult[K comparable, V any] struct {
	// Key is the key corresponding to the refreshed entry.
	Key K
	// Value is the value corresponding to the refreshed entry.
	Value V
	// Err is the error that Loader / BulkLoader returned.
	Err error
}
