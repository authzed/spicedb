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

// Package otter contains in-memory caching functionality.
//
// A [Cache] is similar to a hash table, but it also has additional support for policies to bound the map.
//
// [Cache] instances should always be configured and created using [Options].
//
// The [Cache] also has [Cache.Get]/[Cache.BulkGet]/[Cache.Refresh]/[Cache.Refresh] methods
// which allows the cache to populate itself on a miss and offers refresh capabilities.
//
// Additional functionality such as bounding by the entry's size, deletion notifications, statistics,
// and eviction policies are described in the [Options].
//
// See https://maypok86.github.io/otter/user-guide/v2/getting-started/ for more information about otter.
package otter
