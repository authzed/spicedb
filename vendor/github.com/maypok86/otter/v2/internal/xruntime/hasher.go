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

package xruntime

import "hash/maphash"

type Hasher[T comparable] struct {
	seed maphash.Seed
}

func NewHasher[T comparable]() Hasher[T] {
	return Hasher[T]{
		seed: maphash.MakeSeed(),
	}
}

func (h Hasher[T]) Hash(t T) uint64 {
	return maphash.Comparable(h.seed, t)
}
