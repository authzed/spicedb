// Copyright (c) 2023 Alexey Mayshev and contributors. All rights reserved.
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
	"github.com/maypok86/otter/v2/internal/generated/node"
)

// reason represents the reason for writing the item to the cache.
type reason uint8

const (
	unknownReason reason = iota
	addReason
	deleteReason
	updateReason
)

// task is a set of information to update the cache:
// node, reason for write, difference after node weight change, etc.
type task[K comparable, V any] struct {
	n             node.Node[K, V]
	old           node.Node[K, V]
	writeReason   reason
	deletionCause DeletionCause
}

// node returns the node contained in the task. If node was not specified, it returns nil.
func (t *task[K, V]) node() node.Node[K, V] {
	return t.n
}

// oldNode returns the old node contained in the task. If old node was not specified, it returns nil.
func (t *task[K, V]) oldNode() node.Node[K, V] {
	return t.old
}
