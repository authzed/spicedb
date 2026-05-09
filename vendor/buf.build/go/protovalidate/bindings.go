// Copyright 2023-2025 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package protovalidate

import (
	"sync"

	"github.com/google/cel-go/interpreter"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// bindings implements interpreter.Activation, providing a lightweight named
// bindings to cel.Program executions.
type bindings struct {
	This  optional[any]
	Rules protoreflect.ProtoMessage
	Rule  any
	NowFn func() *timestamppb.Timestamp
	ts    *timestamppb.Timestamp
}

// ResolveName implements interpreter.Activation.
func (p *bindings) ResolveName(name string) (any, bool) {
	switch name {
	case "this":
		return p.This.Val, p.This.Set
	case "rules":
		return p.Rules, p.Rules != nil
	case "rule":
		return p.Rule, p.Rule != nil
	case "now":
		if p.NowFn != nil {
			if p.ts == nil {
				p.ts = p.NowFn()
			}
			return p.ts, true
		}
	}
	return nil, false
}

// Parent implements interpreter.Activation.
func (p *bindings) Parent() interpreter.Activation { return nil }

//nolint:gochecknoglobals
var bindingsPool = &sync.Pool{
	New: func() any { return &bindings{} },
}

func getBindings() *bindings {
	return bindingsPool.Get().(*bindings) //nolint:errcheck,forcetypeassert
}

func putBindings(binds *bindings) {
	*binds = bindings{}
	bindingsPool.Put(binds)
}

type optional[T any] struct {
	Val T
	Set bool
}

func newOptional[T any](v T) optional[T] {
	return optional[T]{Val: v, Set: true}
}
