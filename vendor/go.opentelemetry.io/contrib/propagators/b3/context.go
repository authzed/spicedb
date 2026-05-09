// Copyright The OpenTelemetry Authors
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

package b3 // import "go.opentelemetry.io/contrib/propagators/b3"

import "context"

type b3KeyType int

const (
	debugKey b3KeyType = iota
	deferredKey
)

// withDebug returns a copy of parent with debug set as the debug flag value .
func withDebug(parent context.Context, debug bool) context.Context {
	return context.WithValue(parent, debugKey, debug)
}

// debugFromContext returns the debug value stored in ctx.
//
// If no debug value is stored in ctx false is returned.
func debugFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if debug, ok := ctx.Value(debugKey).(bool); ok {
		return debug
	}
	return false
}

// withDeferred returns a copy of parent with deferred set as the deferred flag value .
func withDeferred(parent context.Context, deferred bool) context.Context {
	return context.WithValue(parent, deferredKey, deferred)
}

// deferredFromContext returns the deferred value stored in ctx.
//
// If no deferred value is stored in ctx false is returned.
func deferredFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	if deferred, ok := ctx.Value(deferredKey).(bool); ok {
		return deferred
	}
	return false
}
