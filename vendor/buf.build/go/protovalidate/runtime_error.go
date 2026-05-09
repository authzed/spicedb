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

import "strings"

// A RuntimeError is returned if a valid CEL expression evaluation is terminated.
// The two built-in reasons are 'no_matching_overload' when a CEL function has
// no overload for the types of the arguments or 'no_such_field' when a map or
// message does not contain the desired field.
type RuntimeError struct {
	cause error
}

func (err *RuntimeError) Error() string {
	if err == nil {
		return ""
	}
	var builder strings.Builder
	_, _ = builder.WriteString("runtime error")
	if err.cause != nil {
		_, _ = builder.WriteString(": ")
		_, _ = builder.WriteString(err.cause.Error())
	}
	return builder.String()
}

func (err *RuntimeError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.cause
}
