// Copyright 2023 Mostyn Bramley-Moore.
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

// Package s2 is an experimental wrapper for using
// github.com/klauspost/compress/s2 stream compression with gRPC.

// Package github.com/mostynb/go-grpc-compression/s2 is an experimental
// wrapper for using github.com/klauspost/compress/s2 stream compression
// with gRPC.
//
// If you import this package, it will register itself as the encoder for
// the "s2" compressor, overriding any previously registered compressors
// with this name.
//
// If you don't want to override previously registered "s2" compressors,
// then you should instead import
// github.com/mostynb/go-grpc-compression/nonclobbering/s2
package s2

import (
	internals2 "github.com/mostynb/go-grpc-compression/internal/s2"
)

const Name = internals2.Name

func init() {
	clobbering := true
	internals2.PretendInit(clobbering)
}
