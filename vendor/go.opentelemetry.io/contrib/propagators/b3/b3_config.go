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

type config struct {
	// InjectEncoding are the B3 encodings used when injecting trace
	// information. If no encoding is specified (i.e. `B3Unspecified`)
	// `B3SingleHeader` will be used as the default.
	InjectEncoding Encoding
}

// Option interface used for setting optional config properties.
type Option interface {
	apply(*config)
}

type optionFunc func(*config)

func (o optionFunc) apply(c *config) {
	o(c)
}

// newConfig creates a new config struct and applies opts to it.
func newConfig(opts ...Option) *config {
	c := &config{}
	for _, opt := range opts {
		opt.apply(c)
	}
	return c
}

// Encoding is a bitmask representation of the B3 encoding type.
type Encoding uint8

// supports returns if e has o bit(s) set.
func (e Encoding) supports(o Encoding) bool {
	return e&o == o
}

const (
	// B3Unspecified is an unspecified B3 encoding.
	B3Unspecified Encoding = 0
	// B3MultipleHeader is a B3 encoding that uses multiple headers to
	// transmit tracing information all prefixed with `x-b3-`.
	//    x-b3-traceid: {TraceId}
	//    x-b3-parentspanid: {ParentSpanId}
	//    x-b3-spanid: {SpanId}
	//    x-b3-sampled: {SamplingState}
	//    x-b3-flags: {DebugFlag}
	B3MultipleHeader Encoding = 1 << iota
	// B3SingleHeader is a B3 encoding that uses a single header named `b3`
	// to transmit tracing information.
	//    b3: {TraceId}-{SpanId}-{SamplingState}-{ParentSpanId}
	B3SingleHeader
)

// WithInjectEncoding sets the encoding the propagator will inject.
// The encoding is interpreted as a bitmask. Therefore
//
//	WithInjectEncoding(B3SingleHeader | B3MultipleHeader)
//
// means the propagator will inject both single and multi B3 headers.
func WithInjectEncoding(encoding Encoding) Option {
	return optionFunc(func(c *config) {
		c.InjectEncoding = encoding
	})
}
