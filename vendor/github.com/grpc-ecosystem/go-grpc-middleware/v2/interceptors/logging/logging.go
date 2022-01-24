// Copyright (c) The go-grpc-middleware Authors.
// Licensed under the Apache License 2.0.

package logging

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors"
)

// Decision defines rules for enabling start and end of logging.
type Decision int

const (
	// NoLogCall - Logging is disabled.
	NoLogCall Decision = iota
	// LogFinishCall - Only finish logs of request is enabled.
	LogFinishCall
	// LogStartAndFinishCall - Logging of start and end of request is enabled.
	LogStartAndFinishCall
)

var (
	// SystemTag is tag representing an event inside gRPC call.
	SystemTag = []string{"protocol", "grpc"}
	// ComponentFieldKey is a tag representing the client/server that is calling.
	ComponentFieldKey    = "grpc.component"
	KindServerFieldValue = "server"
	KindClientFieldValue = "client"
	ServiceFieldKey      = "grpc.service"
	MethodFieldKey       = "grpc.method"
	MethodTypeFieldKey   = "grpc.method_type"
)

type fieldsCtxMarker struct{}

var (
	// fieldsCtxMarkerKey is the Context value marker that is used by logging middleware to read and write logging fields into context.
	fieldsCtxMarkerKey = &fieldsCtxMarker{}
)

// Fields represents logging fields. It has to have even number of elements (pairs).
type Fields []string

func newCommonFields(kind string, c interceptors.CallMeta) Fields {
	return Fields{
		SystemTag[0], SystemTag[1],
		ComponentFieldKey, kind,
		ServiceFieldKey, c.Service,
		MethodFieldKey, c.Method,
		MethodTypeFieldKey, string(c.Typ),
	}
}

// Iter returns FieldsIterator.
func (f Fields) Iter() FieldsIterator {
	// We start from -2 as we iterate over two items per iteration and first iteration will advance iterator to 0.
	return &iter{i: -2, f: f}
}

// FieldsIterator is an interface allowing to iterate over fields.
type FieldsIterator interface {
	Next() bool
	At() (k, v string)
}

type iter struct {
	f Fields
	i int
}

func (i *iter) Next() bool {
	if i.i >= len(i.f) {
		return false
	}

	i.i += 2
	return i.i < len(i.f)
}

func (i *iter) At() (k, v string) {
	if i.i < 0 || i.i >= len(i.f) {
		return "", ""
	}

	if i.i+1 == len(i.f) {
		// Non even number of elements, add empty string.
		return i.f[i.i], ""
	}
	return i.f[i.i], i.f[i.i+1]
}

// AppendUnique returns fields which is the union of all keys. Any keys that already exist in the log fields will take precedence over duplicates in add.
func (f Fields) AppendUnique(add Fields) Fields {
	if len(add) == 0 {
		return f
	}

	existing := map[string]struct{}{}
	i := f.Iter()
	for i.Next() {
		k, _ := i.At()
		existing[k] = struct{}{}
	}

	n := make(Fields, len(f), len(f)+len(add))
	copy(n, f)

	a := add.Iter()
	for a.Next() {
		k, v := a.At()
		if _, ok := existing[k]; ok {
			continue
		}
		n = append(n, k, v)
	}
	return n
}

// PayloadDecision defines rules for enabling payload logging of request and responses.
type PayloadDecision int

const (
	// NoPayloadLogging - Payload logging is disabled.
	NoPayloadLogging PayloadDecision = iota
	// LogPayloadRequest - Only logging of requests is enabled.
	LogPayloadRequest
	// LogPayloadResponse - Only logging of responses is enabled.
	LogPayloadResponse
	// LogPayloadRequestAndResponse - Logging of both requests and responses is enabled.
	LogPayloadRequestAndResponse
)

// ServerPayloadLoggingDecider is a user-provided function for deciding whether to log the server-side
// request/response payloads
type ServerPayloadLoggingDecider func(ctx context.Context, fullMethodName string, servingObject interface{}) PayloadDecision

// ClientPayloadLoggingDecider is a user-provided function for deciding whether to log the client-side
// request/response payloads
type ClientPayloadLoggingDecider func(ctx context.Context, fullMethodName string) PayloadDecision

// ExtractFields returns logging.Fields object from the Context.
// Logging interceptor adds fields into context when used.
// If there are no fields in the context, returns an empty Fields value.
//
// It's useful for server implementations to use this method to instantiate request logger for consistent fields (e.g request-id/tracing-id).
func ExtractFields(ctx context.Context) Fields {
	t, ok := ctx.Value(fieldsCtxMarkerKey).(Fields)
	if !ok {
		return Fields{}
	}
	n := make(Fields, len(t))
	copy(n, t)
	return n
}

// InjectFields allows adding Fields to any existing Fields that will be used by the logging interceptor.
func InjectFields(ctx context.Context, f Fields) context.Context {
	return context.WithValue(ctx, fieldsCtxMarkerKey, ExtractFields(ctx).AppendUnique(f))
}

// JsonPBMarshaler is a marshaler that serializes protobuf messages.
type JsonPBMarshaler interface {
	Marshal(pb proto.Message) ([]byte, error)
}

// Logger is unified interface that we used for all our interceptors. Official implementations are available under
// provider/ directory as separate modules.
type Logger interface {
	// Log logs the fields for given log level. We can assume users (middleware library) will put fields in pairs and
	// those will be unique.
	Log(Level, string)
	// With returns Logger with given fields appended. We can assume users (middleware library) will put fields in pairs
	// and those will be unique.
	With(fields ...string) Logger
}

// Level represents logging level.
type Level string

const (
	DEBUG   = Level("debug")
	INFO    = Level("info")
	WARNING = Level("warning")
	ERROR   = Level("error")
)
