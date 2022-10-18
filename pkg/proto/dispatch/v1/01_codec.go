// This file registers a gRPC codec that replaces the default gRPC proto codec
// with one that attempts to use protobuf codecs in the following order:
// - PreMarshaler
// - vtprotobuf
// - google.golang.org/encoding/proto
// - github.com/golang/protobuf/proto

package dispatchv1

import (
	"fmt"

	protolegacy "github.com/golang/protobuf/proto" //nolint
	"google.golang.org/grpc/encoding"
	"google.golang.org/protobuf/proto"

	// Guarantee that the built-in proto is called registered before this one
	// so that it can be replaced.
	_ "google.golang.org/grpc/encoding/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

type preMarshaler interface {
	PreMarshaledBytes() []byte
}

type vtprotoCodec struct{}

func (vtprotoCodec) Name() string { return Name }

func (vtprotoCodec) Marshal(v any) ([]byte, error) {
	if m, ok := v.(preMarshaler); ok {
		bytes := m.PreMarshaledBytes()
		if len(bytes) > 0 {
			return bytes, nil
		}
	}

	if m, ok := v.(vtprotoMessage); ok {
		return m.MarshalVT()
	}

	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
	}

	if m, ok := v.(protolegacy.Message); ok {
		return protolegacy.Marshal(m)
	}

	return nil, fmt.Errorf("failed to marshal, message is %T, want proto.Message", v)
}

func (vtprotoCodec) Unmarshal(data []byte, v any) error {
	if m, ok := v.(vtprotoMessage); ok {
		return m.UnmarshalVT(data)
	}

	if m, ok := v.(proto.Message); ok {
		return proto.Unmarshal(data, m)
	}

	if m, ok := v.(protolegacy.Message); ok {
		return protolegacy.Unmarshal(data, m)
	}

	return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
}

func init() { encoding.RegisterCodec(vtprotoCodec{}) }

// Implement the preMarshaler interface

func (r *DispatchCheckResponse) PreMarshaledBytes() []byte              { return r.Marshaled }
func (r *DispatchExpandResponse) PreMarshaledBytes() []byte             { return r.Marshaled }
func (r *DispatchReachableResourcesResponse) PreMarshaledBytes() []byte { return r.Marshaled }
func (r *DispatchLookupResponse) PreMarshaledBytes() []byte             { return r.Marshaled }
func (r *DispatchLookupSubjectsResponse) PreMarshaledBytes() []byte     { return r.Marshaled }
