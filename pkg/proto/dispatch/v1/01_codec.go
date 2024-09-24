// This file registers a gRPC codec that replaces the default gRPC proto codec
// with one that attempts to use protobuf codecs in the following order:
// - vtprotobuf
// - google.golang.org/encoding/proto

package dispatchv1

import (
	"fmt"

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

type vtprotoCodec struct{}

func (vtprotoCodec) Name() string { return Name }

func (vtprotoCodec) Marshal(v any) ([]byte, error) {
	if m, ok := v.(vtprotoMessage); ok {
		return m.MarshalVT()
	}

	if m, ok := v.(proto.Message); ok {
		return proto.Marshal(m)
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

	return fmt.Errorf("failed to unmarshal, message is %T, want proto.Message", v)
}

func init() {
	encoding.RegisterCodec(vtprotoCodec{})
}
