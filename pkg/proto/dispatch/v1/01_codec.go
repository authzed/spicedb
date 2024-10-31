// This file registers a gRPC codec that replaces the default gRPC proto codec
// with one that attempts to use protobuf codecs in the following order:
// - vtprotobuf
// - google.golang.org/encoding/proto

package dispatchv1

import (
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"

	// Guarantee that the built-in proto is called registered before this one
	// so that it can be replaced.
	_ "google.golang.org/grpc/encoding/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type vtprotoMessage interface {
	MarshalToSizedBufferVT(data []byte) (int, error)
	UnmarshalVT([]byte) error
	SizeVT() int
}

type vtprotoCodec struct {
	fallback encoding.CodecV2
}

func (vtprotoCodec) Name() string { return Name }

func (c *vtprotoCodec) Marshal(v any) (mem.BufferSlice, error) {
	if m, ok := v.(vtprotoMessage); ok {
		size := m.SizeVT()
		if mem.IsBelowBufferPoolingThreshold(size) {
			buf := make([]byte, size)
			n, err := m.MarshalToSizedBufferVT(buf)
			if err != nil {
				return nil, err
			}
			return mem.BufferSlice{mem.SliceBuffer(buf[:n])}, nil
		}
		pool := mem.DefaultBufferPool()
		buf := pool.Get(size)
		n, err := m.MarshalToSizedBufferVT(*buf)
		if err != nil {
			pool.Put(buf)
			return nil, err
		}
		*buf = (*buf)[:n]
		return mem.BufferSlice{mem.NewBuffer(buf, pool)}, nil
	}

	return c.fallback.Marshal(v)
}

func (c *vtprotoCodec) Unmarshal(data mem.BufferSlice, v any) error {
	if m, ok := v.(vtprotoMessage); ok {
		buf := data.MaterializeToBuffer(mem.DefaultBufferPool())
		defer buf.Free()
		return m.UnmarshalVT(buf.ReadOnlyData())
	}

	return c.fallback.Unmarshal(data, v)
}

func init() {
	encoding.RegisterCodecV2(&vtprotoCodec{
		fallback: encoding.GetCodecV2("proto"),
	})
}
