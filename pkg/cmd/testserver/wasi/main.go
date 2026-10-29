//go:build wasip1

// Package main builds the SpiceDB testing server as a WASI reactor module, for use by test
// doubles in client libraries. See README.md for the ABI.
package main

import (
	"context"
	"encoding/binary"
	"unsafe"

	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	log "github.com/authzed/spicedb/internal/logging"
	"github.com/authzed/spicedb/pkg/cmd/testserver/inprocess"
)

// The first byte of every result buffer is one of these.
const (
	resultOK    byte = 0
	resultError byte = 1
)

var (
	server     *inprocess.Server
	sessions   = map[uint32]*inprocess.Session{}
	nextHandle uint32

	// buffers keeps every buffer handed to the host reachable until the host frees it.
	buffers = map[uint32][]byte{}
)

func init() {
	log.SetGlobalLogger(zerolog.Nop())
}

// alloc returns a buffer of the given size for the host to write an argument into. The host
// must release it with free.
//
//go:wasmexport alloc
func alloc(size uint32) uint32 {
	return keep(make([]byte, size))
}

// free releases a buffer returned by alloc or contained in a packed result.
//
//go:wasmexport free
func free(ptr uint32) {
	delete(buffers, ptr)
}

// session_new creates a session with its own empty datastore. If schema is non-empty, it is
// written with WriteSchema, and the session is discarded if that fails. On success, the result
// payload is the session handle, as a little-endian uint32.
//
//go:wasmexport session_new
func sessionNew(schemaPtr, schemaLen uint32) uint64 {
	if server == nil {
		s, err := inprocess.NewServer(nil)
		if err != nil {
			return errorResult(err)
		}
		server = s
	}

	session := server.NewSession()
	if schemaLen > 0 {
		req, err := (&v1.WriteSchemaRequest{Schema: string(hostBytes(schemaPtr, schemaLen))}).MarshalVT()
		if err != nil {
			return errorResult(err)
		}
		if _, err := session.Call(context.Background(), "/authzed.api.v1.SchemaService/WriteSchema", req); err != nil {
			_ = session.Close()
			return errorResult(err)
		}
	}

	nextHandle++
	sessions[nextHandle] = session
	return okResult(binary.LittleEndian.AppendUint32(nil, nextHandle))
}

// session_call invokes an API method, named by its full gRPC method name, against a session.
// The result payload is the serialized response message, or a framed sequence of messages for
// server-streaming methods. Client-streaming methods take a framed sequence of request messages.
//
//go:wasmexport session_call
func sessionCall(handle, methodPtr, methodLen, reqPtr, reqLen uint32) uint64 {
	session, ok := sessions[handle]
	if !ok {
		return errorResult(status.Errorf(codes.InvalidArgument, "unknown session handle %d", handle))
	}

	method := string(hostBytes(methodPtr, methodLen))
	resp, err := session.Call(context.Background(), method, hostBytes(reqPtr, reqLen))
	if err != nil {
		return errorResult(err)
	}
	return okResult(resp)
}

// session_free discards a session and its datastore.
//
//go:wasmexport session_free
func sessionFree(handle uint32) {
	session, ok := sessions[handle]
	if !ok {
		return
	}
	delete(sessions, handle)
	_ = session.Close()
}

// hostBytes returns the bytes the host wrote into a buffer returned by alloc.
func hostBytes(ptr, length uint32) []byte {
	if length == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), length) //nolint:govet // ptr is a buffer from alloc, kept reachable in buffers
}

func keep(buf []byte) uint32 {
	if len(buf) == 0 {
		buf = make([]byte, 1)
	}
	ptr := uint32(uintptr(unsafe.Pointer(&buf[0]))) //nolint:gosec // wasm32 pointers are 32-bit
	buffers[ptr] = buf
	return ptr
}

// okResult and errorResult return a buffer holding a status byte and a payload, packed as
// (ptr << 32 | len). The host must free the buffer.
func okResult(payload []byte) uint64 {
	return packResult(append([]byte{resultOK}, payload...))
}

func errorResult(err error) uint64 {
	payload, marshalErr := proto.Marshal(status.Convert(err).Proto())
	if marshalErr != nil {
		payload, _ = proto.Marshal(status.New(codes.Internal, marshalErr.Error()).Proto())
	}
	return packResult(append([]byte{resultError}, payload...))
}

func packResult(buf []byte) uint64 {
	return uint64(keep(buf))<<32 | uint64(len(buf))
}
