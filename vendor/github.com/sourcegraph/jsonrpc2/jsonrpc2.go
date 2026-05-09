// Package jsonrpc2 provides a client and server implementation of
// [JSON-RPC 2.0](http://www.jsonrpc.org/specification).
package jsonrpc2

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// JSONRPC2 describes an interface for issuing requests that speak the
// JSON-RPC 2 protocol.  It isn't really necessary for this package
// itself, but is useful for external users that use the interface as
// an API boundary.
type JSONRPC2 interface {
	// Call issues a standard request (http://www.jsonrpc.org/specification#request_object).
	Call(ctx context.Context, method string, params, result interface{}, opt ...CallOption) error

	// Notify issues a notification request (http://www.jsonrpc.org/specification#notification).
	Notify(ctx context.Context, method string, params interface{}, opt ...CallOption) error

	// Close closes the underlying connection, if it exists.
	Close() error
}

// Error represents a JSON-RPC response error.
type Error struct {
	Code    int64            `json:"code"`
	Message string           `json:"message"`
	Data    *json.RawMessage `json:"data,omitempty"`
}

// SetError sets e.Data to the JSON encoding of v. If JSON
// marshaling fails, it panics.
func (e *Error) SetError(v interface{}) {
	b, err := json.Marshal(v)
	if err != nil {
		panic("Error.SetData: " + err.Error())
	}
	e.Data = (*json.RawMessage)(&b)
}

// Error implements the Go error interface.
func (e *Error) Error() string {
	return fmt.Sprintf("jsonrpc2: code %v message: %s", e.Code, e.Message)
}

// Errors defined in the JSON-RPC spec. See
// http://www.jsonrpc.org/specification#error_object.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// Handler handles JSON-RPC requests and notifications.
type Handler interface {
	// Handle is called to handle a request. No other requests are handled
	// until it returns. If you do not require strict ordering behavior
	// of received RPCs, it is suggested to wrap your handler in
	// AsyncHandler.
	Handle(context.Context, *Conn, *Request)
}

// ID represents a JSON-RPC 2.0 request ID, which may be either a
// string or number (or null, which is unsupported).
type ID struct {
	// At most one of Num or Str may be nonzero. If both are zero
	// valued, then IsNum specifies which field's value is to be used
	// as the ID.
	Num uint64
	Str string

	// IsString controls whether the Num or Str field's value should be
	// used as the ID, when both are zero valued. It must always be
	// set to true if the request ID is a string.
	IsString bool
}

func (id ID) String() string {
	if id.IsString {
		return strconv.Quote(id.Str)
	}
	return strconv.FormatUint(id.Num, 10)
}

// MarshalJSON implements json.Marshaler.
func (id ID) MarshalJSON() ([]byte, error) {
	if id.IsString {
		return json.Marshal(id.Str)
	}
	return json.Marshal(id.Num)
}

// UnmarshalJSON implements json.Unmarshaler.
func (id *ID) UnmarshalJSON(data []byte) error {
	// Support both uint64 and string IDs.
	var v uint64
	if err := json.Unmarshal(data, &v); err == nil {
		*id = ID{Num: v}
		return nil
	}
	var v2 string
	if err := json.Unmarshal(data, &v2); err != nil {
		return err
	}
	*id = ID{Str: v2, IsString: true}
	return nil
}

// ErrClosed indicates that the JSON-RPC connection is closed (or in
// the process of closing).
var ErrClosed = errors.New("jsonrpc2: connection is closed")

var jsonNull = json.RawMessage("null")
