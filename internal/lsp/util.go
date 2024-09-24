package lsp

import (
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/sourcegraph/jsonrpc2"
)

const (
	codeUninitialized int64 = 32002
)

func unmarshalParams[T any](r *jsonrpc2.Request) (T, error) {
	var params T
	if r.Params == nil {
		return params, invalidParams(errors.New("params not provided"))
	}
	if err := json.Unmarshal(*r.Params, &params); err != nil {
		return params, invalidParams(err)
	}
	return params, nil
}

func invalidParams(err error) *jsonrpc2.Error {
	return &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidParams,
		Message: err.Error(),
	}
}

func invalidRequest(err error) *jsonrpc2.Error {
	return &jsonrpc2.Error{
		Code:    jsonrpc2.CodeInvalidRequest,
		Message: err.Error(),
	}
}

type stdrwc struct{}

var _ io.ReadWriteCloser = (*stdrwc)(nil)

func (stdrwc) Read(p []byte) (int, error)  { return os.Stdin.Read(p) }
func (stdrwc) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stdrwc) Close() error {
	if err := os.Stdin.Close(); err != nil {
		return err
	}
	return os.Stdout.Close()
}
