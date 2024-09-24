package lsp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/sourcegraph/go-lsp"
	"github.com/sourcegraph/jsonrpc2"
	"github.com/stretchr/testify/require"
)

type testStream struct {
	toRead   chan string
	received chan string
}

var _ io.ReadWriteCloser = (*testStream)(nil)

func (ts *testStream) Read(p []byte) (int, error) {
	read := <-ts.toRead
	if len(read) == 0 {
		return 0, io.EOF
	}

	copy(p, read)
	return len(read), nil
}

func (ts *testStream) Write(p []byte) (int, error) {
	ts.received <- string(p)
	return len(p), nil
}

func (ts *testStream) Close() error {
	return nil
}

type lspTester struct {
	t      *testing.T
	ts     *testStream
	server *Server
}

func (lt *lspTester) initialize() {
	resp, serverState := sendAndReceive[lsp.InitializeResult](lt, "initialize", InitializeParams{
		Capabilities: ClientCapabilities{
			Diagnostics: DiagnosticWorkspaceClientCapabilities{
				RefreshSupport: true,
			},
		},
	})
	require.Equal(lt.t, serverStateInitialized, serverState)
	require.True(lt.t, resp.Capabilities.DocumentFormattingProvider)
}

func (lt *lspTester) setFileContents(path string, contents string) {
	sendAndReceive[any](lt, "textDocument/didChange", lsp.DidChangeTextDocumentParams{
		TextDocument: lsp.VersionedTextDocumentIdentifier{
			TextDocumentIdentifier: lsp.TextDocumentIdentifier{URI: lsp.DocumentURI(path)},
			Version:                1,
		},
		ContentChanges: []lsp.TextDocumentContentChangeEvent{
			{
				Text: contents,
			},
		},
	})
}

func sendAndExpectError(lt *lspTester, method string, params interface{}) (*jsonrpc2.Error, serverState) {
	paramsBytes, err := json.Marshal(params)
	require.NoError(lt.t, err)

	paramsMsg := json.RawMessage(paramsBytes)

	r := &jsonrpc2.Request{
		Method: method,
		ID:     jsonrpc2.ID{Num: 1},
		Params: &paramsMsg,
	}
	message, err := r.MarshalJSON()
	require.NoError(lt.t, err)

	lt.ts.toRead <- fmt.Sprintf("Content-Length: %d\r\n", len(message))
	lt.ts.toRead <- "\r\n"
	lt.ts.toRead <- string(message)

	select {
	case received := <-lt.ts.received:
		lines := strings.Split(received, "\r\n")
		require.Greater(lt.t, len(lines), 2)

		var resp jsonrpc2.Response
		err := json.Unmarshal([]byte(lines[2]), &resp)
		require.NoError(lt.t, err)
		require.NotNil(lt.t, resp.Error)

		return resp.Error, lt.server.state

	case <-time.After(1 * time.Second):
		lt.t.Fatal("timed out waiting for response")
	}

	return nil, serverStateNotInitialized
}

func sendAndReceive[T any](lt *lspTester, method string, params interface{}) (T, serverState) {
	paramsBytes, err := json.Marshal(params)
	require.NoError(lt.t, err)

	paramsMsg := json.RawMessage(paramsBytes)

	r := &jsonrpc2.Request{
		Method: method,
		ID:     jsonrpc2.ID{Num: 1},
		Params: &paramsMsg,
	}
	message, err := r.MarshalJSON()
	require.NoError(lt.t, err)

	lt.ts.toRead <- fmt.Sprintf("Content-Length: %d\r\n", len(message))
	lt.ts.toRead <- "\r\n"
	lt.ts.toRead <- string(message)

	select {
	case received := <-lt.ts.received:
		lines := strings.Split(received, "\r\n")
		require.Greater(lt.t, len(lines), 2)

		var resp jsonrpc2.Response
		err := json.Unmarshal([]byte(lines[2]), &resp)
		require.NoError(lt.t, err)
		require.Nil(lt.t, resp.Error)

		var result T
		err = json.Unmarshal(*resp.Result, &result)
		require.NoError(lt.t, err)

		return result, lt.server.state

	case <-time.After(1 * time.Second):
		lt.t.Fatal("timed out waiting for response")
	}

	var empty T
	return empty, serverStateNotInitialized
}

func newLSPTester(t *testing.T) *lspTester {
	ctx := context.Background()
	ts := newTestStream()

	var connOpts []jsonrpc2.ConnOpt
	stream := jsonrpc2.NewBufferedStream(ts, jsonrpc2.VSCodeObjectCodec{})

	server := NewServer()
	conn := jsonrpc2.NewConn(ctx, stream, server, connOpts...)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	return &lspTester{t, ts, server}
}

func newTestStream() *testStream {
	return &testStream{make(chan string), make(chan string)}
}
