package jsonrpc2

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
)

// Conn is a JSON-RPC client/server connection. The JSON-RPC protocol
// is symmetric, so a Conn runs on both ends of a client-server
// connection.
type Conn struct {
	stream ObjectStream

	h Handler

	mu      sync.Mutex
	closed  bool
	seq     uint64
	pending map[ID]*call

	sending sync.Mutex

	disconnect chan struct{}

	logger Logger

	// Set by ConnOpt funcs.
	onRecv []func(*Request, *Response)
	onSend []func(*Request, *Response)
}

var _ JSONRPC2 = (*Conn)(nil)

// NewConn creates a new JSON-RPC client/server connection using the
// given ReadWriteCloser (typically a TCP connection or stdio). The
// JSON-RPC protocol is symmetric, so a Conn runs on both ends of a
// client-server connection.
//
// NewClient consumes conn, so you should call Close on the returned
// client not on the given conn.
func NewConn(ctx context.Context, stream ObjectStream, h Handler, opts ...ConnOpt) *Conn {
	c := &Conn{
		stream:     stream,
		h:          h,
		pending:    map[ID]*call{},
		disconnect: make(chan struct{}),
		logger:     log.New(os.Stderr, "", log.LstdFlags),
	}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt(c)
	}
	go c.readMessages(ctx)
	return c
}

// Close closes the JSON-RPC connection. The connection may not be
// used after it has been closed.
func (c *Conn) Close() error {
	return c.close(nil)
}

// Call initiates a JSON-RPC call using the specified method and params, and
// waits for the response. If the response is successful, its result is stored
// in result (a pointer to a value that can be JSON-unmarshaled into);
// otherwise, a non-nil error is returned. See DispatchCall for more details.
func (c *Conn) Call(ctx context.Context, method string, params, result interface{}, opts ...CallOption) error {
	call, err := c.DispatchCall(ctx, method, params, opts...)
	if err != nil {
		return err
	}
	return call.Wait(ctx, result)
}

// DisconnectNotify returns a channel that is closed when the
// underlying connection is disconnected.
func (c *Conn) DisconnectNotify() <-chan struct{} {
	return c.disconnect
}

// DispatchCall dispatches a JSON-RPC call using the specified method and
// params, and returns a call proxy or an error. Call Wait() on the returned
// proxy to receive the response. Only use this function if you need to do work
// after dispatching the request, otherwise use Call.
//
// The params member is omitted from the JSON-RPC request if the given params is
// nil. Use json.RawMessage("null") to send a JSON-RPC request with its params
// member set to null.
func (c *Conn) DispatchCall(ctx context.Context, method string, params interface{}, opts ...CallOption) (Waiter, error) {
	req := &Request{Method: method}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.apply(req); err != nil {
			return Waiter{}, err
		}
	}
	if params != nil {
		if err := req.SetParams(params); err != nil {
			return Waiter{}, err
		}
	}
	call, err := c.send(ctx, &anyMessage{request: req}, true)
	if err != nil {
		return Waiter{}, err
	}
	return Waiter{call: call}, nil
}

// Notify is like Call, but it returns when the notification request is sent
// (without waiting for a response, because JSON-RPC notifications do not have
// responses).
//
// The params member is omitted from the JSON-RPC request if the given params is
// nil. Use json.RawMessage("null") to send a JSON-RPC request with its params
// member set to null.
func (c *Conn) Notify(ctx context.Context, method string, params interface{}, opts ...CallOption) error {
	req := &Request{Method: method, Notif: true}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt.apply(req); err != nil {
			return err
		}
	}
	if params != nil {
		if err := req.SetParams(params); err != nil {
			return err
		}
	}
	_, err := c.send(ctx, &anyMessage{request: req}, false)
	return err
}

// Reply sends a successful response with a result.
func (c *Conn) Reply(ctx context.Context, id ID, result interface{}) error {
	resp := &Response{ID: id}
	if err := resp.SetResult(result); err != nil {
		return err
	}
	_, err := c.send(ctx, &anyMessage{response: resp}, false)
	return err
}

// ReplyWithError sends a response with an error.
func (c *Conn) ReplyWithError(ctx context.Context, id ID, respErr *Error) error {
	_, err := c.send(ctx, &anyMessage{response: &Response{ID: id, Error: respErr}}, false)
	return err
}

// SendResponse sends resp to the peer. It is lower level than (*Conn).Reply.
func (c *Conn) SendResponse(ctx context.Context, resp *Response) error {
	_, err := c.send(ctx, &anyMessage{response: resp}, false)
	return err
}

func (c *Conn) close(cause error) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return ErrClosed
	}

	for _, call := range c.pending {
		close(call.done)
	}

	if cause != nil && cause != io.EOF && cause != io.ErrUnexpectedEOF {
		c.logger.Printf("jsonrpc2: protocol error: %v\n", cause)
	}

	close(c.disconnect)
	c.closed = true
	return c.stream.Close()
}

func (c *Conn) readMessages(ctx context.Context) {
	for {
		var m anyMessage
		err := c.stream.ReadObject(&m)
		if err != nil {
			c.close(err)
			return
		}

		switch {
		// TODO: handle the case where both request and response are nil.

		case m.request != nil:
			for _, onRecv := range c.onRecv {
				onRecv(m.request, nil)
			}
			c.h.Handle(ctx, c, m.request)

		case m.response != nil:
			resp := m.response
			id := resp.ID
			c.mu.Lock()
			call := c.pending[id]
			delete(c.pending, id)
			c.mu.Unlock()

			var req *Request
			if call != nil {
				call.response = resp
				req = call.request
			}

			for _, onRecv := range c.onRecv {
				onRecv(req, resp)
			}

			if call == nil {
				c.logger.Printf("jsonrpc2: ignoring response #%s with no corresponding request\n", id)
				continue
			}

			var err error
			if resp.Error != nil {
				err = resp.Error
			}

			call.done <- err
			close(call.done)
		}
	}
}

func (c *Conn) send(_ context.Context, m *anyMessage, wait bool) (cc *call, err error) {
	c.sending.Lock()
	defer c.sending.Unlock()

	// double check the error isn't due to being closed while sending.
	defer func() {
		if err != nil {
			c.mu.Lock()
			if c.closed {
				err = ErrClosed
			}
			c.mu.Unlock()
		}
	}()

	// m.request.ID could be changed, so we store a copy to correctly
	// clean up pending
	var id ID

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}

	// Assign a default id if not set
	if m.request != nil && wait {
		cc = &call{request: m.request, seq: c.seq, done: make(chan error, 1)}

		isIDUnset := len(m.request.ID.Str) == 0 && m.request.ID.Num == 0
		if isIDUnset {
			if m.request.ID.IsString {
				m.request.ID.Str = strconv.FormatUint(c.seq, 10)
			} else {
				m.request.ID.Num = c.seq
			}
		}
		c.seq++
	}
	c.mu.Unlock()

	if len(c.onSend) > 0 {
		var (
			req  *Request
			resp *Response
		)
		switch {
		case m.request != nil:
			req = m.request
		case m.response != nil:
			resp = m.response
		}
		for _, onSend := range c.onSend {
			onSend(req, resp)
		}
	}

	// Store requests so we can later associate them with incoming
	// responses.
	if m.request != nil && wait {
		c.mu.Lock()
		id = m.request.ID
		c.pending[id] = cc
		c.mu.Unlock()
	}

	// From here on, if we fail to send this, then we need to remove
	// this from the pending map so we don't block on it or pile up
	// pending entries for unsent messages.
	defer func() {
		if err != nil {
			if cc != nil {
				c.mu.Lock()
				delete(c.pending, id)
				c.mu.Unlock()
			}
		}
	}()

	if err := c.stream.WriteObject(m); err != nil {
		return nil, err
	}
	return cc, nil
}

// Waiter proxies an ongoing JSON-RPC call.
type Waiter struct {
	*call
}

// Wait for the result of an ongoing JSON-RPC call. If the response
// is successful, its result is stored in result (a pointer to a
// value that can be JSON-unmarshaled into); otherwise, a non-nil
// error is returned.
func (w Waiter) Wait(ctx context.Context, result interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()

	case err, ok := <-w.call.done:
		if !ok {
			return ErrClosed
		}
		if err != nil || result == nil {
			return err
		}
		if w.call.response.Result == nil {
			w.call.response.Result = &jsonNull
		}
		return json.Unmarshal(*w.call.response.Result, result)
	}
}

// call represents a JSON-RPC call over its entire lifecycle.
type call struct {
	request  *Request
	response *Response
	seq      uint64 // the seq of the request
	done     chan error
}

// anyMessage represents either a JSON Request or Response.
type anyMessage struct {
	request  *Request
	response *Response
}

func (m anyMessage) MarshalJSON() ([]byte, error) {
	var v interface{}
	switch {
	case m.request != nil && m.response == nil:
		v = m.request
	case m.request == nil && m.response != nil:
		v = m.response
	}
	if v != nil {
		return json.Marshal(v)
	}
	return nil, errors.New("jsonrpc2: message must have exactly one of the request or response fields set")
}

func (m *anyMessage) UnmarshalJSON(data []byte) error {
	// The presence of these fields distinguishes between the 2
	// message types.
	type msg struct {
		ID     interface{}              `json:"id"`
		Method *string                  `json:"method"`
		Result anyValueWithExplicitNull `json:"result"`
		Error  interface{}              `json:"error"`
	}

	var isRequest, isResponse bool
	checkType := func(m *msg) error {
		mIsRequest := m.Method != nil
		mIsResponse := m.Result.null || m.Result.value != nil || m.Error != nil
		if (!mIsRequest && !mIsResponse) || (mIsRequest && mIsResponse) {
			return errors.New("jsonrpc2: unable to determine message type (request or response)")
		}
		if (mIsRequest && isResponse) || (mIsResponse && isRequest) {
			return errors.New("jsonrpc2: batch message type mismatch (must be all requests or all responses)")
		}
		isRequest = mIsRequest
		isResponse = mIsResponse
		return nil
	}

	if isArray := len(data) > 0 && data[0] == '['; isArray {
		var msgs []msg
		if err := json.Unmarshal(data, &msgs); err != nil {
			return err
		}
		if len(msgs) == 0 {
			return errors.New("jsonrpc2: invalid empty batch")
		}
		for i := range msgs {
			if err := checkType(&msgs[i]); err != nil {
				return err
			}
		}
	} else {
		var m msg
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		if err := checkType(&m); err != nil {
			return err
		}
	}

	var v interface{}
	switch {
	case isRequest && !isResponse:
		v = &m.request
	case !isRequest && isResponse:
		v = &m.response
	}
	if err := json.Unmarshal(data, v); err != nil {
		return err
	}
	if !isRequest && isResponse && m.response.Error == nil && m.response.Result == nil {
		m.response.Result = &jsonNull
	}
	return nil
}

// anyValueWithExplicitNull is used to distinguish {} from
// {"result":null} by anyMessage's JSON unmarshaler.
type anyValueWithExplicitNull struct {
	null  bool // JSON "null"
	value interface{}
}

func (v anyValueWithExplicitNull) MarshalJSON() ([]byte, error) {
	return json.Marshal(v.value)
}

func (v *anyValueWithExplicitNull) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if string(data) == "null" {
		*v = anyValueWithExplicitNull{null: true}
		return nil
	}
	*v = anyValueWithExplicitNull{}
	return json.Unmarshal(data, &v.value)
}
