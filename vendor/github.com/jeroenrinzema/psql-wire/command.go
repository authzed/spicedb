package wire

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// NewErrUnimplementedMessageType is called whenever an unimplemented message
// type is sent. This error indicates to the client that the sent message cannot
// be processed at this moment in time.
func NewErrUnimplementedMessageType(t types.ClientMessage) error {
	err := fmt.Errorf("unimplemented client message type: %d", t)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.ConnectionDoesNotExist), psqlerr.LevelFatal)
}

// NewErrUnkownStatement is returned whenever no executable has been found for
// the given name.
func NewErrUnkownStatement(name string) error {
	err := fmt.Errorf("unknown executeable: %s", name)
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.InvalidPreparedStatementDefinition), psqlerr.LevelFatal)
}

// NewErrUndefinedStatement is returned whenever no statement has been defined
// within the incoming query.
func NewErrUndefinedStatement() error {
	err := errors.New("no statement has been defined")
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.Syntax), psqlerr.LevelError)
}

// NewErrMultipleCommandsStatements is returned whenever multiple statements have been
// given within a single query during the extended query protocol.
func NewErrMultipleCommandsStatements() error {
	err := errors.New("cannot insert multiple commands into a prepared statement")
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.Syntax), psqlerr.LevelError)
}

// newErrClientCopyFailed is returned whenever the client aborts a copy operation.
func newErrClientCopyFailed(desc string) error {
	err := fmt.Errorf("client aborted copy: %s", desc)
	// TODO: What error code should this really be?
	return psqlerr.WithSeverity(psqlerr.WithCode(err, codes.Uncategorized), psqlerr.LevelError)
}

type Session struct {
	*Server
	Statements StatementCache
	Portals    PortalCache
	Attributes map[string]interface{}

	// pipelining
	ParallelPipeline ParallelPipelineConfig
	ResponseQueue    *ResponseQueue
}

// consumeCommands consumes incoming commands sent over the Postgres wire connection.
// Commands consumed from the connection are returned through a go channel.
// Responses for the given message type are written back to the client.
// This method keeps consuming messages until the client issues a close message
// or the connection is terminated.
func (srv *Session) consumeCommands(ctx context.Context, conn net.Conn, reader *buffer.Reader, writer *buffer.Writer) error {
	srv.logger.Debug("ready for query... starting to consume commands")

	err := readyForQuery(writer, types.ServerIdle)
	if err != nil {
		return err
	}

	defer srv.Close()

	for {
		if err = srv.consumeSingleCommand(ctx, reader, writer, conn); err != nil {
			return err
		}
	}
}

func (srv *Session) consumeSingleCommand(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer, conn net.Conn) error {
	t, length, err := reader.ReadTypedMsg()
	if err == io.EOF {
		return err
	}

	// NOTE: we could recover from this scenario
	if errors.Is(err, buffer.ErrMessageSizeExceeded) {
		err = handleMessageSizeExceeded(reader, writer, err)
		if err != nil {
			return err
		}

		return nil
	}

	if err != nil {
		return err
	}

	if srv.closing.Load() {
		return nil
	}

	// NOTE: we increase the wait group by one in order to make sure that idle
	// connections are not blocking a close.
	srv.wg.Add(1)
	srv.logger.Debug("<- incoming command", slog.Int("length", length), slog.String("type", t.String()))
	err = srv.handleCommand(ctx, conn, t, reader, writer)
	srv.wg.Done()
	if errors.Is(err, io.EOF) {
		return nil
	}

	return err
}

// handleMessageSizeExceeded attempts to unwrap the given error message as
// message size exceeded. The expected message size will be consumed and
// discarded from the given reader. An error message is written to the client
// once the expected message size is read.
//
// The given error is returned if it does not contain an message size exceeded
// type. A fatal error is returned when an unexpected error is returned while
// consuming the expected message size or when attempting to write the error
// message back to the client.
func handleMessageSizeExceeded(reader *buffer.Reader, writer *buffer.Writer, exceeded error) (err error) {
	unwrapped, has := buffer.UnwrapMessageSizeExceeded(exceeded)
	if !has {
		return exceeded
	}

	err = reader.Slurp(unwrapped.Size)
	if err != nil {
		return err
	}

	return ErrorCode(writer, exceeded)
}

// handleCommand handles the given client message. A client message includes a
// message type and reader buffer containing the actual message. The type
// indicates a action executed by the client.
// https://www.postgresql.org/docs/14/protocol-message-formats.html
func (srv *Session) handleCommand(ctx context.Context, conn net.Conn, t types.ClientMessage, reader *buffer.Reader, writer *buffer.Writer) error {
	switch t {
	case types.ClientSimpleQuery:
		return srv.handleSimpleQuery(ctx, reader, writer)
	case types.ClientExecute:
		return srv.handleExecute(ctx, reader, writer)
	case types.ClientParse:
		return srv.handleParse(ctx, reader, writer)
	case types.ClientDescribe:
		// The Describe message (portal variant) specifies the name of an
		// existing portal (or an empty string for the unnamed portal). The
		// response is a RowDescription message describing the rows that will be
		// returned by executing the portal; or a NoData message if the portal
		// does not contain a query that will return rows; or ErrorResponse if
		// there is no such portal.
		//
		// The Describe message (statement variant) specifies the name of an
		// existing prepared statement (or an empty string for the unnamed
		// prepared statement). The response is a ParameterDescription message
		// describing the parameters needed by the statement, followed by a
		// RowDescription message describing the rows that will be returned when
		// the statement is eventually executed (or a NoData message if the
		// statement will not return rows). ErrorResponse is issued if there is
		// no such prepared statement. Note that since Bind has not yet been
		// issued, the formats to be used for returned columns are not yet known
		// to the backend; the format code fields in the RowDescription message
		// will be zeroes in this case.
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
		return srv.handleDescribe(ctx, reader, writer)
	case types.ClientSync:
		// TODO: Include the ability to catch sync messages in order to
		// close the current transaction.
		//
		// At completion of each series of extended-query messages, the frontend
		// should issue a Sync message. This parameterless message causes the
		// backend to close the current transaction if it's not inside a
		// BEGIN/COMMIT transaction block (“close” meaning to commit if no
		// error, or roll back if error). Then a ReadyForQuery response is
		// issued. The purpose of Sync is to provide a resynchronization point
		// for error recovery. When an error is detected while processing any
		// extended-query message, the backend issues ErrorResponse, then reads
		// and discards messages until a Sync is reached, then issues
		// ReadyForQuery and returns to normal message processing. (But note
		// that no skipping occurs if an error is detected while processing Sync
		// — this ensures that there is one and only one ReadyForQuery sent for
		// each Sync.)
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
		return srv.handleSync(ctx, writer)
	case types.ClientBind:
		return srv.handleBind(ctx, reader, writer)
	case types.ClientFlush:
		// The Flush message does not cause any specific
		// output to be generated, but forces the backend to deliver any data
		// pending in its output buffers. A Flush must be sent after any
		// extended-query command except Sync, if the frontend wishes to examine
		// the results of that command before issuing more commands. Without
		// Flush, messages returned by the backend will be combined into the
		// minimum possible number of packets to minimize network overhead.
		// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
		if srv.ParallelPipeline.Enabled {
			// NOTE: We use processResponseQueue which blocks until all pending
			// results are ready. Since our main loop blocks on reading the next
			// command, we cannot asynchronously stream results while waiting for
			// the client. The client waits for results after Flush before sending
			// the next command. This creates a deadlock if we don't deliver results here.
			// Effectively, Flush becomes a synchronization barrier for the current batch.
			if err := srv.processResponseQueue(ctx, writer); err != nil {
				return err
			}
		}

		if srv.FlushConn != nil {
			return srv.FlushConn(ctx)
		}

		return nil
	case types.ClientCopyData, types.ClientCopyDone, types.ClientCopyFail:
		// We're supposed to ignore these messages, per the protocol spec. This
		// state will happen when an error occurs on the server-side during a copy
		// operation: the server will send an error and a ready message back to
		// the client, and must then ignore further copy messages. See:
		// https://github.com/postgres/postgres/blob/6e1dd2773eb60a6ab87b27b8d9391b756e904ac3/src/backend/tcop/postgres.c#L4295
		return nil
	case types.ClientClose:
		writer.Start(types.ServerCloseComplete) //nolint:errcheck
		writer.End()                            //nolint:errcheck
		return nil
	case types.ClientTerminate:
		err := srv.handleConnTerminate(ctx)
		if err != nil {
			return err
		}

		err = conn.Close()
		if err != nil {
			return err
		}

		return io.EOF
	default:
		return ErrorCode(writer, NewErrUnimplementedMessageType(t))
	}
}

func (srv *Session) handleSimpleQuery(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.parse == nil {
		return ErrorCode(writer, NewErrUnimplementedMessageType(types.ClientSimpleQuery))
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming simple query", slog.String("query", query))

	// NOTE: If a completely empty (no contents other than whitespace) query
	// string is received, the response is EmptyQueryResponse followed by
	// ReadyForQuery.
	// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
	if strings.TrimSpace(query) == "" {
		writer.Start(types.ServerEmptyQuery)
		err = writer.End()
		if err != nil {
			return err
		}

		return readyForQuery(writer, types.ServerIdle)
	}

	statements, err := srv.parse(ctx, query)
	if err != nil {
		return ErrorCode(writer, err)
	}

	if len(statements) == 0 {
		return ErrorCode(writer, NewErrUndefinedStatement())
	}

	// NOTE: it is possible to send multiple statements in one simple query.
	for index := range statements {
		err = statements[index].columns.Define(ctx, writer, nil)
		if err != nil {
			return ErrorCode(writer, err)
		}

		err = statements[index].fn(ctx, NewDataWriter(ctx, statements[index].columns, nil, NoLimit, reader, writer), nil)
		if err != nil {
			return ErrorCode(writer, err)
		}
	}

	return readyForQuery(writer, types.ServerIdle)
}

func (srv *Session) handleParse(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.parse == nil || srv.Statements == nil {
		err := NewErrUnimplementedMessageType(types.ClientParse)
		if srv.ParallelPipeline.Enabled {
			return srv.drainQueueAndWriteError(ctx, writer, err)
		}
		return ErrorCode(writer, err)
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	query, err := reader.GetString()
	if err != nil {
		return err
	}

	// NOTE: the number of parameter data types specified (can be
	// zero). Note that this is not an indication of the number of parameters
	// that might appear in the query string, only the number that the frontend
	// wants to prespecify types for.
	parameters, err := reader.GetUint16()
	if err != nil {
		return err
	}

	srv.logger.Debug("predefined parameters", slog.Int("parameters", int(parameters)))

	for i := uint16(0); i < parameters; i++ {
		// TODO: Specifies the object ID of the parameter data type
		//
		// Specifies the object ID of the parameter data type. Placing a zero here
		// is equivalent to leaving the type unspecified.
		// `reader.GetUint32()`
	}

	if srv.ParallelPipeline.Enabled {
		return srv.parsePipelined(ctx, writer, name, query)
	}

	statement, err := singleStatement(srv.parse(ctx, query))
	if err != nil {
		return ErrorCode(writer, err)
	}

	srv.logger.Debug("incoming extended query", slog.String("query", query), slog.String("name", name), slog.Int("parameters", len(statement.parameters)))

	err = srv.Statements.Set(ctx, name, statement)
	if err != nil {
		return ErrorCode(writer, err)
	}

	writer.Start(types.ServerParseComplete)
	return writer.End()
}

// parsePipelined handles Parse in parallel pipeline mode
func (srv *Session) parsePipelined(ctx context.Context, writer *buffer.Writer, name, query string) error {
	statement, err := singleStatement(srv.parse(ctx, query))
	if err != nil {
		return srv.drainQueueAndWriteError(ctx, writer, err)
	}

	srv.logger.Debug("incoming extended query", slog.String("query", query), slog.String("name", name), slog.Int("parameters", len(statement.parameters)))

	err = srv.Statements.Set(ctx, name, statement)
	if err != nil {
		return srv.drainQueueAndWriteError(ctx, writer, err)
	}

	srv.ResponseQueue.Enqueue(NewParseCompleteEvent())
	return nil
}

func (srv *Session) handleDescribe(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	d, err := reader.GetBytes(1)
	if err != nil {
		return err
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	srv.logger.Debug("incoming describe request", slog.String("type", types.DescribeMessage(d[0]).String()), slog.String("name", name))

	if srv.ParallelPipeline.Enabled {
		return srv.describePipelined(ctx, writer, types.DescribeMessage(d[0]), name)
	}

	switch types.DescribeMessage(d[0]) {
	case types.DescribeStatement:
		statement, err := srv.Statements.Get(ctx, name)
		if err != nil {
			return err
		}

		if statement == nil {
			return ErrorCode(writer, errors.New("unknown statement"))
		}

		if err := srv.writeParameterDescription(writer, statement.parameters); err != nil {
			return err
		}
		return srv.writeColumnDescription(ctx, writer, nil, statement.columns)

	case types.DescribePortal:
		portal, err := srv.Portals.Get(ctx, name)
		if err != nil {
			return err
		}

		if portal == nil {
			return ErrorCode(writer, errors.New("unknown portal"))
		}

		return srv.writeColumnDescription(ctx, writer, portal.formats, portal.statement.columns)
	}

	return ErrorCode(writer, fmt.Errorf("unknown describe command: %s", string(d[0])))
}

// describePipelined handles Describe in parallel pipeline mode
func (srv *Session) describePipelined(ctx context.Context, writer *buffer.Writer, descType types.DescribeMessage, name string) error {
	switch descType {
	case types.DescribeStatement:
		statement, err := srv.Statements.Get(ctx, name)
		if err != nil {
			return srv.drainQueueAndWriteError(ctx, writer, err)
		}

		if statement == nil {
			return srv.drainQueueAndWriteError(ctx, writer, errors.New("unknown statement"))
		}

		srv.ResponseQueue.Enqueue(NewStmtDescribeEvent(statement.parameters, statement.columns))
		return nil

	case types.DescribePortal:
		portal, err := srv.Portals.Get(ctx, name)
		if err != nil {
			return srv.drainQueueAndWriteError(ctx, writer, err)
		}

		if portal == nil {
			return srv.drainQueueAndWriteError(ctx, writer, errors.New("unknown portal"))
		}

		srv.ResponseQueue.Enqueue(NewPortalDescribeEvent(portal.statement.columns, portal.formats))
		return nil
	}

	return srv.drainQueueAndWriteError(ctx, writer, fmt.Errorf("unknown describe command: %s", string(descType)))
}

// https://www.postgresql.org/docs/15/protocol-message-formats.html
func (srv *Session) writeParameterDescription(writer *buffer.Writer, parameters []uint32) error {
	writer.Start(types.ServerParameterDescription)
	writer.AddInt16(int16(len(parameters)))

	for _, parameter := range parameters {
		writer.AddInt32(int32(parameter))
	}

	return writer.End()
}

// writeColumnDescription attempts to write the statement column descriptions
// back to the writer buffer. Information about the returned columns is written
// to the client.
// https://www.postgresql.org/docs/15/protocol-message-formats.html
func (srv *Session) writeColumnDescription(ctx context.Context, writer *buffer.Writer, formats []FormatCode, columns Columns) error {
	if len(columns) == 0 {
		writer.Start(types.ServerNoData)
		return writer.End()
	}

	return columns.Define(ctx, writer, formats)
}

func (srv *Session) handleBind(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	name, err := reader.GetString()
	if err != nil {
		return err
	}

	statement, err := reader.GetString()
	if err != nil {
		return err
	}

	parameters, err := srv.readParameters(ctx, reader)
	if err != nil {
		return err
	}

	formats, err := srv.readColumnTypes(reader)
	if err != nil {
		return err
	}

	if srv.ParallelPipeline.Enabled {
		return srv.bindPipelined(ctx, writer, name, statement, parameters, formats)
	}

	stmt, err := srv.Statements.Get(ctx, statement)
	if err != nil {
		return err
	}

	if stmt == nil {
		return NewErrUnkownStatement(statement)
	}

	err = srv.Portals.Bind(ctx, name, stmt, parameters, formats)
	if err != nil {
		return err
	}

	writer.Start(types.ServerBindComplete)
	return writer.End()
}

// bindPipelined handles Bind in parallel pipeline mode
func (srv *Session) bindPipelined(ctx context.Context, writer *buffer.Writer, name, statement string, parameters []Parameter, formats []FormatCode) error {
	stmt, err := srv.Statements.Get(ctx, statement)
	if err != nil {
		return srv.drainQueueAndWriteError(ctx, writer, err)
	}

	if stmt == nil {
		return srv.drainQueueAndWriteError(ctx, writer, NewErrUnkownStatement(statement))
	}

	err = srv.Portals.Bind(ctx, name, stmt, parameters, formats)
	if err != nil {
		return srv.drainQueueAndWriteError(ctx, writer, err)
	}

	srv.ResponseQueue.Enqueue(NewBindCompleteEvent())
	return nil
}

// readParameters attempts to read all incoming parameters from the given
// reader. The parameters are parsed and returned.
// https://www.postgresql.org/docs/14/protocol-message-formats.html
func (srv *Session) readParameters(ctx context.Context, reader *buffer.Reader) ([]Parameter, error) {
	// NOTE: read the total amount of parameter format length that will be send
	// by the client. This can be zero to indicate that there are no parameters
	// or that the parameters all use the default format (text); or one, in
	// which case the specified format code is applied to all parameters; or it
	// can equal the actual number of parameters.
	length, err := reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading parameters format codes", slog.Uint64("length", uint64(length)))

	defaultFormat := TextFormat
	formats := make([]FormatCode, length)
	for i := uint16(0); i < length; i++ {
		format, err := reader.GetUint16()
		if err != nil {
			return nil, err
		}

		// NOTE: we have to set the default format code to the given format code
		// if only one is given according to the protocol specs. The for loop
		// should not be aborted since the formats slice is buffered.
		if length == 1 {
			defaultFormat = FormatCode(format)
		}

		formats[i] = FormatCode(format)
	}

	// NOTE: read the total amount of parameter values that will be send
	// by the client.
	length, err = reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading parameters values", slog.Uint64("length", uint64(length)))

	parameters := make([]Parameter, length)
	for i := 0; i < int(length); i++ {
		length, err := reader.GetInt32()
		if err != nil {
			return nil, err
		}

		value, err := reader.GetBytes(int(length))
		if err != nil {
			return nil, err
		}

		srv.logger.Debug("incoming parameter", slog.String("value", string(value)))

		format := defaultFormat
		if len(formats) > int(i) {
			format = formats[i]
		}

		parameters[i] = NewParameter(TypeMap(ctx), format, value)
	}

	return parameters, nil
}

func (srv *Session) readColumnTypes(reader *buffer.Reader) ([]FormatCode, error) {
	length, err := reader.GetUint16()
	if err != nil {
		return nil, err
	}

	srv.logger.Debug("reading column format codes", slog.Uint64("length", uint64(length)))

	columns := make([]FormatCode, length)
	for i := uint16(0); i < length; i++ {
		format, err := reader.GetUint16()
		if err != nil {
			return nil, err
		}

		columns[i] = FormatCode(format)
	}

	return columns, nil
}

func (srv *Session) handleExecute(ctx context.Context, reader *buffer.Reader, writer *buffer.Writer) error {
	if srv.Statements == nil {
		err := NewErrUnimplementedMessageType(types.ClientExecute)
		if srv.ParallelPipeline.Enabled {
			return srv.drainQueueAndWriteError(ctx, writer, err)
		}
		return ErrorCode(writer, err)
	}

	name, err := reader.GetString()
	if err != nil {
		return err
	}

	// NOTE: maximum number of limit to return, if portal contains a
	// query that returns limit (ignored otherwise). Zero denotes "no limit".
	limit, err := reader.GetUint32()
	if err != nil {
		return err
	}

	srv.logger.Debug("executing", slog.String("name", name), slog.Uint64("limit", uint64(limit)))

	if srv.ParallelPipeline.Enabled {
		return srv.executePipelined(ctx, writer, name, limit)
	}

	err = srv.Portals.Execute(ctx, name, Limit(limit), reader, writer)
	if err != nil {
		return ErrorCode(writer, err)
	}

	return nil
}

// executePipelined handles Execute in parallel pipeline mode
func (srv *Session) executePipelined(ctx context.Context, writer *buffer.Writer, name string, limit uint32) error {
	portal, err := srv.Portals.Get(ctx, name)
	if err != nil {
		return srv.drainQueueAndWriteError(ctx, writer, err)
	}

	if portal == nil {
		return srv.drainQueueAndWriteError(ctx, writer, errors.New("unknown portal"))
	}

	// Create result channel and queue the event
	resultChan := make(chan *QueuedDataWriter, 1)
	srv.ResponseQueue.Enqueue(NewExecuteEvent(resultChan, portal.formats))

	// Launch async execution
	go srv.executeAsync(ctx, portal, limit, resultChan)

	return nil
}

// executeAsync runs the portal execution in a separate goroutine
func (srv *Session) executeAsync(ctx context.Context, portal *Portal, limit uint32, resultChan chan<- *QueuedDataWriter) {
	defer close(resultChan)
	defer func() {
		if r := recover(); r != nil {
			collector := NewQueuedDataWriter(ctx, portal.statement.columns, Limit(limit))
			collector.SetError(fmt.Errorf("panic during execution: %v", r))
			resultChan <- collector
		}
	}()

	srv.logger.Debug("starting async execution",
		slog.Bool("has_function", portal.statement.fn != nil))

	collector := NewQueuedDataWriter(ctx, portal.statement.columns, Limit(limit))

	err := portal.statement.fn(ctx, collector, portal.parameters)
	if err != nil {
		collector.SetError(err)
	}

	srv.logger.Debug("async execution complete",
		slog.Bool("has_error", collector.GetError() != nil),
		slog.Int("rows", int(collector.Written())))

	resultChan <- collector
}

// handleSync handles the Sync message (extended query protocol)
func (srv *Session) handleSync(ctx context.Context, writer *buffer.Writer) error {
	if srv.ParallelPipeline.Enabled {
		srv.logger.Debug("draining response queue", slog.Int("length", srv.ResponseQueue.Len()))

		if err := srv.processResponseQueue(ctx, writer); err != nil {
			return err
		}
	}

	// Original synchronous behavior - just return ReadyForQuery
	return readyForQuery(writer, types.ServerIdle)
}

// processResponseQueue drains the queue and writes all events to the writer
func (srv *Session) processResponseQueue(ctx context.Context, writer *buffer.Writer) error {
	events, queueErr := srv.ResponseQueue.DrainSync(ctx)

	for _, event := range events {
		if err := srv.writeQueuedResponse(ctx, writer, event); err != nil {
			return err
		}
	}

	if queueErr != nil {
		if err := ErrorCode(writer, queueErr); err != nil {
			return err
		}
	}

	srv.ResponseQueue.Clear()
	return nil
}

func (srv *Session) handleConnTerminate(ctx context.Context) error {
	if srv.TerminateConn == nil {
		return nil
	}

	return srv.TerminateConn(ctx)
}

// drainQueueOnError drains the response queue and writes all pending responses.
// This can be called when an error occurs to flush any queued successful operations
// before reporting the error.
func (srv *Session) drainQueueOnError(ctx context.Context, writer *buffer.Writer) error {
	if srv.ResponseQueue == nil || srv.ResponseQueue.Len() == 0 {
		return nil
	}

	events, queueErr := srv.ResponseQueue.DrainSync(ctx)
	for _, event := range events {
		if werr := srv.writeQueuedResponse(ctx, writer, event); werr != nil {
			return werr
		}
	}

	srv.ResponseQueue.Clear()

	if queueErr != nil {
		return ErrorCode(writer, queueErr)
	}

	return nil
}

// drainQueueAndWriteError drains the queue and returns an error code.
// This ensures all pending successful responses are written before reporting an error.
func (srv *Session) drainQueueAndWriteError(ctx context.Context, writer *buffer.Writer, err error) error {
	if drainErr := srv.drainQueueOnError(ctx, writer); drainErr != nil {
		return drainErr
	}
	return ErrorCode(writer, err)
}

func singleStatement(stmts PreparedStatements, err error) (*PreparedStatement, error) {
	if err != nil {
		return nil, err
	}

	if len(stmts) > 1 {
		return nil, NewErrMultipleCommandsStatements()
	}

	if len(stmts) == 0 {
		return nil, NewErrUndefinedStatement()
	}

	return stmts[0], nil
}

// writeQueuedResponse writes a queued response event to the wire
func (srv *Session) writeQueuedResponse(ctx context.Context, writer *buffer.Writer, event *ResponseEvent) error {
	switch event.Kind {
	case ResponseParseComplete:
		writer.Start(types.ServerParseComplete)
		return writer.End()

	case ResponseBindComplete:
		writer.Start(types.ServerBindComplete)
		return writer.End()

	case ResponseStmtDescribe:
		// Statement Describe writes ParameterDescription followed by RowDescription
		if err := srv.writeParameterDescription(writer, event.Parameters); err != nil {
			return err
		}
		// Write column description (no formats for statement describe)
		return srv.writeColumnDescription(ctx, writer, nil, event.Columns)

	case ResponsePortalDescribe:
		// Portal Describe only writes RowDescription
		return srv.writeColumnDescription(ctx, writer, event.Formats, event.Columns)

	case ResponseExecute:
		// Execute writes DataRows followed by CommandComplete
		if event.Result == nil {
			// No result yet, this shouldn't happen in normal flow
			return errors.New("execute event has no result")
		}

		// Check for execution error
		if err := event.Result.GetError(); err != nil {
			return ErrorCode(writer, err)
		}

		// Use DataWriter for correct encoding
		// Note: We use NoLimit here because the result is already limited during execution
		dataWriter := NewDataWriter(ctx, event.Result.Columns(), event.Formats, NoLimit, nil, writer)

		return event.Result.Replay(ctx, dataWriter)

	default:
		return fmt.Errorf("unknown response event kind: %v", event.Kind)
	}
}

func (srv *Session) Close() {
	srv.Statements.Close()
	srv.Portals.Close()
}
