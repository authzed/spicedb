package wire

import (
	"context"
	"errors"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Limit represents the maximum number of rows to be written.
// Zero denotes “no limit”.
type Limit uint32

const NoLimit Limit = 0

// DataWriter represents a writer interface for writing columns and data rows
// using the Postgres wire to the connected client.
type DataWriter interface {
	// Row writes a single data row containing the values inside the given slice to
	// the underlaying Postgres client. The column headers have to be written before
	// sending rows. Each item inside the slice represents a single column value.
	// The slice length needs to be the same length as the defined columns. Nil
	// values are encoded as NULL values.
	Row([]any) error

	// Limit returns the maximum number of rows to be written passed within the
	// wire protocol. A value of 0 indicates no limit.
	Limit() uint32

	// Written returns the number of rows written to the client.
	Written() uint32

	// Empty announces to the client an empty response and that no data rows should
	// be expected.
	Empty() error

	// Columns returns the columns that are currently defined within the writer.
	Columns() Columns

	// Complete announces to the client that the command has been completed and
	// no further data should be expected.
	//
	// See [CommandComplete] for the expected format for different queries.
	//
	// [CommandComplete]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COMMANDCOMPLETE
	Complete(description string) error

	// CopyIn sends a [CopyInResponse] to the client, to initiate a CopyIn
	// operation. The copy operation can be used to send large amounts of data to
	// the server in a single transaction. A column reader has to be used to read
	// the data that is sent by the client to the CopyReader.
	CopyIn(format FormatCode) (*CopyReader, error)
}

// ErrDataWritten is returned when an empty result is attempted to be sent to the
// client while data has already been written.
var ErrDataWritten = errors.New("data has already been written")

// ErrClosedWriter is returned when the data writer has been closed.
var ErrClosedWriter = errors.New("closed writer")

var ErrRowLimitExceeded = pgerror.WithCode(errors.New("row limit exceeded"), codes.ProgramLimitExceeded)

// NewDataWriter constructs a new data writer using the given context and
// buffer. The returned writer should be handled with caution as it is not safe
// for concurrent use. Concurrent access to the same data without proper
// synchronization can result in unexpected behavior and data corruption.
func NewDataWriter(ctx context.Context, columns Columns, formats []FormatCode, limit Limit, reader *buffer.Reader, writer *buffer.Writer) DataWriter {
	return &dataWriter{
		ctx:     ctx,
		columns: columns,
		formats: formats,
		limit:   limit,
		client:  writer,
		reader:  reader,
	}
}

// dataWriter is a implementation of the DataWriter interface.
type dataWriter struct {
	ctx     context.Context
	columns Columns
	formats []FormatCode
	limit   Limit
	client  *buffer.Writer
	reader  *buffer.Reader
	closed  bool
	written uint32
}

func (writer *dataWriter) Columns() Columns {
	return writer.columns
}

func (writer *dataWriter) Define(columns Columns) error {
	if writer.closed {
		return ErrClosedWriter
	}

	writer.columns = columns
	return writer.columns.Define(writer.ctx, writer.client, writer.formats)
}

func (writer *dataWriter) Row(values []any) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.limit != 0 && Limit(writer.written) >= writer.limit {
		return ErrRowLimitExceeded
	}

	writer.written++

	return writer.columns.Write(writer.ctx, writer.formats, writer.client, values)
}

func (writer *dataWriter) CopyIn(format FormatCode) (*CopyReader, error) {
	if writer.closed {
		return nil, ErrClosedWriter
	}

	err := writer.columns.CopyIn(writer.ctx, writer.client, format)
	if err != nil {
		return nil, err
	}

	return NewCopyReader(writer.reader, writer.client, writer.columns), nil
}

func (writer *dataWriter) Empty() error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written != 0 {
		return ErrDataWritten
	}

	defer writer.close()
	return nil
}

func (writer *dataWriter) Limit() uint32 {
	return uint32(writer.limit)
}

func (writer *dataWriter) Written() uint32 {
	return writer.written
}

func (writer *dataWriter) Complete(description string) error {
	if writer.closed {
		return ErrClosedWriter
	}

	if writer.written == 0 && writer.columns != nil {
		err := writer.Empty()
		if err != nil {
			return err
		}
	}

	defer writer.close()
	return commandComplete(writer.client, description)
}

func (writer *dataWriter) close() {
	writer.closed = true
}

// commandComplete announces that the requested command has successfully been executed.
// The given description is written back to the client and could be used to send
// additional meta data to the user.
func commandComplete(writer *buffer.Writer, description string) error {
	writer.Start(types.ServerCommandComplete)
	writer.AddString(description)
	writer.AddNullTerminate()
	return writer.End()
}
