package wire

import (
	"github.com/jeroenrinzema/psql-wire/codes"
	psqlerr "github.com/jeroenrinzema/psql-wire/errors"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// errFieldType represents the error fields.
type errFieldType byte

// http://www.postgresql.org/docs/current/static/protocol-error-fields.html
//
//nolint:varcheck,deadcode
const (
	errFieldSeverity       errFieldType = 'S'
	errFieldMsgPrimary     errFieldType = 'M'
	errFieldSQLState       errFieldType = 'C'
	errFieldDetail         errFieldType = 'D'
	errFieldHint           errFieldType = 'H'
	errFieldSrcFile        errFieldType = 'F'
	errFieldSrcLine        errFieldType = 'L'
	errFieldSrcFunction    errFieldType = 'R'
	errFieldConstraintName errFieldType = 'n'
)

// ErrorCode writes an error message as response to a command with the given
// severity and error message. A ready for query message is written back to the
// client once the error has been written indicating the end of a command cycle.
// https://www.postgresql.org/docs/current/static/protocol-error-fields.html
func ErrorCode(writer *buffer.Writer, err error) error {
	desc := psqlerr.Flatten(err)

	writer.Start(types.ServerErrorResponse)

	writer.AddByte(byte(errFieldSeverity))
	writer.AddString(string(desc.Severity))
	writer.AddNullTerminate()
	writer.AddByte(byte(errFieldSQLState))
	writer.AddString(string(desc.Code))
	writer.AddNullTerminate()
	writer.AddByte(byte(errFieldMsgPrimary))
	writer.AddString(desc.Message)
	writer.AddNullTerminate()

	if desc.Hint != "" {
		writer.AddByte(byte(errFieldHint))
		writer.AddString(desc.Hint)
		writer.AddNullTerminate()
	}

	if desc.Detail != "" {
		writer.AddByte(byte(errFieldDetail))
		writer.AddString(desc.Detail)
		writer.AddNullTerminate()
	}

	if desc.Source != nil {
		writer.AddByte(byte(errFieldSrcFile))
		writer.AddString(desc.Source.File)
		writer.AddNullTerminate()

		writer.AddByte(byte(errFieldSrcLine))
		writer.AddInt32(desc.Source.Line)
		writer.AddNullTerminate()

		writer.AddByte(byte(errFieldSrcFunction))
		writer.AddString(desc.Source.Function)
		writer.AddNullTerminate()
	}

	writer.AddNullTerminate()
	err = writer.End()
	if err != nil {
		return err
	}

	// NOTE: we are writing a ready for query message to indicate the end of a
	// command cycle. However, for authentication failures, we skip this
	// because the connection will be terminated.
	if desc.Code == codes.InvalidPassword {
		return nil
	}

	return readyForQuery(writer, types.ServerIdle)
}
