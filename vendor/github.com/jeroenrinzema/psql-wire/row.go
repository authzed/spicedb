package wire

import (
	"context"
	"errors"
	"fmt"

	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// Columns represent a collection of columns.
type Columns []Column

// Define writes the table [RowDescription] headers for the given table and the
// containing columns. The headers have to be written before any data rows could
// be send back to the client. The given columns are encoded using the given
// format codes. Columns could be encoded as Text or Binary. If you provide a
// single format code, it will be applied to all columns.
//
// [RowDescription]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
func (columns Columns) Define(ctx context.Context, writer *buffer.Writer, formats []FormatCode) error {
	if len(columns) == 0 {
		return nil
	}

	writer.Start(types.ServerRowDescription)
	writer.AddInt16(int16(len(columns)))

	if len(formats) == 0 {
		formats = []FormatCode{TextFormat}
	}

	for index := range columns {
		format := formats[0]
		if len(formats) > index {
			format = formats[index]
		}

		columns[index].Define(ctx, writer, format)
	}

	return writer.End()
}

// CopyIn sends a [CopyInResponse] to the client, to initiate a CopyIn
// operation. Based on the given columns within the prepared statement.
// https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-COPYINRESPONSE
func (columns Columns) CopyIn(ctx context.Context, writer *buffer.Writer, format FormatCode) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if len(columns) == 0 {
		return errors.New("at least one column needs to be defined within the prepared statement")
	}

	writer.Start(types.ServerCopyInResponse)
	writer.AddByte(byte(format))
	writer.AddInt16(int16(len(columns)))

	for range columns {
		writer.AddInt16(int16(format))
	}

	return writer.End()
}

// Write writes the given column values back to the client. The given columns
// are encoded using the given format codes. Columns could be encoded as Text or
// Binary. If you provide a single format code, it will be applied to all
// columns.
func (columns Columns) Write(ctx context.Context, formats []FormatCode, writer *buffer.Writer, srcs []any) (err error) {
	if len(srcs) != len(columns) {
		return fmt.Errorf("unexpected columns, %d columns are defined inside the given table but %d were given", len(columns), len(srcs))
	}

	writer.Start(types.ServerDataRow)
	writer.AddInt16(int16(len(columns)))

	if len(formats) == 0 {
		formats = []FormatCode{TextFormat}
	}

	for index, column := range columns {
		format := formats[0]
		if len(formats) > index {
			format = formats[index]
		}

		err = column.Write(ctx, writer, format, srcs[index])
		if err != nil {
			return err
		}
	}

	return writer.End()
}

// Column represents a table column and its [attributes] such as name, type and
// encode formatter.
//
// [attributes]: https://www.postgresql.org/docs/8.3/catalog-pg-attribute.html
type Column struct {
	Table        int32  // table id
	ID           int32  // column identifier
	Attr         int16  // column attribute number
	Name         string // column name
	AttrNo       int16  // column attribute no (optional)
	Oid          uint32
	Width        int16
	TypeModifier int32
}

// Define writes the column header values to the given writer.
// This method is used to define a column inside [RowDescription] message defining
// the column type, width, and name.
//
// [RowDescription]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-ROWDESCRIPTION
func (column Column) Define(ctx context.Context, writer *buffer.Writer, format FormatCode) {
	writer.AddString(column.Name)
	writer.AddNullTerminate()
	writer.AddInt32(column.Table)
	writer.AddInt16(column.AttrNo)
	writer.AddInt32(int32(column.Oid))
	writer.AddInt16(column.Width)
	// TODO: Support type for type modifiers
	//
	// Some types could be overridden using the type modifier field within a RowDescription.
	// Type modifier (see pg_attribute.atttypmod). The meaning of the
	// modifier is type-specific.
	// Atttypmod records type-specific data supplied at table creation time (for
	// example, the maximum length of a varchar column). It is passed to
	// type-specific input functions and length coercion functions. The value
	// will generally be -1 for types that do not need atttypmod.
	//
	// https://www.postgresql.org/docs/current/protocol-message-formats.html
	// https://www.postgresql.org/docs/current/catalog-pg-attribute.html

	writer.AddInt32(-1)
	writer.AddInt16(int16(format))
}

// Write encodes the given source value using the column type definition and connection
// info. The encoded byte buffer is added to the given write buffer. This method
// Is used to encode values and return them inside a [DataRow] message.
//
// [DataRow]: https://www.postgresql.org/docs/current/protocol-message-formats.html#PROTOCOL-MESSAGE-FORMATS-DATAROW
func (column Column) Write(ctx context.Context, writer *buffer.Writer, format FormatCode, src any) (err error) {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	tm := TypeMap(ctx)
	if tm == nil {
		return errors.New("postgres connection info has not been defined inside the given context")
	}

	bb := make([]byte, 0)
	bb, err = tm.Encode(uint32(column.Oid), int16(format), src, bb)
	if err != nil {
		return err
	}

	// NOTE: The length of the column value, in bytes (this count does
	// not include itself). Can be zero. As a special case, -1 indicates a NULL
	// column value. No value bytes follow in the NULL case.
	length := int32(len(bb))
	if src == nil {
		length = -1
	}

	writer.AddInt32(length)
	writer.AddBytes(bb)

	return nil
}
