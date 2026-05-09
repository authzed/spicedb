package wire

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jeroenrinzema/psql-wire/pkg/buffer"
	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

// CopySignature is the signature that is used to identify the start of a copy-in
// stream. The signature is used to identify the start of a copy-in stream and is
// used to determine the start of the copy-in data.
// https://www.postgresql.org/docs/current/sql-copy.html
var CopySignature = []byte("PGCOPY\n\377\r\n\000")

// NewCopyReader creates a new copy reader that reads copy-in data from the given
// reader and writes the data to the given writer. The columns are used to determine
// the format of the data that is read from the reader.
func NewCopyReader(reader *buffer.Reader, writer *buffer.Writer, columns Columns) *CopyReader {
	return &CopyReader{
		Reader:  reader,
		writer:  writer,
		columns: columns, // NOTE: the columns are only used to determine the format of the data that is read from the reader.
		chunk:   make([]byte, reader.MaxMessageSize),
	}
}

type CopyReader struct {
	*buffer.Reader
	writer  *buffer.Writer
	columns Columns
	chunk   []byte
}

// Columns returns the columns that are currently defined within the copy reader.
func (r *CopyReader) Columns() Columns {
	return r.columns
}

// Read reads a single chunk from the copy-in stream. The read chunk is returned
// as a byte slice. If the end of the copy-in stream is reached, an io.EOF error
// is returned.
func (r *CopyReader) Read() error {
reader:
	for {
		typed, _, err := r.ReadTypedMsg()
		if err != nil {
			return err
		}

		switch typed {
		case types.ClientFlush, types.ClientSync:
			// The backend will ignore Flush and Sync messages received during copy-in mode.
			// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
			continue reader
		case types.ClientCopyData:
			return nil
		case types.ClientCopyDone:
			return io.EOF
		case types.ClientCopyFail:
			desc, err := r.GetString()
			if err != nil {
				return err
			}
			return ErrorCode(r.writer, newErrClientCopyFailed(desc))
		default:
			// Receipt of any other non-copy message type constitutes an error that
			// will abort the copy-in state as described above.
			// https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-COPY
			return ErrorCode(r.writer, NewErrUnimplementedMessageType(typed))
		}
	}
}

// Scanner is a function that scans a byte slice and returns the value as an any
type Scanner func(value []byte) (any, error)

// NewScanner creates a new scanner that scans a byte slice and returns the value
// as an any. The scanner uses the given map to decode the value and the given
// type to determine the format of the data that is scanned.
func NewScanner(tm *pgtype.Map, column Column, format FormatCode) (Scanner, error) {
	typed, has := tm.TypeForOID(uint32(column.Oid))
	if !has {
		return nil, fmt.Errorf("unknown column type: %d", column.Oid)
	}

	return func(value []byte) (any, error) {
		return typed.Codec.DecodeValue(tm, typed.OID, int16(format), value)
	}, nil
}

// NewBinaryColumnReader creates a new column reader that reads rows from the
// given copy reader and returns the values as a slice of any values. The
// columns are used to determine the format of the data that is read from the
// reader. If the end of the copy-in stream is reached, an io.EOF error is
// returned.
func NewBinaryColumnReader(ctx context.Context, copy *CopyReader) (_ *BinaryCopyReader, err error) {
	tm := TypeMap(ctx)
	if tm == nil {
		return nil, errors.New("postgres connection info has not been defined inside the given context")
	}

	scanners := make([]Scanner, len(copy.columns))
	for index, column := range copy.columns {
		scanners[index], err = NewScanner(tm, column, BinaryFormat)
		if err != nil {
			return nil, err
		}
	}

	return &BinaryCopyReader{
		typeMap:  tm,
		reader:   copy,
		scanners: scanners,
	}, nil
}

type BinaryCopyReader struct {
	typeMap  *pgtype.Map
	reader   *CopyReader
	scanners []Scanner
}

// Read reads a single row from the copy-in stream. The read row is returned as a
// slice of any values. If the end of the copy-in stream is reached, an io.EOF error
// is returned.
func (r *BinaryCopyReader) Read(ctx context.Context) (_ []any, err error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// NOTE: read the next chunk from the copy-in stream if the current chunk is empty.
	if len(r.reader.Msg) == 0 {
		err = r.reader.Read()
		if err != nil {
			return nil, err
		}

		has := bytes.HasPrefix(r.reader.Msg, CopySignature)
		if has {
			_, err = r.reader.GetBytes(len(CopySignature))
			if err != nil {
				return nil, err
			}

			// NOTE: 2 x 32-bit integer fields are send after the signature which we ignore for now.
			_, err = r.reader.GetBytes(8)
			if err != nil {
				return nil, err
			}
		}
	}

	fields, err := r.reader.GetUint16()
	if err != nil {
		return nil, err
	}

	row := make([]any, fields)
	for index := range fields {
		length, err := r.reader.GetUint32()
		if err != nil {
			return nil, fmt.Errorf("unexpected field length: %w", err)
		}

		// NOTE: as a special case, -1 (or 255 255 255 255) indicates a NULL field value.
		if length == math.MaxUint32 {
			// r.row[index] = nil
			continue
		}

		value, err := r.reader.GetBytes(int(length))
		if err != nil {
			return nil, fmt.Errorf("unexpected value: %w", err)
		}

		row[index], err = r.scanners[index](value)
		if err != nil {
			return nil, err
		}
	}

	return row, nil
}

// read text data

type TextCopyReader struct {
	typeMap    *pgtype.Map
	reader     *CopyReader
	scanners   []Scanner
	csvReader  *csv.Reader
	buffer     *bytes.Buffer
	bufScanner *bufio.Scanner
	nullValue  string // PostgreSQL NULL value string (default empty)
}

func NewTextColumnReader(ctx context.Context, copy *CopyReader, csvReader *csv.Reader, csvReaderBuffer *bytes.Buffer, nullValue string) (_ *TextCopyReader, err error) {
	tm := TypeMap(ctx)
	if tm == nil {
		return nil, errors.New("postgres connection info has not been defined inside the given context")
	}

	scanners := make([]Scanner, len(copy.columns))
	for index, column := range copy.columns {
		scanners[index], err = NewScanner(tm, column, TextFormat)
		if err != nil {
			return nil, err
		}
	}

	reader := &TextCopyReader{
		typeMap:    tm,
		reader:     copy,
		scanners:   scanners,
		csvReader:  csvReader,
		buffer:     csvReaderBuffer,
		bufScanner: bufio.NewScanner(csvReaderBuffer),
		nullValue:  nullValue,
	}

	return reader, nil
}

// Read reads a single row from the copy-in stream. The read row is returned as a
// slice of any values. If the end of the copy-in stream is reached, an io.EOF error
// is returned.
func (r *TextCopyReader) Read(ctx context.Context) (_ []any, err error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	for {
		// Try to read a CSV record from the current buffer
		record, err := r.csvReader.Read()
		if err == io.EOF {
			// CSV reader hit EOF, need more data from copy stream
			err = r.reader.Read()
			if err == io.EOF {
				// End of copy stream, no more data
				return nil, io.EOF
			}
			if err != nil {
				return nil, err
			}

			// Process PostgreSQL CSV escape sequences before adding to buffer
			processedData := r.preprocessPostgreSQLCSV(r.reader.Msg)
			r.buffer.Write(processedData)

			// Clear the message after copying to buffer
			r.reader.Msg = r.reader.Msg[:0]

			// Continue loop to try reading CSV record again
			continue
		}
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("CSV parsing error: %w", err)
		}
		// Successfully read a CSV record, convert to row
		return r.convertRecord(record)
	}
}

// convertRecord converts a CSV record to a slice of typed values
func (r *TextCopyReader) convertRecord(record []string) ([]any, error) {
	if len(record) != len(r.scanners) {
		return nil, fmt.Errorf("CSV record has %d fields, expected %d", len(record), len(r.scanners))
	}

	row := make([]any, len(record))
	for i, field := range record {
		// Handle NULL values - check both empty string (default) and custom NULL value
		if field == r.nullValue || (r.nullValue == "" && field == "") {
			row[i] = nil
			continue
		}

		// Convert string field to appropriate type using scanner
		value, err := r.scanners[i]([]byte(field))
		if err != nil {
			return nil, fmt.Errorf("failed to scan field %d: %w", i, err)
		}
		row[i] = value
	}

	return row, nil
}

// preprocessPostgreSQLCSV converts PostgreSQL CSV escape sequences to RFC 4180 format
// PostgreSQL uses \ as escape character, but Go's csv package expects "" for quote escaping
func (r *TextCopyReader) preprocessPostgreSQLCSV(data []byte) []byte {
	// Convert PostgreSQL \" to "" for RFC 4180 compliance
	result := bytes.ReplaceAll(data, []byte(`\"`), []byte(`""`))
	return result
}
