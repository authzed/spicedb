package wire

import (
	"context"
	"errors"
)

// QueuedDataWriter implements DataWriter interface
// It collects query results in memory for later replay during pipelining
type QueuedDataWriter struct {
	columns Columns
	rows    [][]any
	tag     string
	empty   bool
	written uint32
	err     error
	limit   Limit
}

// Implement DataWriter interface

func (rc *QueuedDataWriter) Row(values []any) error {
	if rc.err != nil {
		return rc.err
	}

	rc.rows = append(rc.rows, values)
	rc.written++
	return nil
}

func (rc *QueuedDataWriter) Complete(tag string) error {
	rc.tag = tag
	return nil
}

func (rc *QueuedDataWriter) Empty() error {
	rc.empty = true
	return nil
}

func (rc *QueuedDataWriter) Columns() Columns {
	return rc.columns
}

func (rc *QueuedDataWriter) Written() uint32 {
	return rc.written
}

func (rc *QueuedDataWriter) CopyIn(format FormatCode) (*CopyReader, error) {
	return nil, errors.New("CopyIn not supported in pipeline mode")
}

func (rc *QueuedDataWriter) Limit() uint32 {
	return uint32(rc.limit)
}

// SetError sets the error state
func (rc *QueuedDataWriter) SetError(err error) {
	rc.err = err
}

// GetError gets the error state
func (rc *QueuedDataWriter) GetError() error {
	return rc.err
}

// Replay writes all collected data to a real DataWriter
func (rc *QueuedDataWriter) Replay(ctx context.Context, writer DataWriter) error {
	if rc.err != nil {
		return rc.err
	}

	// Write all collected rows
	for _, row := range rc.rows {
		if err := writer.Row(row); err != nil {
			return err
		}
	}

	// Send completion
	if rc.tag != "" {
		return writer.Complete(rc.tag)
	}

	return nil
}

// NewQueuedDataWriter creates a DataWriter that collects results for pipelining
func NewQueuedDataWriter(ctx context.Context, columns Columns, limit Limit) *QueuedDataWriter {
	return &QueuedDataWriter{
		columns: columns,
		limit:   limit,
	}
}
