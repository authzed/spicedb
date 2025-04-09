package common

import (
	"context"
	"fmt"

	"github.com/ccoveille/go-safecast"
	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	chunkSize int = 5000
	buffSize  int = 1000
)

type tupleSourceAdapter struct {
	source datastore.BulkWriteRelationshipSource
	ctx    context.Context

	current      *tuple.Relationship
	err          error
	valuesBuffer []any
	colNames     []string
}

// Next returns true if there is another row and makes the next row data
// available to Values(). When there are no more rows available or an error
// has occurred it returns false.
func (tg *tupleSourceAdapter) Next() bool {
	tg.current, tg.err = tg.source.Next(tg.ctx)
	return tg.current != nil
}

// Values returns the values for the current row.
func (tg *tupleSourceAdapter) Values() ([]any, error) {
	var caveatName string
	var caveatContext map[string]any
	if tg.current.OptionalCaveat != nil {
		caveatName = tg.current.OptionalCaveat.CaveatName
		caveatContext = tg.current.OptionalCaveat.Context.AsMap()
	}

	tg.valuesBuffer[0] = tg.current.Resource.ObjectType
	tg.valuesBuffer[1] = tg.current.Resource.ObjectID
	tg.valuesBuffer[2] = tg.current.Resource.Relation
	tg.valuesBuffer[3] = tg.current.Subject.ObjectType
	tg.valuesBuffer[4] = tg.current.Subject.ObjectID
	tg.valuesBuffer[5] = tg.current.Subject.Relation
	tg.valuesBuffer[6] = caveatName
	tg.valuesBuffer[7] = caveatContext
	tg.valuesBuffer[8] = tg.current.OptionalExpiration

	if len(tg.colNames) > 9 && tg.current.OptionalIntegrity != nil {
		tg.valuesBuffer[9] = tg.current.OptionalIntegrity.KeyId
		tg.valuesBuffer[10] = tg.current.OptionalIntegrity.Hash
		tg.valuesBuffer[11] = tg.current.OptionalIntegrity.HashedAt.AsTime()
	}

	return tg.valuesBuffer, nil
}

// Err returns any error that has been encountered by the CopyFromSource. If
// this is not nil *Conn.CopyFrom will abort the copy.
func (tg *tupleSourceAdapter) Err() error {
	return tg.err
}

func BulkLoad(
	ctx context.Context,
	tx pgx.Tx,
	tupleTableName string,
	colNames []string,
	iter datastore.BulkWriteRelationshipSource,
) (uint64, error) {
	adapter := &tupleSourceAdapter{
		source:       iter,
		ctx:          ctx,
		valuesBuffer: make([]any, len(colNames)),
		colNames:     colNames,
	}
	copied, err := tx.CopyFrom(ctx, pgx.Identifier{tupleTableName}, colNames, adapter)
	uintCopied, castErr := safecast.ToUint64(copied)
	if castErr != nil {
		return 0, spiceerrors.MustBugf("number copied was negative: %v", castErr)
	}
	return uintCopied, err
}

// pipeReader implements a buffered pipe for relationships
type pipeReader struct {
	source          datastore.BulkWriteRelationshipSource
	ctx             context.Context
	buffer          []*tuple.Relationship
	bufferSize      int
	readPos         int
	writePos        int
	sourceExhausted bool
}

func newPipeReader(ctx context.Context, source datastore.BulkWriteRelationshipSource, bufferSize int) *pipeReader {
	return &pipeReader{
		source:     source,
		ctx:        ctx,
		buffer:     make([]*tuple.Relationship, bufferSize),
		bufferSize: bufferSize,
	}
}

// readFromSource attempts to fill the buffer from the source
func (p *pipeReader) readFromSource() error {
	if p.sourceExhausted {
		return nil
	}

	// Fill buffer as much as possible
	for p.writePos < p.bufferSize {
		rel, err := p.source.Next(p.ctx)
		if err != nil {
			return err
		}

		if rel == nil {
			p.sourceExhausted = true
			break
		}

		p.buffer[p.writePos] = rel
		p.writePos++
	}

	return nil
}

// Next gets the next relationship from the buffer or source
func (p *pipeReader) Next() (*tuple.Relationship, error) {
	// If we've read all buffered items, reset and refill
	if p.readPos >= p.writePos {
		if p.sourceExhausted {
			return nil, nil // No more data
		}

		// Reset positions
		p.readPos = 0
		p.writePos = 0

		// Fill buffer
		if err := p.readFromSource(); err != nil {
			return nil, err
		}

		// If still no data after refill
		if p.writePos == 0 {
			return nil, nil
		}
	}

	// Return next item from buffer
	rel := p.buffer[p.readPos]
	p.readPos++
	return rel, nil
}

// Reset resets the reader to start from beginning of source
func (p *pipeReader) Reset() {
	// Can't actually reset the original source, so this is a no-op
	// This means we need to perform the pipe operations in the right order
}

// tupleTableAdapter for writing to the tuple table
type tupleTableAdapter struct {
	pipe         *pipeReader
	current      *tuple.Relationship
	err          error
	valuesBuffer []any
	colNames     []string
}

// Next for tupleTableAdapter
func (t *tupleTableAdapter) Next() bool {
	t.current, t.err = t.pipe.Next()
	return t.current != nil
}

// Values for tupleTableAdapter
func (t *tupleTableAdapter) Values() ([]any, error) {
	var caveatName string
	var caveatContext map[string]any
	if t.current.OptionalCaveat != nil {
		caveatName = t.current.OptionalCaveat.CaveatName
		caveatContext = t.current.OptionalCaveat.Context.AsMap()
	}

	t.valuesBuffer[0] = t.current.Resource.ObjectType
	t.valuesBuffer[1] = t.current.Resource.ObjectID
	t.valuesBuffer[2] = t.current.Resource.Relation
	t.valuesBuffer[3] = t.current.Subject.ObjectType
	t.valuesBuffer[4] = t.current.Subject.ObjectID
	t.valuesBuffer[5] = t.current.Subject.Relation
	t.valuesBuffer[6] = caveatName
	t.valuesBuffer[7] = caveatContext
	t.valuesBuffer[8] = t.current.OptionalExpiration

	if len(t.colNames) > 9 && t.current.OptionalIntegrity != nil {
		t.valuesBuffer[9] = t.current.OptionalIntegrity.KeyId
		t.valuesBuffer[10] = t.current.OptionalIntegrity.Hash
		t.valuesBuffer[11] = t.current.OptionalIntegrity.HashedAt.AsTime()
	}

	return t.valuesBuffer, nil
}

// Err for tupleTableAdapter
func (t *tupleTableAdapter) Err() error {
	return t.err
}

// resourceDataAdapter for writing resource objects to the object_data table
type resourceDataAdapter struct {
	pipe         *pipeReader
	current      *tuple.Relationship
	err          error
	valuesBuffer []any
	colNames     []string
}

func (r *resourceDataAdapter) Next() bool {
	for {
		r.current, r.err = r.pipe.Next()
		if r.current == nil || r.err != nil {
			return false
		}
		// Only process rows that have ObjectData
		if r.current.Resource.ObjectData != nil {
			return true
		}
	}
}

func (r *resourceDataAdapter) Values() ([]any, error) {
	if r.current.Resource.ObjectData == nil {
		return nil, fmt.Errorf("resource object data is nil")
	}
	r.valuesBuffer[0] = r.current.Resource.ObjectType
	r.valuesBuffer[1] = r.current.Resource.ObjectID
	r.valuesBuffer[2] = r.current.Resource.ObjectData
	return r.valuesBuffer, nil
}

func (r *resourceDataAdapter) Err() error {
	return r.err
}

// objectDataAdapter for writing to the object_data table
type subjectDataAdapter struct {
	pipe         *pipeReader
	current      *tuple.Relationship
	err          error
	valuesBuffer []any
	colNames     []string
}

func (s *subjectDataAdapter) Next() bool {
	for {
		s.current, s.err = s.pipe.Next()
		if s.current == nil || s.err != nil {
			return false
		}
		// Only process rows that have ObjectData
		if s.current.Subject.ObjectData != nil {
			return true
		}
	}
}

func (s *subjectDataAdapter) Values() ([]any, error) {
	if s.current.Subject.ObjectData == nil {
		return nil, fmt.Errorf("subject object data is nil")
	}
	s.valuesBuffer[0] = s.current.Subject.ObjectType
	s.valuesBuffer[1] = s.current.Subject.ObjectID
	s.valuesBuffer[2] = s.current.Subject.ObjectData
	return s.valuesBuffer, nil
}

func (s *subjectDataAdapter) Err() error {
	return s.err
}

// copySourceFactory creates a new copy source from the original iterator
type copySourceFactory struct {
	ctx        context.Context
	source     datastore.BulkWriteRelationshipSource
	bufferSize int
}

func newCopySourceFactory(ctx context.Context, source datastore.BulkWriteRelationshipSource, bufferSize int) *copySourceFactory {
	return &copySourceFactory{
		ctx:        ctx,
		source:     source,
		bufferSize: bufferSize,
	}
}

// createPipeReader creates a new pipe reader that reads from the original source
func (f *copySourceFactory) createPipeReader() *pipeReader {
	return newPipeReader(f.ctx, f.source, f.bufferSize)
}

// createTupleAdapter creates a new adapter for the tuple table
func (f *copySourceFactory) createTupleAdapter(pipe *pipeReader, colNames []string) *tupleTableAdapter {
	return &tupleTableAdapter{
		pipe:         pipe,
		valuesBuffer: make([]any, len(colNames)),
		colNames:     colNames,
	}
}

func (f *copySourceFactory) createResourceDataAdapter(pipe *pipeReader, colNames []string) *resourceDataAdapter {
	return &resourceDataAdapter{
		pipe:         pipe,
		valuesBuffer: make([]any, len(colNames)),
		colNames:     colNames,
	}
}

func (f *copySourceFactory) createSubjetcDataAdapter(pipe *pipeReader, colNames []string) *subjectDataAdapter {
	return &subjectDataAdapter{
		pipe:         pipe,
		valuesBuffer: make([]any, len(colNames)),
		colNames:     colNames,
	}
}

// BulkLoadSequential loads data into both tables sequentially
// It uses a chunked approach to avoid keeping all data in memory
func BulkLoadWithObjectData(
	ctx context.Context,
	tx pgx.Tx,
	tupleTableName string,
	tupleColNames []string,
	objectDataTableName string,
	objectDataColNames []string,
	iter datastore.BulkWriteRelationshipSource,
) (uint64, error) {
	// Process in chunks to avoid memory issues with large datasets
	var totalCopied int64
	var totalCopiedUint uint64

	for {
		// Create a limited source for this chunk
		chunkSource := newLimitedSource(ctx, iter, chunkSize)

		// Skip if no data remains
		if chunkSource.isEmpty {
			break
		}

		// Create factory for this chunk
		factory := newCopySourceFactory(ctx, chunkSource, chunkSize)

		// First pass: Copy to tuple table
		pipeTuples := factory.createPipeReader()
		tupleAdapter := factory.createTupleAdapter(pipeTuples, tupleColNames)

		tuplesCopied, err := tx.CopyFrom(ctx, pgx.Identifier{tupleTableName}, tupleColNames, tupleAdapter)
		if err != nil {
			return totalCopiedUint, err
		}

		// Reset to beginning of chunk
		chunkSource.Reset()

		// Second pass: Copy resources to object_data table
		pipeResources := factory.createPipeReader()
		resourcesAdapter := factory.createResourceDataAdapter(pipeResources, objectDataColNames)

		_, err = tx.CopyFrom(ctx, pgx.Identifier{objectDataTableName}, objectDataColNames, resourcesAdapter)
		if err != nil {
			return totalCopiedUint, err
		}
		// Reset to beginning of chunk
		chunkSource.Reset()

		// Third pass: Copy subjects to object_data table
		pipeSubjects := factory.createPipeReader()
		subjectsAdapter := factory.createSubjetcDataAdapter(pipeSubjects, objectDataColNames)

		_, err = tx.CopyFrom(ctx, pgx.Identifier{objectDataTableName}, objectDataColNames, subjectsAdapter)
		if err != nil {
			return totalCopiedUint, err
		}

		totalCopied += tuplesCopied

		// Break if this was the last chunk
		if chunkSource.isLast {
			break
		}
	}

	totalCopiedUint, err := safecast.ToUint64(totalCopied)
	if err != nil {
		return totalCopiedUint, spiceerrors.MustBugf("number copied was negative: %v", err)
	}

	return totalCopiedUint, nil
}

// limitedSource is a wrapper that limits the number of items read from the source
type limitedSource struct {
	source    datastore.BulkWriteRelationshipSource
	ctx       context.Context
	limit     int
	count     int
	buffer    []*tuple.Relationship
	bufferPos int
	isLast    bool // True if this is the last chunk
	isEmpty   bool // True if no data was available
	readPhase bool // False during first pass, true during second pass
}

func newLimitedSource(ctx context.Context, source datastore.BulkWriteRelationshipSource, limit int) *limitedSource {
	return &limitedSource{
		source: source,
		ctx:    ctx,
		limit:  limit,
		buffer: make([]*tuple.Relationship, 0, limit),
	}
}

// Next returns the next relationship up to the limit
func (l *limitedSource) Next(ctx context.Context) (*tuple.Relationship, error) {
	if l.readPhase {
		// Second pass: read from buffer
		if l.bufferPos >= len(l.buffer) {
			return nil, nil
		}
		rel := l.buffer[l.bufferPos]
		l.bufferPos++
		return rel, nil
	} else {
		// First pass: read from source and buffer
		if l.count >= l.limit {
			l.isLast = false
			return nil, nil
		}

		rel, err := l.source.Next(ctx)
		if err != nil {
			return nil, err
		}

		if rel == nil {
			l.isLast = true
			if l.count == 0 {
				l.isEmpty = true
			}
			return nil, nil
		}

		l.buffer = append(l.buffer, rel)
		l.count++
		return rel, nil
	}
}

// Reset resets to read from the buffer
func (l *limitedSource) Reset() {
	l.readPhase = true
	l.bufferPos = 0
}
