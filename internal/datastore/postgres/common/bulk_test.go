package common

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

// fakeBulkSource is a BulkWriteRelationshipSource: it yields the given relationships,
// then (nil, nil), unless err is set, in which case it returns (nil, err) immediately.
type fakeBulkSource struct {
	rels []tuple.Relationship
	err  error
	idx  int
}

func (f *fakeBulkSource) Next(_ context.Context) (*tuple.Relationship, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.idx >= len(f.rels) {
		return nil, nil
	}
	r := f.rels[f.idx]
	f.idx++
	return &r, nil
}

// fakeCopyTx is a pgx.Tx whose CopyFrom drains the source (like pgx does) and then
// reports the given error, simulating the driver's opaque COPY-failure wrapper.
type fakeCopyTx struct {
	pgx.Tx
	returnErr error
}

func (f fakeCopyTx) CopyFrom(_ context.Context, _ pgx.Identifier, _ []string, rowSrc pgx.CopyFromSource) (int64, error) {
	for rowSrc.Next() {
		_, _ = rowSrc.Values()
	}
	_ = rowSrc.Err()
	return 0, f.returnErr
}

func TestBulkLoadReturnsSourceErrorNotCopyWrapper(t *testing.T) {
	sourceErr := errors.New("meaningful source failure")
	copyWrapper := errors.New("COPY from stdin failed: inner (SQLSTATE 57014)")

	_, err := BulkLoad(
		t.Context(),
		fakeCopyTx{returnErr: copyWrapper},
		"relation_tuple",
		make([]string, 9),
		&fakeBulkSource{err: sourceErr},
	)

	require.ErrorIs(t, err, sourceErr)
	require.NotErrorIs(t, err, copyWrapper)
}

func TestBulkLoadReturnsCopyErrorWhenSourceDidNotError(t *testing.T) {
	copyErr := errors.New("duplicate key (SQLSTATE 23505)")

	_, err := BulkLoad(
		t.Context(),
		fakeCopyTx{returnErr: copyErr},
		"relation_tuple",
		make([]string, 9),
		&fakeBulkSource{}, // no rows, no error -> adapter.err stays nil
	)

	require.ErrorIs(t, err, copyErr)
}

var _ datastore.BulkWriteRelationshipSource = (*fakeBulkSource)(nil)
