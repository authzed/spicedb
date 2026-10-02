package embedded

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestClosedRelationshipTransaction(t *testing.T) {
	session := &RelationshipTransaction{}
	require.NoError(t, session.check())
	session.closed.Store(true)

	_, err := session.ReadSchema(t.Context())
	require.ErrorIs(t, err, ErrSessionClosed)

	written, err := session.WriteRelationships(t.Context(), []tuple.RelationshipUpdate{
		tuple.Create(tuple.MustParse("document:doc#reader@user:alice")),
	})
	require.ErrorIs(t, err, ErrSessionClosed)
	require.Equal(t, datastore.NoRevision, written.Revision)

	_, err = session.ReadRelationships(t.Context(), datastore.RelationshipsFilter{OptionalResourceType: "document"})
	require.ErrorIs(t, err, ErrSessionClosed)
}

func TestPendingTransactionResults(t *testing.T) {
	expected := revisions.NewForTransactionID(1)
	pending := &PendingTransaction{common.NewPendingTransaction(
		func(context.Context) (datastore.Revision, error) { return expected, nil },
		func(context.Context) error { return nil },
	)}
	result, err := pending.Commit(t.Context())
	require.NoError(t, err)
	require.Equal(t, expected, result.Revision)

	commitErr := errors.New("commit failed")
	pending = &PendingTransaction{common.NewPendingTransaction(
		func(context.Context) (datastore.Revision, error) { return nil, commitErr },
		func(context.Context) error { return nil },
	)}
	result, err = pending.Commit(t.Context())
	require.ErrorIs(t, err, commitErr)
	require.Equal(t, datastore.NoRevision, result.Revision)

	rollbackErr := errors.New("rollback failed")
	pending = &PendingTransaction{common.NewPendingTransaction(
		func(context.Context) (datastore.Revision, error) { return nil, nil },
		func(context.Context) error { return rollbackErr },
	)}
	require.ErrorIs(t, pending.Rollback(t.Context()), rollbackErr)
}
