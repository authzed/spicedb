package common

import (
	"context"
	"database/sql"

	"github.com/jackc/pgx/v5"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

type tupleSourceAdapter struct {
	source datastore.BulkWriteRelationshipSource
	ctx    context.Context

	current *core.RelationTuple
	err     error
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
	var caveatName sql.NullString
	var caveatContext map[string]any
	if tg.current.Caveat != nil {
		caveatName.String = tg.current.Caveat.CaveatName
		caveatName.Valid = true
		caveatContext = tg.current.Caveat.Context.AsMap()
	}

	return []any{
		tg.current.ResourceAndRelation.Namespace,
		tg.current.ResourceAndRelation.ObjectId,
		tg.current.ResourceAndRelation.Relation,
		tg.current.Subject.Namespace,
		tg.current.Subject.ObjectId,
		tg.current.Subject.Relation,
		caveatName,
		caveatContext,
	}, nil
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
		source: iter,
		ctx:    ctx,
	}
	copied, err := tx.CopyFrom(ctx, pgx.Identifier{tupleTableName}, colNames, adapter)
	return uint64(copied), err
}
