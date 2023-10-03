package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	sq "github.com/Masterminds/squirrel"
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/go-sql-driver/mysql"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"

	"github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/spiceerrors"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	errUnableToWriteRelationships     = "unable to write relationships: %w"
	errUnableToBulkWriteRelationships = "unable to bulk write relationships: %w"
	errUnableToDeleteRelationships    = "unable to delete relationships: %w"
	errUnableToWriteConfig            = "unable to write namespace config: %w"
	errUnableToDeleteConfig           = "unable to delete namespace config: %w"

	bulkInsertRowsLimit = 1_000
)

var (
	duplicateEntryRegex          = regexp.MustCompile(`^Duplicate entry '(.+)' for key 'uq_relation_tuple_living'$`)
	duplicateEntryFullIndexRegex = regexp.MustCompile(`^Duplicate entry '(.+)' for key 'relation_tuple.uq_relation_tuple_living'$`)
)

type mysqlReadWriteTXN struct {
	*mysqlReader

	tupleTableName string
	tx             *sql.Tx
	newTxnID       uint64
}

// caveatContextWrapper is used to marshall maps into MySQLs JSON data type
type caveatContextWrapper map[string]any

func (cc *caveatContextWrapper) Scan(val any) error {
	v, ok := val.([]byte)
	if !ok {
		return fmt.Errorf("unsupported type: %T", v)
	}
	return json.Unmarshal(v, &cc)
}

func (cc *caveatContextWrapper) Value() (driver.Value, error) {
	return json.Marshal(&cc)
}

// WriteRelationships takes a list of existing relationships that must exist, and a list of
// tuple mutations and applies it to the datastore for the specified namespace.
func (rwt *mysqlReadWriteTXN) WriteRelationships(ctx context.Context, mutations []*core.RelationTupleUpdate) error {
	// TODO(jschorr): Determine if we can do this in a more efficient manner using ON CONFLICT UPDATE
	// rather than SELECT FOR UPDATE as we've been doing.
	//
	// NOTE: There are some fundamental changes introduced to prevent a deadlock in MySQL vs the initial
	// Postgres implementation from which this was copied.

	bulkWrite := rwt.WriteTupleQuery
	bulkWriteHasValues := false

	selectForUpdateQuery := rwt.QueryTuplesWithIdsQuery

	clauses := sq.Or{}
	createAndTouchMutationsByTuple := make(map[string]*core.RelationTupleUpdate, len(mutations))

	// Collect all TOUCH and DELETE operations. CREATE is handled below.
	for _, mut := range mutations {
		tpl := mut.Tuple
		tplString := tuple.StringWithoutCaveat(tpl)

		switch mut.Operation {
		case core.RelationTupleUpdate_CREATE:
			createAndTouchMutationsByTuple[tplString] = mut

		case core.RelationTupleUpdate_TOUCH:
			createAndTouchMutationsByTuple[tplString] = mut
			clauses = append(clauses, exactRelationshipClause(tpl))

		case core.RelationTupleUpdate_DELETE:
			clauses = append(clauses, exactRelationshipClause(tpl))

		default:
			return spiceerrors.MustBugf("unknown mutation operation")
		}
	}

	if len(clauses) > 0 {
		query, args, err := selectForUpdateQuery.
			Where(clauses).
			Where(sq.GtOrEq{colDeletedTxn: rwt.newTxnID}).
			ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		rows, err := rwt.tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
		defer common.LogOnError(ctx, rows.Close)

		foundTpl := &core.RelationTuple{
			ResourceAndRelation: &core.ObjectAndRelation{},
			Subject:             &core.ObjectAndRelation{},
		}

		var caveatName string
		var caveatContext caveatContextWrapper

		tupleIdsToDelete := make([]int64, 0, len(clauses))
		for rows.Next() {
			var tupleID int64
			if err := rows.Scan(
				&tupleID,
				&foundTpl.ResourceAndRelation.Namespace,
				&foundTpl.ResourceAndRelation.ObjectId,
				&foundTpl.ResourceAndRelation.Relation,
				&foundTpl.Subject.Namespace,
				&foundTpl.Subject.ObjectId,
				&foundTpl.Subject.Relation,
				&caveatName,
				&caveatContext,
			); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}

			// if the relationship to be deleted is for a TOUCH operation and the caveat
			// name or context has not changed, then remove it from delete and create.
			tplString := tuple.StringWithoutCaveat(foundTpl)
			if mut, ok := createAndTouchMutationsByTuple[tplString]; ok {
				foundTpl.Caveat, err = common.ContextualizedCaveatFrom(caveatName, caveatContext)
				if err != nil {
					return fmt.Errorf(errUnableToQueryTuples, err)
				}

				// Ensure the tuples are the same.
				if tuple.Equal(mut.Tuple, foundTpl) {
					delete(createAndTouchMutationsByTuple, tplString)
					continue
				}
			}

			tupleIdsToDelete = append(tupleIdsToDelete, tupleID)
		}

		if rows.Err() != nil {
			return fmt.Errorf(errUnableToWriteRelationships, rows.Err())
		}

		if len(tupleIdsToDelete) > 0 {
			query, args, err := rwt.
				DeleteTupleQuery.
				Where(sq.Eq{colID: tupleIdsToDelete}).
				Set(colDeletedTxn, rwt.newTxnID).
				ToSql()
			if err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
			if _, err := rwt.tx.ExecContext(ctx, query, args...); err != nil {
				return fmt.Errorf(errUnableToWriteRelationships, err)
			}
		}
	}

	for _, mut := range createAndTouchMutationsByTuple {
		tpl := mut.Tuple

		var caveatName string
		var caveatContext caveatContextWrapper
		if tpl.Caveat != nil {
			caveatName = tpl.Caveat.CaveatName
			caveatContext = tpl.Caveat.Context.AsMap()
		}
		bulkWrite = bulkWrite.Values(
			tpl.ResourceAndRelation.Namespace,
			tpl.ResourceAndRelation.ObjectId,
			tpl.ResourceAndRelation.Relation,
			tpl.Subject.Namespace,
			tpl.Subject.ObjectId,
			tpl.Subject.Relation,
			caveatName,
			&caveatContext,
			rwt.newTxnID,
		)
		bulkWriteHasValues = true
	}

	if bulkWriteHasValues {
		query, args, err := bulkWrite.ToSql()
		if err != nil {
			return fmt.Errorf(errUnableToWriteRelationships, err)
		}

		_, err = rwt.tx.ExecContext(ctx, query, args...)
		if err != nil {
			if cerr := convertToWriteConstraintError(err); cerr != nil {
				return cerr
			}

			return fmt.Errorf(errUnableToWriteRelationships, err)
		}
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteRelationships(ctx context.Context, filter *v1.RelationshipFilter) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
	// Add clauses for the ResourceFilter
	query := rwt.DeleteTupleQuery.Where(sq.Eq{colNamespace: filter.ResourceType})
	if filter.OptionalResourceId != "" {
		query = query.Where(sq.Eq{colObjectID: filter.OptionalResourceId})
	}
	if filter.OptionalRelation != "" {
		query = query.Where(sq.Eq{colRelation: filter.OptionalRelation})
	}

	// Add clauses for the SubjectFilter
	if subjectFilter := filter.OptionalSubjectFilter; subjectFilter != nil {
		query = query.Where(sq.Eq{colUsersetNamespace: subjectFilter.SubjectType})
		if subjectFilter.OptionalSubjectId != "" {
			query = query.Where(sq.Eq{colUsersetObjectID: subjectFilter.OptionalSubjectId})
		}
		if relationFilter := subjectFilter.OptionalRelation; relationFilter != nil {
			query = query.Where(sq.Eq{colUsersetRelation: stringz.DefaultEmpty(relationFilter.Relation, datastore.Ellipsis)})
		}
	}

	query = query.Set(colDeletedTxn, rwt.newTxnID)

	querySQL, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	if _, err := rwt.tx.ExecContext(ctx, querySQL, args...); err != nil {
		return fmt.Errorf(errUnableToDeleteRelationships, err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) WriteNamespaces(ctx context.Context, newNamespaces ...*core.NamespaceDefinition) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor

	deletedNamespaceClause := sq.Or{}
	writeQuery := rwt.WriteNamespaceQuery

	for _, newNamespace := range newNamespaces {
		serialized, err := proto.Marshal(newNamespace)
		if err != nil {
			return fmt.Errorf(errUnableToWriteConfig, err)
		}

		deletedNamespaceClause = append(deletedNamespaceClause, sq.Eq{colNamespace: newNamespace.Name})
		writeQuery = writeQuery.Values(newNamespace.Name, serialized, rwt.newTxnID)
	}

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.And{sq.Eq{colDeletedTxn: liveDeletedTxnID}, deletedNamespaceClause}).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	query, args, err := writeQuery.ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf(errUnableToWriteConfig, err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) DeleteNamespaces(ctx context.Context, nsNames ...string) error {
	// TODO (@vroldanbet) dupe from postgres datastore - need to refactor

	// For each namespace, check they exist and collect predicates for the
	// "WHERE" clause to delete the namespaces and associated tuples.
	nsClauses := make([]sq.Sqlizer, 0, len(nsNames))
	tplClauses := make([]sq.Sqlizer, 0, len(nsNames))
	for _, nsName := range nsNames {
		// TODO(jzelinskie): check these in one query
		baseQuery := rwt.ReadNamespaceQuery.Where(sq.Eq{colDeletedTxn: liveDeletedTxnID})
		_, createdAt, err := loadNamespace(ctx, nsName, rwt.tx, baseQuery)
		switch {
		case errors.As(err, &datastore.ErrNamespaceNotFound{}):
			// TODO(jzelinskie): return the name of the missing namespace
			return err
		case err == nil:
			break
		default:
			return fmt.Errorf(errUnableToDeleteConfig, err)
		}

		nsClauses = append(nsClauses, sq.Eq{colNamespace: nsName, colCreatedTxn: createdAt})
		tplClauses = append(tplClauses, sq.Eq{colNamespace: nsName})
	}

	delSQL, delArgs, err := rwt.DeleteNamespaceQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Or(nsClauses)).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, delSQL, delArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	deleteTupleSQL, deleteTupleArgs, err := rwt.DeleteNamespaceTuplesQuery.
		Set(colDeletedTxn, rwt.newTxnID).
		Where(sq.Or(tplClauses)).
		ToSql()
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	_, err = rwt.tx.ExecContext(ctx, deleteTupleSQL, deleteTupleArgs...)
	if err != nil {
		return fmt.Errorf(errUnableToDeleteConfig, err)
	}

	return nil
}

func (rwt *mysqlReadWriteTXN) BulkLoad(ctx context.Context, iter datastore.BulkWriteRelationshipSource) (uint64, error) {
	var sqlStmt bytes.Buffer

	sql, _, err := rwt.WriteTupleQuery.Values(1, 2, 3, 4, 5, 6, 7, 8, 9).ToSql()
	if err != nil {
		return 0, err
	}

	var numWritten uint64
	var tpl *core.RelationTuple

	// Bootstrap the loop
	tpl, err = iter.Next(ctx)

	for tpl != nil && err == nil {
		sqlStmt.Reset()
		sqlStmt.WriteString(sql)
		var args []interface{}
		var batchLen uint64

		for ; tpl != nil && err == nil && batchLen < bulkInsertRowsLimit; tpl, err = iter.Next(ctx) {
			if batchLen != 0 {
				sqlStmt.WriteString(",(?,?,?,?,?,?,?,?,?)")
			}

			var caveatName string
			var caveatContext caveatContextWrapper
			if tpl.Caveat != nil {
				caveatName = tpl.Caveat.CaveatName
				caveatContext = tpl.Caveat.Context.AsMap()
			}
			args = append(args,
				tpl.ResourceAndRelation.Namespace,
				tpl.ResourceAndRelation.ObjectId,
				tpl.ResourceAndRelation.Relation,
				tpl.Subject.Namespace,
				tpl.Subject.ObjectId,
				tpl.Subject.Relation,
				caveatName,
				&caveatContext,
				rwt.newTxnID,
			)
			batchLen++
		}
		if err != nil {
			return 0, fmt.Errorf(errUnableToBulkWriteRelationships, err)
		}

		if batchLen > 0 {
			log.Warn().Uint64("count", batchLen).Uint64("written", numWritten).Msg("writing batch")
			if _, err := rwt.tx.Exec(sqlStmt.String(), args...); err != nil {
				return 0, fmt.Errorf(errUnableToBulkWriteRelationships, fmt.Errorf("error writing batch: %w", err))
			}
		}

		numWritten += batchLen
	}
	if err != nil {
		return 0, fmt.Errorf(errUnableToBulkWriteRelationships, err)
	}

	return numWritten, nil
}

func convertToWriteConstraintError(err error) error {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) && mysqlErr.Number == errMysqlDuplicateEntry {
		found := duplicateEntryRegex.FindStringSubmatch(mysqlErr.Message)
		if found != nil {
			parts := strings.Split(found[1], "-")
			if len(parts) == 7 {
				return common.NewCreateRelationshipExistsError(&core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: parts[0],
						ObjectId:  parts[1],
						Relation:  parts[2],
					},
					Subject: &core.ObjectAndRelation{
						Namespace: parts[3],
						ObjectId:  parts[4],
						Relation:  parts[5],
					},
				})
			}
		}

		found = duplicateEntryFullIndexRegex.FindStringSubmatch(mysqlErr.Message)
		if found != nil {
			parts := strings.Split(found[1], "-")
			if len(parts) == 7 {
				return common.NewCreateRelationshipExistsError(&core.RelationTuple{
					ResourceAndRelation: &core.ObjectAndRelation{
						Namespace: parts[0],
						ObjectId:  parts[1],
						Relation:  parts[2],
					},
					Subject: &core.ObjectAndRelation{
						Namespace: parts[3],
						ObjectId:  parts[4],
						Relation:  parts[5],
					},
				})
			}
		}

		return common.NewCreateRelationshipExistsError(nil)
	}
	return nil
}

// TODO (@vroldanbet) dupe from postgres datastore - need to refactor
func exactRelationshipClause(r *core.RelationTuple) sq.Eq {
	return sq.Eq{
		colNamespace:        r.ResourceAndRelation.Namespace,
		colObjectID:         r.ResourceAndRelation.ObjectId,
		colRelation:         r.ResourceAndRelation.Relation,
		colUsersetNamespace: r.Subject.Namespace,
		colUsersetObjectID:  r.Subject.ObjectId,
		colUsersetRelation:  r.Subject.Relation,
	}
}

var _ datastore.ReadWriteTransaction = &mysqlReadWriteTXN{}
