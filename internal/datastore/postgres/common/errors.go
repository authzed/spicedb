package common

import (
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	pgUniqueConstraintViolation = "23505"
	pgSerializationFailure      = "40001"
	pgTransactionAborted        = "25P02"
	pgReadOnlyTransaction       = "25006"
	pgQueryCanceled             = "57014"
	pgInvalidArgument           = "22023"
)

var (
	createConflictDetailsRegex              = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
	createConflictDetailsRegexWithoutCaveat = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
)

// IsQueryCanceledError returns true if the error is a Postgres error indicating a query was canceled.
func IsQueryCanceledError(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == pgQueryCanceled
}

// IsConstraintFailureError returns true if the error is a Postgres error indicating a constraint
// failure.
func IsConstraintFailureError(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == pgUniqueConstraintViolation
}

// IsReadOnlyTransactionError returns true if the error is a Postgres error indicating a read-only
// transaction.
func IsReadOnlyTransactionError(err error) bool {
	var pgerr *pgconn.PgError
	return errors.As(err, &pgerr) && pgerr.Code == pgReadOnlyTransaction
}

// IsSerializationFailureError returns true if the error is a Postgres error indicating a
// revision replication error.
func IsReplicationLagError(err error) bool {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) {
		return (pgerr.Code == pgInvalidArgument && strings.Contains(pgerr.Message, "is in the future")) ||
			strings.Contains(pgerr.Message, "replica missing revision") ||
			IsSerializationError(err)
	}

	return false
}

// ConvertToWriteConstraintError converts the given Postgres error into a CreateRelationshipExistsError
// if applicable. If not applicable, returns nils.
func ConvertToWriteConstraintError(livingTupleConstraints []string, err error) error {
	var pgerr *pgconn.PgError

	if errors.As(err, &pgerr) && pgerr.Code == pgUniqueConstraintViolation && slices.Contains(livingTupleConstraints, pgerr.ConstraintName) {
		found := createConflictDetailsRegex.FindStringSubmatch(pgerr.Detail)
		if found != nil {
			return dscommon.NewCreateRelationshipExistsError(&tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: strings.TrimSpace(found[2]),
						ObjectID:   strings.TrimSpace(found[3]),
						Relation:   strings.TrimSpace(found[4]),
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: strings.TrimSpace(found[5]),
						ObjectID:   strings.TrimSpace(found[6]),
						Relation:   strings.TrimSpace(found[7]),
					},
				},
			})
		}

		found = createConflictDetailsRegexWithoutCaveat.FindStringSubmatch(pgerr.Detail)
		if found != nil {
			return dscommon.NewCreateRelationshipExistsError(&tuple.Relationship{
				RelationshipReference: tuple.RelationshipReference{
					Resource: tuple.ObjectAndRelation{
						ObjectType: strings.TrimSpace(found[2]),
						ObjectID:   strings.TrimSpace(found[3]),
						Relation:   strings.TrimSpace(found[4]),
					},
					Subject: tuple.ObjectAndRelation{
						ObjectType: strings.TrimSpace(found[5]),
						ObjectID:   strings.TrimSpace(found[6]),
						Relation:   strings.TrimSpace(found[7]),
					},
				},
			})
		}

		return dscommon.NewCreateRelationshipExistsError(nil)
	}

	return nil
}
