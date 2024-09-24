package common

import (
	"errors"
	"regexp"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	pgUniqueConstraintViolation = "23505"
	pgSerializationFailure      = "40001"
	pgTransactionAborted        = "25P02"
	pgReadOnlyTransaction       = "25006"
)

var (
	createConflictDetailsRegex              = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
	createConflictDetailsRegexWithoutCaveat = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
)

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

// ConvertToWriteConstraintError converts the given Postgres error into a CreateRelationshipExistsError
// if applicable. If not applicable, returns nils.
func ConvertToWriteConstraintError(livingTupleConstraints []string, err error) error {
	var pgerr *pgconn.PgError

	if errors.As(err, &pgerr) && pgerr.Code == pgUniqueConstraintViolation && slices.Contains(livingTupleConstraints, pgerr.ConstraintName) {
		found := createConflictDetailsRegex.FindStringSubmatch(pgerr.Detail)
		if found != nil {
			return dscommon.NewCreateRelationshipExistsError(&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: strings.TrimSpace(found[2]),
					ObjectId:  strings.TrimSpace(found[3]),
					Relation:  strings.TrimSpace(found[4]),
				},
				Subject: &core.ObjectAndRelation{
					Namespace: strings.TrimSpace(found[5]),
					ObjectId:  strings.TrimSpace(found[6]),
					Relation:  strings.TrimSpace(found[7]),
				},
			})
		}

		found = createConflictDetailsRegexWithoutCaveat.FindStringSubmatch(pgerr.Detail)
		if found != nil {
			return dscommon.NewCreateRelationshipExistsError(&core.RelationTuple{
				ResourceAndRelation: &core.ObjectAndRelation{
					Namespace: strings.TrimSpace(found[2]),
					ObjectId:  strings.TrimSpace(found[3]),
					Relation:  strings.TrimSpace(found[4]),
				},
				Subject: &core.ObjectAndRelation{
					Namespace: strings.TrimSpace(found[5]),
					ObjectId:  strings.TrimSpace(found[6]),
					Relation:  strings.TrimSpace(found[7]),
				},
			})
		}

		return dscommon.NewCreateRelationshipExistsError(nil)
	}

	return nil
}
