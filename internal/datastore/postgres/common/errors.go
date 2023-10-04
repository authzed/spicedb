package common

import (
	"errors"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	dscommon "github.com/authzed/spicedb/internal/datastore/common"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	pgUniqueConstraintViolation = "23505"
	pgSerializationFailure      = "40001"
	pgTransactionAborted        = "25P02"
)

var (
	createConflictDetailsRegex              = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
	createConflictDetailsRegexWithoutCaveat = regexp.MustCompile(`^Key (.+)=\(([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)\) already exists`)
)

// ConvertToWriteConstraintError converts the given Postgres error into a CreateRelationshipExistsError
// if applicable. If not applicable, returns nils.
func ConvertToWriteConstraintError(livingTupleConstraint string, err error) error {
	var pgerr *pgconn.PgError
	if errors.As(err, &pgerr) && pgerr.Code == pgUniqueConstraintViolation && pgerr.ConstraintName == livingTupleConstraint {
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
