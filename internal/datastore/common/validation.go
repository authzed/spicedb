package common

import (
	"fmt"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/pkg/tuple"
)

// ValidateUpdatesToWrite performs basic validation on relationship updates going into datastores.
func ValidateUpdatesToWrite(updates []*v1.RelationshipUpdate) error {
	for _, update := range updates {
		err := update.HandwrittenValidate()
		if err != nil {
			return err
		}

		if update.Relationship.Subject.Object.ObjectId == tuple.PublicWildcard && update.Relationship.Subject.OptionalRelation != "" {
			return fmt.Errorf(
				"Attempt to write a wildcard relationship (`%s`) with a non-empty relation. Please report this bug",
				tuple.RelString(update.Relationship),
			)
		}
	}

	return nil
}
