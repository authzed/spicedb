package development

import (
	"context"
	"fmt"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type invalidRelationError struct {
	error
	subject *core.ObjectAndRelation
	onr     *core.ObjectAndRelation
}

func validateTupleWrite(
	ctx context.Context,
	tpl *core.RelationTuple,
	ds datastore.Reader,
) error {
	err := tuple.ValidateResourceID(tpl.ResourceAndRelation.ObjectId)
	if err != nil {
		return err
	}

	err = tuple.ValidateSubjectID(tpl.Subject.ObjectId)
	if err != nil {
		return err
	}

	if err := namespace.CheckNamespaceAndRelation(
		ctx,
		tpl.ResourceAndRelation.Namespace,
		tpl.ResourceAndRelation.Relation,
		false, // Disallow ellipsis
		ds,
	); err != nil {
		return err
	}

	// Ensure wildcard writes have no subject relation.
	if tpl.Subject.ObjectId == tuple.PublicWildcard {
		if tpl.Subject.Relation != tuple.Ellipsis {
			return invalidRelationError{
				error:   fmt.Errorf("wildcard relationships require a subject relation of `...` on %v", tpl.ResourceAndRelation),
				subject: tpl.Subject,
				onr:     tpl.ResourceAndRelation,
			}
		}
	}

	if err := namespace.CheckNamespaceAndRelation(
		ctx,
		tpl.Subject.Namespace,
		tpl.Subject.Relation,
		true, // Allow Ellipsis
		ds,
	); err != nil {
		return err
	}

	_, ts, err := namespace.ReadNamespaceAndTypes(ctx, tpl.ResourceAndRelation.Namespace, ds)
	if err != nil {
		return err
	}

	if ts.IsPermission(tpl.ResourceAndRelation.Relation) {
		return invalidRelationError{
			error:   fmt.Errorf("cannot write a relationship to permission %s", tpl.ResourceAndRelation),
			subject: tpl.Subject,
			onr:     tpl.ResourceAndRelation,
		}
	}

	if tpl.Subject.ObjectId == tuple.PublicWildcard {
		isAllowed, err := ts.IsAllowedPublicNamespace(
			tpl.ResourceAndRelation.Relation,
			tpl.Subject.Namespace)
		if err != nil {
			return err
		}

		if isAllowed != namespace.PublicSubjectAllowed {
			return invalidRelationError{
				error:   fmt.Errorf("wildcard subjects of type `%s` are not allowed on `%s`", tpl.Subject.Namespace, tuple.StringONR(tpl.ResourceAndRelation)),
				subject: tpl.Subject,
				onr:     tpl.ResourceAndRelation,
			}
		}
	} else {
		isAllowed, err := ts.IsAllowedDirectRelation(
			tpl.ResourceAndRelation.Relation,
			tpl.Subject.Namespace,
			tpl.Subject.Relation)
		if err != nil {
			return err
		}

		if isAllowed == namespace.DirectRelationNotValid {
			return invalidRelationError{
				error:   fmt.Errorf("relation/permission `%s` is not allowed as the subject of `%s`", tuple.StringONR(tpl.Subject), tuple.StringONR(tpl.ResourceAndRelation)),
				subject: tpl.Subject,
				onr:     tpl.ResourceAndRelation,
			}
		}
	}

	return nil
}
