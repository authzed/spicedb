package v0

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"

	"github.com/authzed/spicedb/internal/namespace"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/tuple"
)

type invalidRelationError struct {
	error
	subject *v0.User
	onr     *v0.ObjectAndRelation
}

func validateTupleWrite(
	ctx context.Context,
	tpl *v0.RelationTuple,
	ds datastore.Reader,
) error {
	err := tuple.ValidateResourceID(tpl.ObjectAndRelation.ObjectId)
	if err != nil {
		return err
	}

	err = tuple.ValidateSubjectID(tpl.User.GetUserset().ObjectId)
	if err != nil {
		return err
	}

	if err := namespace.CheckNamespaceAndRelation(
		ctx,
		tpl.ObjectAndRelation.Namespace,
		tpl.ObjectAndRelation.Relation,
		false, // Disallow ellipsis
		ds,
	); err != nil {
		return err
	}

	// Ensure wildcard writes have no subject relation.
	if tpl.User.GetUserset().ObjectId == tuple.PublicWildcard {
		if tpl.User.GetUserset().Relation != tuple.Ellipsis {
			return invalidRelationError{
				error:   fmt.Errorf("wildcard relationships require a subject relation of `...` on %v", tpl.ObjectAndRelation),
				subject: tpl.User,
				onr:     tpl.ObjectAndRelation,
			}
		}
	}

	if err := namespace.CheckNamespaceAndRelation(
		ctx,
		tpl.User.GetUserset().Namespace,
		tpl.User.GetUserset().Relation,
		true, // Allow Ellipsis
		ds,
	); err != nil {
		return err
	}

	_, ts, err := namespace.ReadNamespaceAndTypes(ctx, tpl.ObjectAndRelation.Namespace, ds)
	if err != nil {
		return err
	}

	if ts.IsPermission(tpl.ObjectAndRelation.Relation) {
		return invalidRelationError{
			error:   fmt.Errorf("cannot write a relationship to permission %s", tpl.ObjectAndRelation),
			subject: tpl.User,
			onr:     tpl.ObjectAndRelation,
		}
	}

	if tpl.User.GetUserset().ObjectId == tuple.PublicWildcard {
		isAllowed, err := ts.IsAllowedPublicNamespace(
			tpl.ObjectAndRelation.Relation,
			tpl.User.GetUserset().Namespace)
		if err != nil {
			return err
		}

		if isAllowed != namespace.PublicSubjectAllowed {
			return invalidRelationError{
				error:   fmt.Errorf("wildcard subjects of type %s are not allowed on %v", tpl.User.GetUserset().Namespace, tpl.ObjectAndRelation),
				subject: tpl.User,
				onr:     tpl.ObjectAndRelation,
			}
		}
	} else {
		isAllowed, err := ts.IsAllowedDirectRelation(
			tpl.ObjectAndRelation.Relation,
			tpl.User.GetUserset().Namespace,
			tpl.User.GetUserset().Relation)
		if err != nil {
			return err
		}

		if isAllowed == namespace.DirectRelationNotValid {
			return invalidRelationError{
				error:   fmt.Errorf("relation/permission %v is not allowed as the subject of %v", tpl.User, tpl.ObjectAndRelation),
				subject: tpl.User,
				onr:     tpl.ObjectAndRelation,
			}
		}
	}

	return nil
}
