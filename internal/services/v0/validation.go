package v0

import (
	"context"
	"fmt"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/internal/namespace"
)

type invalidRelationError struct {
	error
	subject *v0.User
	onr     *v0.ObjectAndRelation
}

func validateTupleWrite(
	ctx context.Context,
	tpl *v0.RelationTuple,
	nsm namespace.Manager,
	revision decimal.Decimal,
) error {
	if err := nsm.CheckNamespaceAndRelation(
		ctx,
		tpl.ObjectAndRelation.Namespace,
		tpl.ObjectAndRelation.Relation,
		false, // Disallow ellipsis
		revision,
	); err != nil {
		return err
	}

	if err := nsm.CheckNamespaceAndRelation(
		ctx,
		tpl.User.GetUserset().Namespace,
		tpl.User.GetUserset().Relation,
		true, // Allow Ellipsis
		revision,
	); err != nil {
		return err
	}

	_, ts, err := nsm.ReadNamespaceAndTypes(ctx, tpl.ObjectAndRelation.Namespace, revision)
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

	return nil
}
