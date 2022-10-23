package revisions

import (
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

type DecimalRevision struct {
	decimal.Decimal
}

var NoRevision DecimalRevision

func NewFromDecimal(d decimal.Decimal) DecimalRevision {
	return DecimalRevision{d}
}

func (d DecimalRevision) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		return false
	}

	rhsD := rhs.(DecimalRevision)

	return d.Decimal.Equal(rhsD.Decimal)
}

func (d DecimalRevision) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = DecimalRevision{decimal.Zero}
	}

	rhsD := rhs.(DecimalRevision)

	return d.Decimal.GreaterThan(rhsD.Decimal)
}

func (d DecimalRevision) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = DecimalRevision{decimal.Zero}
	}

	rhsD := rhs.(DecimalRevision)

	return d.Decimal.LessThan(rhsD.Decimal)
}

var _ datastore.Revision = DecimalRevision{}

type DecimalDecoder struct{}

func (DecimalDecoder) RevisionFromString(s string) (datastore.Revision, error) {
	parsed, err := decimal.NewFromString(s)
	if err != nil {
		return datastore.NoRevision, err
	}
	return DecimalRevision{parsed}, nil
}
