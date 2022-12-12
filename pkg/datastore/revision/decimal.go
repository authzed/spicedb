package revision

import (
	"github.com/shopspring/decimal"

	"github.com/authzed/spicedb/pkg/datastore"
)

type Decimal struct {
	decimal.Decimal
}

var NoRevision Decimal

func NewFromDecimal(d decimal.Decimal) Decimal {
	return Decimal{d}
}

func (d Decimal) Equal(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		return false
	}

	rhsD := rhs.(Decimal)

	return d.Decimal.Equal(rhsD.Decimal)
}

func (d Decimal) GreaterThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = Decimal{decimal.Zero}
	}

	rhsD := rhs.(Decimal)

	return d.Decimal.GreaterThan(rhsD.Decimal)
}

func (d Decimal) LessThan(rhs datastore.Revision) bool {
	if rhs == datastore.NoRevision {
		rhs = Decimal{decimal.Zero}
	}

	rhsD := rhs.(Decimal)

	return d.Decimal.LessThan(rhsD.Decimal)
}

var _ datastore.Revision = Decimal{}

type DecimalDecoder struct{}

func (DecimalDecoder) RevisionFromString(s string) (datastore.Revision, error) {
	parsed, err := decimal.NewFromString(s)
	if err != nil {
		return datastore.NoRevision, err
	}
	return Decimal{parsed}, nil
}

// DecimalKeyFunc is used to convert a simple Decimal to an int64 for use in maps.
func DecimalKeyFunc(r Decimal) int64 {
	return r.IntPart()
}

// DecimalKeyLessThanFunc is used to compare keys created by the DecimalKeyFunc.
func DecimalKeyLessThanFunc(lhs, rhs int64) bool {
	return lhs < rhs
}
