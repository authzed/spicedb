package query

import "github.com/authzed/spicedb/pkg/spiceerrors"

type Hint func(it Iterator) error

func ArrowDirectionHint(direction arrowDirection) Hint {
	return func(it Iterator) error {
		if v, ok := it.(*ArrowIterator); ok {
			v.direction = direction
			return nil
		}
		return spiceerrors.MustBugf("ArrowDirectionHint: being applied to non-ArrowIterator, got `%T`", it)
	}
}
