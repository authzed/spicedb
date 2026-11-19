package query

import (
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/spiceerrors"
)

// Hint is a typed, inspectable planning recommendation.
type Hint = planhint.Hint

// ArrowDirectionHint creates an unbound typed arrow recommendation for a legacy advisor.
func ArrowDirectionHint(direction arrowDirection) Hint {
	typed := planhint.InvalidDirection

	switch direction {
	case leftToRight:
		typed = planhint.LeftToRight
	case rightToLeft:
		typed = planhint.RightToLeft
	}

	return Hint{Producer: "legacy-advisor", Payload: planhint.ArrowDirection{Direction: typed}}
}

func applyHint(it Iterator, h Hint) error {
	switch payload := h.Payload.(type) {
	case planhint.ArrowDirection:
		if err := payload.Validate(); err != nil {
			return err
		}

		if arrow, ok := it.(*ArrowIterator); ok {
			arrow.direction = arrowDirection(payload.Direction)
			return nil
		}

		return spiceerrors.MustBugf("arrow direction hint applied to %T", it)
	default:
		return spiceerrors.MustBugf("unsupported execution hint %T", payload)
	}
}
