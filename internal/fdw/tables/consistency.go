package tables

import (
	"strings"

	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

const (
	// Consistency mode values
	consistencyMinimizeLatency = "minimize_latency"
	consistencyFullyConsistent = "fully_consistent"
	consistencyExactPrefix     = "@"

	fieldConsistency = "consistency"
)

var consistencyField = wire.Column{
	Table: 0,
	Name:  fieldConsistency,
	Oid:   uint32(oid.T_text),
	Width: 256,
}

func consistencyFromFields(fields fieldMap[string], parameters []wire.Parameter) (*v1.Consistency, error) {
	// If unspecified, default to minimize latency.
	if _, ok := fields[fieldConsistency]; !ok {
		return &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		}, nil
	}

	consistencyFieldValue, err := optionalStringValue(fields[fieldConsistency], parameters)
	if err != nil {
		return nil, err
	}

	switch consistencyFieldValue {
	case consistencyMinimizeLatency:
		return &v1.Consistency{
			Requirement: &v1.Consistency_MinimizeLatency{
				MinimizeLatency: true,
			},
		}, nil

	case consistencyFullyConsistent:
		return &v1.Consistency{
			Requirement: &v1.Consistency_FullyConsistent{
				FullyConsistent: true,
			},
		}, nil

	default:
		isAtExactSnapshot := false
		if after, ok := strings.CutPrefix(consistencyFieldValue, consistencyExactPrefix); ok {
			consistencyFieldValue = after
			isAtExactSnapshot = true
		}

		// Parse as a ZedToken.
		zedToken := &v1.ZedToken{
			Token: consistencyFieldValue,
		}

		if isAtExactSnapshot {
			return &v1.Consistency{
				Requirement: &v1.Consistency_AtExactSnapshot{
					AtExactSnapshot: zedToken,
				},
			}, nil
		} else {
			return &v1.Consistency{
				Requirement: &v1.Consistency_AtLeastAsFresh{
					AtLeastAsFresh: zedToken,
				},
			}, nil
		}
	}
}
