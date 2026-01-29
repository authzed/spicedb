package common

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pbnjay/memory"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

// At startup, measure 75% of available free memory.
var freeMemory uint64

func init() {
	freeMemory = memory.FreeMemory() / 100 * 75
}

// WriteRelationships is a convenience method to perform the same update operation on a set of relationships
func WriteRelationships(ctx context.Context, ds datastore.Datastore, op tuple.UpdateOperation, rels ...tuple.Relationship) (datastore.Revision, error) {
	updates := make([]tuple.RelationshipUpdate, 0, len(rels))
	for _, rel := range rels {
		ru := tuple.RelationshipUpdate{
			Operation:    op,
			Relationship: rel,
		}
		updates = append(updates, ru)
	}
	return UpdateRelationshipsInDatastore(ctx, ds, updates...)
}

// UpdateRelationshipsInDatastore is a convenience method to perform multiple relation update operations on a Datastore
func UpdateRelationshipsInDatastore(ctx context.Context, ds datastore.Datastore, updates ...tuple.RelationshipUpdate) (datastore.Revision, error) {
	return ds.ReadWriteTx(ctx, func(ctx context.Context, rwt datastore.ReadWriteTransaction) error {
		return rwt.WriteRelationships(ctx, updates)
	})
}

// ContextualizedCaveatFrom convenience method that handles creation of a contextualized caveat
// given the possibility of arguments with zero-values.
func ContextualizedCaveatFrom(name string, context map[string]any) (*core.ContextualizedCaveat, error) {
	var caveat *core.ContextualizedCaveat
	if name != "" {
		strct, err := structpb.NewStruct(context)
		if err != nil {
			return nil, fmt.Errorf("malformed caveat context: %w", err)
		}
		caveat = &core.ContextualizedCaveat{
			CaveatName: name,
			Context:    strct,
		}
	}
	return caveat, nil
}

var errOverHundredPercent = errors.New("percentage greater than 100")

func parsePercent(str string, freeMem uint64) (uint64, error) {
	percent := strings.TrimSuffix(str, "%")
	parsedPercent, err := strconv.ParseUint(percent, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse percentage: %w", err)
	}

	if parsedPercent > 100 {
		return 0, errOverHundredPercent
	}

	return freeMem / 100 * parsedPercent, nil
}

// WatchBufferSize takes a string and interprets it as
// either a percentage of memory (as a percentage of
// 75% of free memory as measured on startup)
// or a humanized byte string and returns the number of
// bytes or an error if the value cannot be interpreted.
// Returns 0 on an empty string.
func WatchBufferSize(sizeString string) (size uint64, err error) {
	if sizeString == "" {
		return 0, nil
	}

	if strings.HasSuffix(sizeString, "%") {
		size, err := parsePercent(sizeString, freeMemory)
		if err != nil {
			return 0, fmt.Errorf("could not parse %s as percentage: %w", sizeString, err)
		}
		return size, nil
	}

	size, err = humanize.ParseBytes(sizeString)
	if err != nil {
		return 0, fmt.Errorf("could not parse %s as a number of bytes: %w", sizeString, err)
	}
	return size, nil
}
