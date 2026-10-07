package relationships

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestValidateNativeUpdates(t *testing.T) {
	rel := tuple.MustParse("document:doc#reader@user:alice")

	require.NoError(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(rel)}, 1, 100, false))
	require.Error(t, ValidateNativeUpdates(nil, 1, 100, false))
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(rel), tuple.Touch(rel)}, 1, 100, false))
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{{Operation: tuple.UpdateOperation(100), Relationship: rel}}, 1, 100, false))
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(tuple.Relationship{})}, 1, 100, false))
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(rel), tuple.Touch(rel)}, 2, 100, false))

	caveated := rel
	caveated.OptionalCaveat = &core.ContextualizedCaveat{
		CaveatName: "sample",
		Context: &structpb.Struct{Fields: map[string]*structpb.Value{
			"value": structpb.NewStringValue("too long"),
		}},
	}
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(caveated)}, 1, 1, false))

	expiring := rel
	expiresAt := time.Now()
	expiring.OptionalExpiration = &expiresAt
	require.Error(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(expiring)}, 1, 100, false))
	require.NoError(t, ValidateNativeUpdates([]tuple.RelationshipUpdate{tuple.Create(expiring)}, 1, 100, true))
}
