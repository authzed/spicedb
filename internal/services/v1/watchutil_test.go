package v1

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

func TestConvertWatchKindToContent(t *testing.T) {
	tests := []struct {
		name  string
		kinds []v1.WatchKind
		exp   datastore.WatchContent
	}{
		{
			name:  "default",
			kinds: []v1.WatchKind{},
			exp:   datastore.WatchRelationships,
		},
		{
			name:  "unspecified",
			kinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_UNSPECIFIED},
			exp:   datastore.WatchRelationships,
		},
		{
			name:  "only_relationship_updates",
			kinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES},
			exp:   datastore.WatchRelationships,
		},
		{
			name:  "relationships_and_schema_updates",
			kinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES},
			exp:   datastore.WatchRelationships | datastore.WatchSchema,
		},
		{
			name:  "relationships_and_checkpoints",
			kinds: []v1.WatchKind{v1.WatchKind_WATCH_KIND_INCLUDE_CHECKPOINTS},
			exp:   datastore.WatchRelationships | datastore.WatchCheckpoints,
		},
		{
			name: "all",
			kinds: []v1.WatchKind{
				v1.WatchKind_WATCH_KIND_INCLUDE_RELATIONSHIP_UPDATES,
				v1.WatchKind_WATCH_KIND_INCLUDE_SCHEMA_UPDATES,
				v1.WatchKind_WATCH_KIND_INCLUDE_CHECKPOINTS,
			},
			exp: datastore.WatchRelationships | datastore.WatchSchema | datastore.WatchCheckpoints,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require := require.New(t)
			result := convertWatchKindToContent(tt.kinds)
			require.Equal(tt.exp, result)
		})
	}
}
