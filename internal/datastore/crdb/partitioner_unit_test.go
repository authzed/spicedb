package crdb

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestParseRangeStartKey(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		wantErr  bool
		wantNS   string
		wantOID  string
		wantRel  string
		wantUNS  string
		wantUOID string
		wantURel string
	}{
		{
			name:     "valid key with 6 PK columns",
			key:      `/Table/53/1/"document"/"doc123"/"viewer"/"user"/"alice"/"member"`,
			wantNS:   "document",
			wantOID:  "doc123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "alice",
			wantURel: "member",
		},
		{
			name:     "valid key with extra trailing parts",
			key:      `/Table/53/1/"ns"/"oid"/"rel"/"uns"/"uoid"/"urel"/extra`,
			wantNS:   "ns",
			wantOID:  "oid",
			wantRel:  "rel",
			wantUNS:  "uns",
			wantUOID: "uoid",
			wantURel: "urel",
		},
		{
			name:     "valid key with slashes in PK values",
			key:      `…/4/"dev_v1/my_organization"/"..."/"dev_v1/my_organization"/"is_enabled"/"ca7fd83c-a53a-4928-a0b1-79f22699cd05"/"ca7fd83c-a53a-4928-a0b1-79f22699cd05"`,
			wantNS:   "dev_v1/my_organization",
			wantOID:  "...",
			wantRel:  "dev_v1/my_organization",
			wantUNS:  "is_enabled",
			wantUOID: "ca7fd83c-a53a-4928-a0b1-79f22699cd05",
			wantURel: "ca7fd83c-a53a-4928-a0b1-79f22699cd05",
		},
		{
			name:     "valid key with pipe in object_id",
			key:      `/Table/53/1/"resource"/"foo|bar"/"viewer"/"user"/"baz|qux"/"..."`,
			wantNS:   "resource",
			wantOID:  "foo|bar",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "baz|qux",
			wantURel: "...",
		},
		{
			name:     "valid key with equals and plus in object_id",
			key:      `/Table/53/1/"resource"/"key=value+extra"/"viewer"/"user"/"id=123+456"/"..."`,
			wantNS:   "resource",
			wantOID:  "key=value+extra",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "id=123+456",
			wantURel: "...",
		},
		{
			name:     "valid key with wildcard subject",
			key:      `/Table/53/1/"resource"/"123"/"viewer"/"user"/"*"/"..."`,
			wantNS:   "resource",
			wantOID:  "123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "*",
			wantURel: "...",
		},
		{
			name:     "valid key with uppercase in object_id",
			key:      `/Table/53/1/"resource"/"AbCdEf123"/"viewer"/"user"/"XyZ789"/"..."`,
			wantNS:   "resource",
			wantOID:  "AbCdEf123",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "XyZ789",
			wantURel: "...",
		},
		{
			name:     "valid key with ellipsis relation",
			key:      `/Table/77/1/"resource3"/"0"/"viewer"/"user"/"0"/"..."`,
			wantNS:   "resource3",
			wantOID:  "0",
			wantRel:  "viewer",
			wantUNS:  "user",
			wantUOID: "0",
			wantURel: "...",
		},
		{
			name:    "key with fewer than 6 quoted values",
			key:     `/Table/53/1/"document"/"doc123"/"viewer"`,
			wantErr: true,
		},
		{
			name:    "key with no quoted values",
			key:     `…/2`,
			wantErr: true,
		},
		{
			name:    "empty key",
			key:     ``,
			wantErr: true,
		},
		{
			name:    "key with only table and index",
			key:     `/Table/53/1`,
			wantErr: true,
		},
		{
			name:    "key with empty quoted value in namespace",
			key:     `/Table/53/1/""/"oid"/"rel"/"uns"/"uoid"/"urel"`,
			wantErr: true,
		},
		{
			name:    "key with empty quoted value in object_id",
			key:     `/Table/53/1/"ns"/""/"rel"/"uns"/"uoid"/"urel"`,
			wantErr: true,
		},
		{
			name:    "before-style key",
			key:     `<before:/Table/77/3>`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cursor, err := parseRangeStartKey(tt.key)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, cursor)

			rel := options.ToRelationship(cursor)
			require.Equal(t, tt.wantNS, rel.Resource.ObjectType)
			require.Equal(t, tt.wantOID, rel.Resource.ObjectID)
			require.Equal(t, tt.wantRel, rel.Resource.Relation)
			require.Equal(t, tt.wantUNS, rel.Subject.ObjectType)
			require.Equal(t, tt.wantUOID, rel.Subject.ObjectID)
			require.Equal(t, tt.wantURel, rel.Subject.Relation)
		})
	}
}

func TestPlanPartitionsLogic(t *testing.T) {
	t.Run("desiredCount=0 returns single partition", func(t *testing.T) {
		// PlanPartitions with 0 should behave like 1.
		// We can't call the real method without a datastore, so test the
		// boundary grouping logic directly.
		partitions := groupBoundaries(nil, 0)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})

	t.Run("desiredCount=1 returns single partition", func(t *testing.T) {
		partitions := groupBoundaries(nil, 1)
		require.Len(t, partitions, 1)
	})

	t.Run("1 boundary produces 2 partitions", func(t *testing.T) {
		b := makeBoundary("ns1", "oid1", "rel1", "uns1", "uoid1", "urel1")
		partitions := groupBoundaries([]options.Cursor{b}, 4)
		require.Len(t, partitions, 2)

		require.Nil(t, partitions[0].LowerBound)
		require.NotNil(t, partitions[0].UpperBound)
		require.NotNil(t, partitions[1].LowerBound)
		require.Nil(t, partitions[1].UpperBound)

		// Shared boundary.
		require.Equal(t,
			*options.ToRelationship(partitions[0].UpperBound),
			*options.ToRelationship(partitions[1].LowerBound),
		)
	})

	t.Run("3 boundaries produce 4 partitions when K=4", func(t *testing.T) {
		boundaries := []options.Cursor{
			makeBoundary("a", "0", "r", "u", "0", "..."),
			makeBoundary("b", "0", "r", "u", "0", "..."),
			makeBoundary("c", "0", "r", "u", "0", "..."),
		}
		partitions := groupBoundaries(boundaries, 4)
		require.Len(t, partitions, 4)

		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[3].UpperBound)

		// All adjacent boundaries match.
		for i := 1; i < len(partitions); i++ {
			require.Equal(t,
				*options.ToRelationship(partitions[i-1].UpperBound),
				*options.ToRelationship(partitions[i].LowerBound),
			)
		}
	})

	t.Run("K larger than boundaries+1 is capped", func(t *testing.T) {
		boundaries := []options.Cursor{
			makeBoundary("a", "0", "r", "u", "0", "..."),
		}
		partitions := groupBoundaries(boundaries, 100)
		require.Len(t, partitions, 2) // 1 boundary → max 2 partitions
	})

	t.Run("many boundaries with small K downsamples", func(t *testing.T) {
		boundaries := make([]options.Cursor, 20)
		for i := range boundaries {
			boundaries[i] = makeBoundary(
				fmt.Sprintf("ns%02d", i), "0", "r", "u", "0", "...",
			)
		}
		partitions := groupBoundaries(boundaries, 4)
		require.Len(t, partitions, 4)

		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[3].UpperBound)

		for i := 1; i < len(partitions); i++ {
			require.Equal(t,
				*options.ToRelationship(partitions[i-1].UpperBound),
				*options.ToRelationship(partitions[i].LowerBound),
			)
		}
	})

	t.Run("empty boundaries returns single partition", func(t *testing.T) {
		partitions := groupBoundaries([]options.Cursor{}, 4)
		require.Len(t, partitions, 1)
		require.Nil(t, partitions[0].LowerBound)
		require.Nil(t, partitions[0].UpperBound)
	})
}

func makeBoundary(ns, oid, rel, uns, uoid, urel string) options.Cursor {
	return options.ToCursor(tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: ns,
				ObjectID:   oid,
				Relation:   rel,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: uns,
				ObjectID:   uoid,
				Relation:   urel,
			},
		},
	})
}
