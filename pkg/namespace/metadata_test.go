package namespace

import (
	"testing"

	"github.com/golang/protobuf/ptypes"
	anypb "github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/require"

	iv1 "github.com/authzed/spicedb/internal/proto/impl/v1"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

func TestMetadata(t *testing.T) {
	require := require.New(t)

	marshalled, err := ptypes.MarshalAny(&iv1.DocComment{
		Comment: "Hi there",
	})
	require.Nil(err)

	marshalled_kind, err := ptypes.MarshalAny(&iv1.RelationMetadata{
		Kind: iv1.RelationMetadata_PERMISSION,
	})
	require.Nil(err)

	ns := &v0.NamespaceDefinition{
		Name: "somens",
		Relation: []*v0.Relation{
			{
				Name: "somerelation",
				Metadata: &v0.Metadata{
					MetadataMessage: []*anypb.Any{
						marshalled_kind, marshalled,
					},
				},
			},
			{
				Name: "anotherrelation",
			},
		},
		Metadata: &v0.Metadata{
			MetadataMessage: []*anypb.Any{
				marshalled,
			},
		},
	}

	verr := ns.Validate()
	require.NoError(verr)

	require.Equal([]string{"Hi there"}, GetComments(ns.Metadata))
	require.Equal([]string{"Hi there"}, GetComments(ns.Relation[0].Metadata))
	require.Equal(iv1.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))

	require.Equal([]string{}, GetComments(ns.Relation[1].Metadata))

	stripped := StripMetadata(ns)
	require.Equal([]string{}, GetComments(stripped.Metadata))
	require.Equal([]string{}, GetComments(stripped.Relation[0].Metadata))

	require.Equal([]string{"Hi there"}, GetComments(ns.Metadata))
	require.Equal([]string{"Hi there"}, GetComments(ns.Relation[0].Metadata))

	require.Equal(iv1.RelationMetadata_UNKNOWN_KIND, GetRelationKind(stripped.Relation[0]))
	require.Equal(iv1.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))
}
