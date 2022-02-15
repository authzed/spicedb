package namespace

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"

	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

func TestMetadata(t *testing.T) {
	require := require.New(t)

	marshalled, err := anypb.New(&iv1.DocComment{
		Comment: "Hi there",
	})
	require.Nil(err)

	marshalledKind, err := anypb.New(&iv1.RelationMetadata{
		Kind: iv1.RelationMetadata_PERMISSION,
	})
	require.Nil(err)

	ns := &core.NamespaceDefinition{
		Name: "somens",
		Relation: []*core.Relation{
			{
				Name: "somerelation",
				Metadata: &core.Metadata{
					MetadataMessage: []*anypb.Any{
						marshalledKind, marshalled,
					},
				},
			},
			{
				Name: "anotherrelation",
			},
		},
		Metadata: &core.Metadata{
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

	FilterUserDefinedMetadataInPlace(ns)
	require.Equal([]string{}, GetComments(ns.Metadata))
	require.Equal([]string{}, GetComments(ns.Relation[0].Metadata))

	require.Equal(iv1.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))
}
