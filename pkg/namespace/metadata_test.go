package namespace

import (
	"testing"

	"github.com/golang/protobuf/ptypes"
	anypb "github.com/golang/protobuf/ptypes/any"
	"github.com/stretchr/testify/require"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

func TestMetadata(t *testing.T) {
	require := require.New(t)

	marshalled, err := ptypes.MarshalAny(&pb.DocComment{
		Comment: "Hi there",
	})
	require.Nil(err)

	marshalled_kind, err := ptypes.MarshalAny(&pb.RelationMetadata{
		Kind: pb.RelationMetadata_PERMISSION,
	})
	require.Nil(err)

	ns := &pb.NamespaceDefinition{
		Name: "somens",
		Relation: []*pb.Relation{
			{
				Name: "somerelation",
				Metadata: &pb.Metadata{
					MetadataMessage: []*anypb.Any{
						marshalled_kind, marshalled,
					},
				},
			},
			{
				Name: "anotherrelation",
			},
		},
		Metadata: &pb.Metadata{
			MetadataMessage: []*anypb.Any{
				marshalled,
			},
		},
	}

	verr := ns.Validate()
	require.NoError(verr)

	require.Equal([]string{"Hi there"}, GetComments(ns.Metadata))
	require.Equal([]string{"Hi there"}, GetComments(ns.Relation[0].Metadata))
	require.Equal(pb.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))

	require.Equal([]string{}, GetComments(ns.Relation[1].Metadata))

	stripped := StripMetadata(ns)
	require.Equal([]string{}, GetComments(stripped.Metadata))
	require.Equal([]string{}, GetComments(stripped.Relation[0].Metadata))

	require.Equal([]string{"Hi there"}, GetComments(ns.Metadata))
	require.Equal([]string{"Hi there"}, GetComments(ns.Relation[0].Metadata))

	require.Equal(pb.RelationMetadata_UNKNOWN_KIND, GetRelationKind(stripped.Relation[0]))
	require.Equal(pb.RelationMetadata_PERMISSION, GetRelationKind(ns.Relation[0]))
}
