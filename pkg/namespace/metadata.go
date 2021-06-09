package namespace

import (
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/proto"

	pb "github.com/authzed/spicedb/pkg/proto/REDACTEDapi/api"
)

// StripMetadata removes all metadata from the given namespace.
func StripMetadata(nsconfig *pb.NamespaceDefinition) *pb.NamespaceDefinition {
	nsconfig = proto.Clone(nsconfig).(*pb.NamespaceDefinition)

	nsconfig.Metadata = nil
	for _, relation := range nsconfig.Relation {
		relation.Metadata = nil
	}
	return nsconfig
}

// GetComments returns the comment metadata found within the given metadata message.
func GetComments(metadata *pb.Metadata) []string {
	if metadata == nil {
		return []string{}
	}

	comments := []string{}
	for _, msg := range metadata.MetadataMessage {
		var dc pb.DocComment
		if err := ptypes.UnmarshalAny(msg, &dc); err == nil {
			comments = append(comments, dc.Comment)
		}
	}

	return comments
}

// GetRelationKind returns the kind of the relation.
func GetRelationKind(relation *pb.Relation) pb.RelationMetadata_RelationKind {
	metadata := relation.Metadata
	if metadata == nil {
		return pb.RelationMetadata_UNKNOWN_KIND
	}

	for _, msg := range metadata.MetadataMessage {
		var rm pb.RelationMetadata
		if err := ptypes.UnmarshalAny(msg, &rm); err == nil {
			return rm.Kind
		}
	}

	return pb.RelationMetadata_UNKNOWN_KIND
}
