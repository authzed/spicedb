package namespace

import (
	"github.com/golang/protobuf/ptypes"
	"google.golang.org/protobuf/proto"

	// TODO: stop exposing private v0 types in package's API
	iv1 "github.com/authzed/spicedb/internal/proto/impl/v1"
	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
)

// StripMetadata removes all metadata from the given namespace.
func StripMetadata(nsconfig *v0.NamespaceDefinition) *v0.NamespaceDefinition {
	nsconfig = proto.Clone(nsconfig).(*v0.NamespaceDefinition)

	nsconfig.Metadata = nil
	for _, relation := range nsconfig.Relation {
		relation.Metadata = nil
	}
	return nsconfig
}

// GetComments returns the comment metadata found within the given metadata message.
func GetComments(metadata *v0.Metadata) []string {
	if metadata == nil {
		return []string{}
	}

	comments := []string{}
	for _, msg := range metadata.MetadataMessage {
		var dc iv1.DocComment
		if err := ptypes.UnmarshalAny(msg, &dc); err == nil {
			comments = append(comments, dc.Comment)
		}
	}

	return comments
}

// GetRelationKind returns the kind of the relation.
func GetRelationKind(relation *v0.Relation) iv1.RelationMetadata_RelationKind {
	metadata := relation.Metadata
	if metadata == nil {
		return iv1.RelationMetadata_UNKNOWN_KIND
	}

	for _, msg := range metadata.MetadataMessage {
		var rm iv1.RelationMetadata
		if err := ptypes.UnmarshalAny(msg, &rm); err == nil {
			return rm.Kind
		}
	}

	return iv1.RelationMetadata_UNKNOWN_KIND
}
