package namespace

import (
	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	iv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// userDefinedMetadataTypeUrls are the type URLs of any user-defined metadata found
//  in the namespace proto. If placed here, FilterUserDefinedMetadata will remove the
// metadata when called on the namespace.
var userDefinedMetadataTypeUrls = map[string]struct{}{
	"type.googleapis.com/impl.v1.DocComment": {},
}

// FilterUserDefinedMetadata removes user-defined metadata (e.g. comments) from the given namespace.
func FilterUserDefinedMetadata(nsconfig *v0.NamespaceDefinition) *v0.NamespaceDefinition {
	nsconfig = proto.Clone(nsconfig).(*v0.NamespaceDefinition)

	nsconfig.Metadata = nil
	for _, relation := range nsconfig.Relation {
		if relation.Metadata != nil {
			var filteredMessages []*anypb.Any
			for _, msg := range relation.Metadata.MetadataMessage {
				if _, ok := userDefinedMetadataTypeUrls[msg.TypeUrl]; !ok {
					filteredMessages = append(filteredMessages, msg)
				}
			}
			relation.Metadata.MetadataMessage = filteredMessages
		}
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
		if err := msg.UnmarshalTo(&dc); err == nil {
			comments = append(comments, dc.Comment)
		}
	}

	return comments
}

// AddComment adds a comment to the given metadata message.
func AddComment(metadata *v0.Metadata, comment string) (*v0.Metadata, error) {
	if metadata == nil {
		metadata = &v0.Metadata{}
	}

	var dc iv1.DocComment
	dc.Comment = comment

	encoded, err := anypb.New(&dc)
	if err != nil {
		return metadata, err
	}

	metadata.MetadataMessage = append(metadata.MetadataMessage, encoded)
	return metadata, nil
}

// GetRelationKind returns the kind of the relation.
func GetRelationKind(relation *v0.Relation) iv1.RelationMetadata_RelationKind {
	metadata := relation.Metadata
	if metadata == nil {
		return iv1.RelationMetadata_UNKNOWN_KIND
	}

	for _, msg := range metadata.MetadataMessage {
		var rm iv1.RelationMetadata
		if err := msg.UnmarshalTo(&rm); err == nil {
			return rm.Kind
		}
	}

	return iv1.RelationMetadata_UNKNOWN_KIND
}

// SetRelationKind sets the kind of relation.
func SetRelationKind(relation *v0.Relation, kind iv1.RelationMetadata_RelationKind) error {
	metadata := relation.Metadata
	if metadata == nil {
		metadata = &v0.Metadata{}
		relation.Metadata = metadata
	}

	var rm iv1.RelationMetadata
	rm.Kind = kind

	encoded, err := anypb.New(&rm)
	if err != nil {
		return err
	}

	metadata.MetadataMessage = append(metadata.MetadataMessage, encoded)
	return nil
}
