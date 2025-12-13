package schema

import (
	"fmt"

	"google.golang.org/protobuf/types/known/anypb"

	corev1 "github.com/authzed/spicedb/pkg/proto/core/v1"
	implv1 "github.com/authzed/spicedb/pkg/proto/impl/v1"
)

// Metadata represents parsed metadata for definitions, caveats, and relations.
// It can contain documentation comments and relation-specific metadata.
type Metadata struct {
	// comments are documentation comments extracted from the schema.
	comments []string

	// relationKind indicates whether this is a relation or permission (only for relations).
	relationKind RelationKind

	// typeAnnotations are type annotations for permissions (only for permissions).
	typeAnnotations []string
}

// RelationKind represents the kind of relation.
type RelationKind int

const (
	// RelationKindUnknown represents an unknown relation kind.
	RelationKindUnknown RelationKind = iota

	// RelationKindRelation represents a relation.
	RelationKindRelation

	// RelationKindPermission represents a permission.
	RelationKindPermission
)

// NewMetadata creates a new empty Metadata.
func NewMetadata() *Metadata {
	return &Metadata{
		comments:        []string{},
		relationKind:    RelationKindUnknown,
		typeAnnotations: []string{},
	}
}

// Comments returns the documentation comments.
func (m *Metadata) Comments() []string {
	if m == nil {
		return []string{}
	}
	return m.comments
}

// RelationKind returns the relation kind.
func (m *Metadata) RelationKind() RelationKind {
	if m == nil {
		return RelationKindUnknown
	}
	return m.relationKind
}

// TypeAnnotations returns the type annotations.
func (m *Metadata) TypeAnnotations() []string {
	if m == nil {
		return []string{}
	}
	return m.typeAnnotations
}

// WithComment adds a comment to the metadata.
func (m *Metadata) WithComment(comment string) *Metadata {
	if m == nil {
		m = NewMetadata()
	}
	m.comments = append(m.comments, comment)
	return m
}

// WithRelationKind sets the relation kind.
func (m *Metadata) WithRelationKind(kind RelationKind) *Metadata {
	if m == nil {
		m = NewMetadata()
	}
	m.relationKind = kind
	return m
}

// WithTypeAnnotations sets the type annotations.
func (m *Metadata) WithTypeAnnotations(types []string) *Metadata {
	if m == nil {
		m = NewMetadata()
	}
	m.typeAnnotations = types
	return m
}

// parseMetadata parses a corev1.Metadata protobuf message into our internal Metadata type.
func parseMetadata(protoMetadata *corev1.Metadata) (*Metadata, error) {
	if protoMetadata == nil {
		return nil, nil
	}

	metadata := NewMetadata()

	for _, anyMsg := range protoMetadata.MetadataMessage {
		// Try to unmarshal as DocComment
		var docComment implv1.DocComment
		if err := anyMsg.UnmarshalTo(&docComment); err == nil {
			metadata.comments = append(metadata.comments, docComment.Comment)
			continue
		}

		// Try to unmarshal as RelationMetadata
		var relationMetadata implv1.RelationMetadata
		if err := anyMsg.UnmarshalTo(&relationMetadata); err == nil {
			switch relationMetadata.Kind {
			case implv1.RelationMetadata_RELATION:
				metadata.relationKind = RelationKindRelation
			case implv1.RelationMetadata_PERMISSION:
				metadata.relationKind = RelationKindPermission
				if relationMetadata.TypeAnnotations != nil {
					metadata.typeAnnotations = relationMetadata.TypeAnnotations.Types
				}
			default:
				metadata.relationKind = RelationKindUnknown
			}
			continue
		}
	}

	// Return nil if no metadata was parsed
	if len(metadata.comments) == 0 && metadata.relationKind == RelationKindUnknown && len(metadata.typeAnnotations) == 0 {
		return nil, nil
	}

	return metadata, nil
}

// encodeMetadata converts our internal Metadata type back to a corev1.Metadata protobuf message.
func encodeMetadata(metadata *Metadata) (*corev1.Metadata, error) {
	if metadata == nil {
		return nil, nil
	}

	protoMetadata := &corev1.Metadata{
		MetadataMessage: []*anypb.Any{},
	}

	// Encode comments
	for _, comment := range metadata.comments {
		docComment := &implv1.DocComment{
			Comment: comment,
		}
		encoded, err := anypb.New(docComment)
		if err != nil {
			return nil, fmt.Errorf("failed to encode doc comment: %w", err)
		}
		protoMetadata.MetadataMessage = append(protoMetadata.MetadataMessage, encoded)
	}

	// Encode relation metadata if present
	if metadata.relationKind != RelationKindUnknown {
		relationMetadata := &implv1.RelationMetadata{}

		switch metadata.relationKind {
		case RelationKindRelation:
			relationMetadata.Kind = implv1.RelationMetadata_RELATION
		case RelationKindPermission:
			relationMetadata.Kind = implv1.RelationMetadata_PERMISSION
			if len(metadata.typeAnnotations) > 0 {
				relationMetadata.TypeAnnotations = &implv1.TypeAnnotations{
					Types: metadata.typeAnnotations,
				}
			}
		}

		encoded, err := anypb.New(relationMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to encode relation metadata: %w", err)
		}
		protoMetadata.MetadataMessage = append(protoMetadata.MetadataMessage, encoded)
	}

	// Return nil if no messages were encoded
	if len(protoMetadata.MetadataMessage) == 0 {
		return nil, nil
	}

	return protoMetadata, nil
}
