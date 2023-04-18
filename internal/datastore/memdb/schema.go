package memdb

import (
	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/hashicorp/go-memdb"
	"github.com/jzelinskie/stringz"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
)

const (
	tableNamespace = "namespace"

	tableRelationship         = "relationship"
	indexID                   = "id"
	indexNamespace            = "namespace"
	indexNamespaceAndRelation = "namespaceAndRelation"
	indexSubjectNamespace     = "subjectNamespace"

	tableChangelog = "changelog"
	indexRevision  = "id"
)

type namespace struct {
	name        string
	configBytes []byte
	updated     datastore.Revision
}

func (ns namespace) MarshalZerologObject(e *zerolog.Event) {
	e.Stringer("rev", ns.updated).Str("name", ns.name)
}

type relationship struct {
	namespace        string
	resourceID       string
	relation         string
	subjectNamespace string
	subjectObjectID  string
	subjectRelation  string
	caveat           *contextualizedCaveat
}

type contextualizedCaveat struct {
	caveatName string
	context    map[string]any
}

func (cr *contextualizedCaveat) ContextualizedCaveat() (*core.ContextualizedCaveat, error) {
	if cr == nil {
		return nil, nil
	}
	v, err := structpb.NewStruct(cr.context)
	if err != nil {
		return nil, err
	}
	return &core.ContextualizedCaveat{
		CaveatName: cr.caveatName,
		Context:    v,
	}, nil
}

func (r relationship) String() string {
	caveat := ""
	if r.caveat != nil {
		caveat = "[" + r.caveat.caveatName + "]"
	}

	return r.namespace + ":" + r.resourceID + "#" + r.relation + "@" + r.subjectNamespace + ":" + r.subjectObjectID + "#" + r.subjectRelation + caveat
}

func (r relationship) MarshalZerologObject(e *zerolog.Event) {
	e.Str("rel", r.String())
}

func (r relationship) Relationship() *v1.Relationship {
	return &v1.Relationship{
		Resource: &v1.ObjectReference{
			ObjectType: r.namespace,
			ObjectId:   r.resourceID,
		},
		Relation: r.relation,
		Subject: &v1.SubjectReference{
			Object: &v1.ObjectReference{
				ObjectType: r.subjectNamespace,
				ObjectId:   r.subjectObjectID,
			},
			OptionalRelation: stringz.Default(r.subjectRelation, "", datastore.Ellipsis),
		},
	}
}

func (r relationship) RelationTuple() (*core.RelationTuple, error) {
	cr, err := r.caveat.ContextualizedCaveat()
	if err != nil {
		return nil, err
	}
	return &core.RelationTuple{
		ResourceAndRelation: &core.ObjectAndRelation{
			Namespace: r.namespace,
			ObjectId:  r.resourceID,
			Relation:  r.relation,
		},
		Subject: &core.ObjectAndRelation{
			Namespace: r.subjectNamespace,
			ObjectId:  r.subjectObjectID,
			Relation:  r.subjectRelation,
		},
		Caveat: cr,
	}, nil
}

type changelog struct {
	revisionNanos int64
	changes       datastore.RevisionChanges
}

var schema = &memdb.DBSchema{
	Tables: map[string]*memdb.TableSchema{
		tableNamespace: {
			Name: tableNamespace,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.StringFieldIndex{Field: "name"},
				},
			},
		},
		tableChangelog: {
			Name: tableChangelog,
			Indexes: map[string]*memdb.IndexSchema{
				indexRevision: {
					Name:    indexRevision,
					Unique:  true,
					Indexer: &memdb.IntFieldIndex{Field: "revisionNanos"},
				},
			},
		},
		tableRelationship: {
			Name: tableRelationship,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:   indexID,
					Unique: true,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "resourceID"},
							&memdb.StringFieldIndex{Field: "relation"},
							&memdb.StringFieldIndex{Field: "subjectNamespace"},
							&memdb.StringFieldIndex{Field: "subjectObjectID"},
							&memdb.StringFieldIndex{Field: "subjectRelation"},
						},
					},
				},
				indexNamespace: {
					Name:    indexNamespace,
					Unique:  false,
					Indexer: &memdb.StringFieldIndex{Field: "namespace"},
				},
				indexNamespaceAndRelation: {
					Name:   indexNamespaceAndRelation,
					Unique: false,
					Indexer: &memdb.CompoundIndex{
						Indexes: []memdb.Indexer{
							&memdb.StringFieldIndex{Field: "namespace"},
							&memdb.StringFieldIndex{Field: "relation"},
						},
					},
				},
				indexSubjectNamespace: {
					Name:    indexSubjectNamespace,
					Unique:  false,
					Indexer: &memdb.StringFieldIndex{Field: "subjectNamespace"},
				},
			},
		},
		tableCaveats: {
			Name: tableCaveats,
			Indexes: map[string]*memdb.IndexSchema{
				indexID: {
					Name:    indexID,
					Unique:  true,
					Indexer: &memdb.StringFieldIndex{Field: "name"},
				},
			},
		},
	},
}
