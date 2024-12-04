package memdb

import (
	"time"

	"github.com/hashicorp/go-memdb"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

const (
	tableNamespace = "namespace"

	tableRelationship         = "relationship"
	indexID                   = "id"
	indexNamespace            = "namespace"
	indexNamespaceAndRelation = "namespaceAndRelation"
	indexSubjectNamespace     = "subjectNamespace"

	tableCounters = "counters"

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

type counter struct {
	name        string
	filterBytes []byte
	count       int
	updated     datastore.Revision
}

type relationship struct {
	namespace        string
	resourceID       string
	relation         string
	subjectNamespace string
	subjectObjectID  string
	subjectRelation  string
	caveat           *contextualizedCaveat
	integrity        *relationshipIntegrity
	expiration       *time.Time
}

type relationshipIntegrity struct {
	keyID     string
	hash      []byte
	timestamp time.Time
}

func (ri relationshipIntegrity) MarshalZerologObject(e *zerolog.Event) {
	e.Str("keyID", ri.keyID).Bytes("hash", ri.hash).Time("timestamp", ri.timestamp)
}

func (ri relationshipIntegrity) RelationshipIntegrity() *core.RelationshipIntegrity {
	return &core.RelationshipIntegrity{
		KeyId:    ri.keyID,
		Hash:     ri.hash,
		HashedAt: timestamppb.New(ri.timestamp),
	}
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

	expiration := ""
	if r.expiration != nil {
		expiration = "[expiration:" + r.expiration.Format(time.RFC3339Nano) + "]"
	}

	return r.namespace + ":" + r.resourceID + "#" + r.relation + "@" + r.subjectNamespace + ":" + r.subjectObjectID + "#" + r.subjectRelation + caveat + expiration
}

func (r relationship) MarshalZerologObject(e *zerolog.Event) {
	e.Str("rel", r.String())
}

func (r relationship) Relationship() (tuple.Relationship, error) {
	cr, err := r.caveat.ContextualizedCaveat()
	if err != nil {
		return tuple.Relationship{}, err
	}

	var ig *core.RelationshipIntegrity
	if r.integrity != nil {
		ig = r.integrity.RelationshipIntegrity()
	}

	return tuple.Relationship{
		RelationshipReference: tuple.RelationshipReference{
			Resource: tuple.ObjectAndRelation{
				ObjectType: r.namespace,
				ObjectID:   r.resourceID,
				Relation:   r.relation,
			},
			Subject: tuple.ObjectAndRelation{
				ObjectType: r.subjectNamespace,
				ObjectID:   r.subjectObjectID,
				Relation:   r.subjectRelation,
			},
		},
		OptionalCaveat:     cr,
		OptionalIntegrity:  ig,
		OptionalExpiration: r.expiration,
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
		tableCounters: {
			Name: tableCounters,
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
