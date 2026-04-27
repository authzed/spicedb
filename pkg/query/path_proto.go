package query

import (
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dispatchv1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
)

// ToProto converts a Path into its dispatch.v1.ResultPath wire representation.
func (p *Path) ToProto() (*dispatchv1.ResultPath, error) {
	if p == nil {
		return nil, nil
	}

	var metadata *structpb.Struct
	if len(p.Metadata) > 0 {
		m, err := structpb.NewStruct(p.Metadata)
		if err != nil {
			return nil, err
		}
		metadata = m
	}

	var expiration *timestamppb.Timestamp
	if p.Expiration != nil {
		expiration = timestamppb.New(*p.Expiration)
	}

	var excluded []*dispatchv1.ResultPath
	if len(p.ExcludedSubjects) > 0 {
		excluded = make([]*dispatchv1.ResultPath, 0, len(p.ExcludedSubjects))
		for _, es := range p.ExcludedSubjects {
			esProto, err := es.ToProto()
			if err != nil {
				return nil, err
			}
			excluded = append(excluded, esProto)
		}
	}

	return &dispatchv1.ResultPath{
		ResourceType:     p.Resource.ObjectType,
		ResourceId:       p.Resource.ObjectID,
		Relation:         p.Relation,
		SubjectType:      p.Subject.ObjectType,
		SubjectId:        p.Subject.ObjectID,
		SubjectRelation:  p.Subject.Relation,
		Caveat:           p.Caveat,
		Expiration:       expiration,
		Integrity:        p.Integrity,
		Metadata:         metadata,
		ExcludedSubjects: excluded,
	}, nil
}

// PathFromProto converts a dispatch.v1.ResultPath back into a Path.
func PathFromProto(rp *dispatchv1.ResultPath) (*Path, error) {
	if rp == nil {
		return nil, nil
	}

	var metadata map[string]any
	if rp.Metadata != nil {
		metadata = rp.Metadata.AsMap()
	}

	var expiration *time.Time
	if rp.Expiration != nil {
		t := rp.Expiration.AsTime()
		expiration = &t
	}

	var excluded []*Path
	if len(rp.ExcludedSubjects) > 0 {
		excluded = make([]*Path, 0, len(rp.ExcludedSubjects))
		for _, esp := range rp.ExcludedSubjects {
			es, err := PathFromProto(esp)
			if err != nil {
				return nil, err
			}
			excluded = append(excluded, es)
		}
	}

	return &Path{
		Resource: Object{
			ObjectType: rp.ResourceType,
			ObjectID:   rp.ResourceId,
		},
		Relation: rp.Relation,
		Subject: ObjectAndRelation{
			ObjectType: rp.SubjectType,
			ObjectID:   rp.SubjectId,
			Relation:   rp.SubjectRelation,
		},
		Caveat:           rp.Caveat,
		Expiration:       expiration,
		Integrity:        rp.Integrity,
		ExcludedSubjects: excluded,
		Metadata:         metadata,
	}, nil
}
