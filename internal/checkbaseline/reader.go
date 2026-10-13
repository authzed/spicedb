package checkbaseline

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/datastore/options"
	"github.com/authzed/spicedb/pkg/tuple"
)

type WorkEvent struct {
	Operation     string
	Filter        json.RawMessage
	Options       json.RawMessage
	Rows, Bytes   int
	Batch         int
	Exhausted     bool
	Error         string
	Relationships []string
}
type Work struct {
	Events     []*WorkEvent
	LateEvents int
}
type Recorder struct {
	mu     sync.Mutex
	work   Work // GUARDED_BY(mu)
	sealed bool // GUARDED_BY(mu)
}
type recorderKey struct{}

func NewRecorder() *Recorder { return &Recorder{} }
func WithRecorder(ctx context.Context, r *Recorder) context.Context {
	ctx = context.WithValue(ctx, recorderKey{}, r)
	return caveats.WithEvaluationObserver(ctx, func(e caveats.EvaluationEvent) { event(ctx, "caveat-"+e.Stage, e.Name, nil, 0) })
}

func recorder(ctx context.Context) *Recorder { r, _ := ctx.Value(recorderKey{}).(*Recorder); return r }

func (r *Recorder) change(f func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed {
		r.work.LateEvents++
	}
	f()
}

func (r *Recorder) Snapshot() Work {
	r.mu.Lock()
	defer r.mu.Unlock()
	b, _ := json.Marshal(r.work)
	var w Work
	_ = json.Unmarshal(b, &w)
	return w
}
func (r *Recorder) Seal() Work { r.mu.Lock(); r.sealed = true; r.mu.Unlock(); return r.Snapshot() }
func event(ctx context.Context, op string, filter, opts any, batch int) (*Recorder, *WorkEvent) {
	r := recorder(ctx)
	if r == nil {
		return nil, nil
	}
	f, _ := json.Marshal(filter)
	o, _ := json.Marshal(opts)
	e := &WorkEvent{Operation: op, Filter: f, Options: o, Batch: batch}
	r.change(func() { r.work.Events = append(r.work.Events, e) })
	return r, e
}

func observed(ctx context.Context, op string, f, o any, batch int, call func() (datastore.RelationshipIterator, error)) (datastore.RelationshipIterator, error) {
	r, e := event(ctx, op, f, o, batch)
	seq, err := call()
	if r == nil {
		return seq, err
	}
	if err != nil {
		r.change(func() { e.Error = err.Error() })
		return nil, err
	}
	return func(yield func(tuple.Relationship, error) bool) {
		for rel, err := range seq {
			r.change(func() {
				if err != nil {
					e.Error = err.Error()
				} else {
					b, _ := json.Marshal(rel)
					e.Rows++
					e.Bytes += len(b)
					e.Relationships = append(e.Relationships, tuple.StringWithoutCaveatOrExpiration(rel))
				}
			})
			if !yield(rel, err) {
				return
			}
		}
		r.change(func() { e.Exhausted = true })
	}, nil
}

type auditLayer struct {
	datalayer.DataLayer
	delay time.Duration
}

func WrapDataLayer(dl datalayer.DataLayer) datalayer.DataLayer { return &auditLayer{DataLayer: dl} }
func WithRelationshipDelay(dl datalayer.DataLayer, delay time.Duration) datalayer.DataLayer {
	return &delayLayer{DataLayer: dl, delay: delay}
}

func (l *auditLayer) SnapshotReader(rev datastore.Revision, hash datalayer.SchemaHash) datalayer.RevisionedReader {
	return &auditReader{RevisionedReader: l.DataLayer.SnapshotReader(rev, hash), delay: l.delay}
}

type auditReader struct {
	datalayer.RevisionedReader
	delay time.Duration
}

func waitDelay(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d == 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *auditReader) QueryRelationships(ctx context.Context, f datastore.RelationshipsFilter, opts ...options.QueryOptionsOption) (datastore.RelationshipIterator, error) {
	o := options.NewQueryOptionsWithOptions(opts...)
	return observed(ctx, "query", f, struct {
		Limit                       *uint64
		Sort                        options.SortOrder
		SkipCaveats, SkipExpiration bool
		After, BeforeOrEqual        options.Cursor
		UseTupleComparison          bool
		Shape                       string
	}{o.Limit, o.Sort, o.SkipCaveats, o.SkipExpiration, o.After, o.BeforeOrEqual, o.UseTupleComparison, string(o.QueryShape)}, len(f.OptionalResourceIds), func() (datastore.RelationshipIterator, error) {
		if err := waitDelay(ctx, r.delay); err != nil {
			return nil, err
		}
		return r.RevisionedReader.QueryRelationships(ctx, f, opts...)
	})
}

func (r *auditReader) ReverseQueryRelationships(ctx context.Context, f datastore.SubjectsFilter, opts ...options.ReverseQueryOptionsOption) (datastore.RelationshipIterator, error) {
	o := options.NewReverseQueryOptionsWithOptions(opts...)
	return observed(ctx, "reverse", f, struct {
		Limit                       *uint64
		Sort                        options.SortOrder
		SkipCaveats, SkipExpiration bool
		After                       options.Cursor
		ResourceRelation            *options.ResourceRelation
		Shape                       string
	}{o.LimitForReverse, o.SortForReverse, o.SkipCaveatsForReverse, o.SkipExpirationForReverse, o.AfterForReverse, o.ResRelation, string(o.QueryShapeForReverse)}, len(f.OptionalSubjectIds), func() (datastore.RelationshipIterator, error) {
		if err := waitDelay(ctx, r.delay); err != nil {
			return nil, err
		}
		return r.RevisionedReader.ReverseQueryRelationships(ctx, f, opts...)
	})
}

func (r *auditReader) ReadSchema(ctx context.Context) (datalayer.SchemaReader, error) {
	rec, e := event(ctx, "schema", nil, nil, 0)
	sr, err := r.RevisionedReader.ReadSchema(ctx)
	if err != nil {
		if rec != nil {
			rec.change(func() { e.Error = err.Error() })
		}
		return nil, err
	}
	wrapper := &auditSchema{SchemaReader: sr}
	if provider, ok := sr.(interface {
		StoredSchema() *datastore.ReadOnlyStoredSchema
	}); ok {
		return &storedAuditSchema{auditSchema: wrapper, provider: provider}, nil
	}
	return wrapper, nil
}

type (
	auditSchema       struct{ datalayer.SchemaReader }
	storedAuditSchema struct {
		*auditSchema
		provider interface {
			StoredSchema() *datastore.ReadOnlyStoredSchema
		}
	}
)

func (s *storedAuditSchema) StoredSchema() *datastore.ReadOnlyStoredSchema {
	return s.provider.StoredSchema()
}

func (s *auditSchema) SchemaText(ctx context.Context) (string, error) {
	rec, e := event(ctx, "SchemaText", nil, nil, 0)
	value, err := s.SchemaReader.SchemaText(ctx)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) LookupTypeDefByName(ctx context.Context, name string) (datastore.RevisionedTypeDefinition, bool, error) {
	rec, e := event(ctx, "LookupTypeDefByName", name, nil, 0)
	value, found, err := s.SchemaReader.LookupTypeDefByName(ctx, name)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, found, err
}

func (s *auditSchema) LookupCaveatDefByName(ctx context.Context, name string) (datastore.RevisionedCaveat, bool, error) {
	rec, e := event(ctx, "LookupCaveatDefByName", name, nil, 0)
	value, found, err := s.SchemaReader.LookupCaveatDefByName(ctx, name)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, found, err
}

func (s *auditSchema) ListAllTypeDefinitions(ctx context.Context) ([]datastore.RevisionedTypeDefinition, error) {
	rec, e := event(ctx, "ListAllTypeDefinitions", nil, nil, 0)
	value, err := s.SchemaReader.ListAllTypeDefinitions(ctx)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) ListAllCaveatDefinitions(ctx context.Context) ([]datastore.RevisionedCaveat, error) {
	rec, e := event(ctx, "ListAllCaveatDefinitions", nil, nil, 0)
	value, err := s.SchemaReader.ListAllCaveatDefinitions(ctx)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) ListAllSchemaDefinitions(ctx context.Context) (map[string]datastore.SchemaDefinition, error) {
	rec, e := event(ctx, "ListAllSchemaDefinitions", nil, nil, 0)
	value, err := s.SchemaReader.ListAllSchemaDefinitions(ctx)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) LookupSchemaDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.SchemaDefinition, error) {
	rec, e := event(ctx, "LookupSchemaDefinitionsByNames", names, nil, 0)
	value, err := s.SchemaReader.LookupSchemaDefinitionsByNames(ctx, names)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) LookupTypeDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.TypeDefinition, error) {
	rec, e := event(ctx, "LookupTypeDefinitionsByNames", names, nil, 0)
	value, err := s.SchemaReader.LookupTypeDefinitionsByNames(ctx, names)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}

func (s *auditSchema) LookupCaveatDefinitionsByNames(ctx context.Context, names []string) (map[string]datastore.CaveatDefinition, error) {
	rec, e := event(ctx, "LookupCaveatDefinitionsByNames", names, nil, 0)
	value, err := s.SchemaReader.LookupCaveatDefinitionsByNames(ctx, names)
	if rec != nil && err != nil {
		rec.change(func() { e.Error = err.Error() })
	}
	return value, err
}
