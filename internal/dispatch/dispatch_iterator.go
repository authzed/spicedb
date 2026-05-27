package dispatch

import (
	"fmt"
	"io"

	"github.com/authzed/spicedb/pkg/query"
)

// DispatchIteratorType is the wire/registry byte for DispatchIterator. It uses
// lowercase 'd' so it does not collide with DatastoreIteratorType ('D').
const DispatchIteratorType query.IteratorType = 'd'

func init() {
	query.MustRegisterIterator(query.IteratorSpec{
		Type: DispatchIteratorType,
		Name: "Dispatch",
		ConstructWithArgs: func(_ *query.IteratorArgs, subs []query.Iterator, key query.CanonicalKey) (query.Iterator, error) {
			if len(subs) != 1 {
				return nil, fmt.Errorf("DispatchIterator requires exactly 1 subiterator, got %d", len(subs))
			}
			d := NewDispatchIterator(subs[0])
			d.canonicalKey = key
			return d, nil
		},
		Deserialize: deserializeDispatch,
	})
}

// DispatchIterator is a single-child passthrough: every Plan method delegates
// straight to its sub-iterator. It carries no state of its own beyond a
// canonical key and the wrapped child, and exists as a marker node in the
// iterator tree that the dispatch layer can recognize and act on.
type DispatchIterator struct {
	subIt        query.Iterator
	canonicalKey query.CanonicalKey
}

var _ query.Iterator = &DispatchIterator{}

// NewDispatchIterator wraps sub in a passthrough DispatchIterator.
func NewDispatchIterator(sub query.Iterator) *DispatchIterator {
	return &DispatchIterator{subIt: sub}
}

func (d *DispatchIterator) CheckImpl(ctx *query.Context, resource query.Object, subject query.ObjectAndRelation) (*query.Path, error) {
	return ctx.Check(d.subIt, resource, subject)
}

func (d *DispatchIterator) IterSubjectsImpl(ctx *query.Context, resource query.Object, filterSubjectType query.ObjectType) (query.PathSeq, error) {
	return ctx.IterSubjects(d.subIt, resource, filterSubjectType)
}

func (d *DispatchIterator) IterResourcesImpl(ctx *query.Context, subject query.ObjectAndRelation, filterResourceType query.ObjectType) (query.PathSeq, error) {
	return ctx.IterResources(d.subIt, subject, filterResourceType)
}

func (d *DispatchIterator) Clone() query.Iterator {
	return &DispatchIterator{
		subIt:        d.subIt.Clone(),
		canonicalKey: d.canonicalKey,
	}
}

func (d *DispatchIterator) Explain() query.Explain {
	return query.Explain{
		Name:       "Dispatch",
		Info:       "Dispatch",
		SubExplain: []query.Explain{d.subIt.Explain()},
	}
}

func (d *DispatchIterator) Subiterators() []query.Iterator {
	return []query.Iterator{d.subIt}
}

func (d *DispatchIterator) ReplaceSubiterators(newSubs []query.Iterator) (query.Iterator, error) {
	if len(newSubs) != 1 {
		return nil, fmt.Errorf("DispatchIterator requires exactly 1 subiterator, got %d", len(newSubs))
	}
	return &DispatchIterator{
		subIt:        newSubs[0],
		canonicalKey: d.canonicalKey,
	}, nil
}

func (d *DispatchIterator) CanonicalKey() query.CanonicalKey {
	return d.canonicalKey
}

func (d *DispatchIterator) ResourceType() ([]query.ObjectType, error) {
	return d.subIt.ResourceType()
}

func (d *DispatchIterator) SubjectTypes() ([]query.ObjectType, error) {
	return d.subIt.SubjectTypes()
}

func (d *DispatchIterator) Serialize(w io.Writer) error {
	return query.SerializeWithHeader(w, DispatchIteratorType, d.canonicalKey, func(buf io.Writer) error {
		return d.subIt.Serialize(buf)
	})
}

func deserializeDispatch(body io.Reader, key query.CanonicalKey, dctx *query.DeserializeContext) (query.Iterator, error) {
	sub, err := query.Deserialize(body, dctx)
	if err != nil {
		return nil, fmt.Errorf("dispatch sub: %w", err)
	}
	d := NewDispatchIterator(sub)
	d.canonicalKey = key
	return d, nil
}
