// Package queryplan prepares policy-configured plans and reuses immutable Check trees owned by stored schemas.
package queryplan

import (
	"bytes"
	"container/list"
	"context"
	"math"
	"sync"

	"github.com/ccoveille/go-safecast/v2"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/genutil/scopedflight"
	"github.com/authzed/spicedb/pkg/query"
	"github.com/authzed/spicedb/pkg/query/planhint"
	"github.com/authzed/spicedb/pkg/query/queryprofile"
)

// Key identifies the schema-dependent construction and policy of one Check plan.
// Request object IDs, revisions, caveat values and recursion depth are execution
// inputs. They must never be captured by the builder or stored in a compiled plan.
type Key struct {
	// Profile includes the version and resolved execution limits.
	Profile queryprofile.Profile

	// Operation identifies the logical operation; only Check plans are retained.
	Operation query.Operation

	// ResourceType identifies a service plan's resource definition.
	ResourceType string

	// Permission identifies a service plan's relation or permission.
	Permission string

	// Subject identifies the normalized subject type and relation used for planning.
	Subject query.ObjectType

	// SerializedPlan identifies receiver plans by their exact serialized bytes.
	SerializedPlan string
}

var cacheKey = datastore.NewDerivedCacheKey[*planCache]()

type compiledPlan struct {
	iterator query.Iterator
	ledger   *planhint.Ledger
	cost     int64
}

type cacheEntry struct {
	key  Key
	plan *compiledPlan
}

type planCache struct {
	mu      sync.Mutex
	entries map[Key]*list.Element // GUARDED_BY(mu)
	order   *list.List            // GUARDED_BY(mu), most recently used first
	used    int64                 // GUARDED_BY(mu)
	budget  int64
	flights scopedflight.Group[Key, *compiledPlan]
}

func newCache(budget int64) *planCache {
	return &planCache{entries: map[Key]*list.Element{}, order: list.New(), budget: budget}
}

// Load builds and compiles a schema-dependent plan once, then returns its
// read-only iterator and an independent execution ledger. Concurrent misses share
// compilation; failed builds are not retained. The owning stored schema budgets
// and discards the cache. Legacy readers and other profiles build without reuse.
func Load(
	ctx context.Context,
	reader datalayer.SchemaReader,
	key Key,
	build func(context.Context) (query.CanonicalOutline, error),
) (query.Iterator, *planhint.Ledger, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	stored, ok := reader.(interface {
		StoredSchema() *datastore.ReadOnlyStoredSchema
	})
	if !ok || stored.StoredSchema() == nil || !key.Profile.ReuseCompiledPlan(key.Operation) {
		plan, err := compile(ctx, key, build, false)
		if err != nil {
			return nil, nil, err
		}

		return plan.iterator, plan.ledger, nil
	}

	schema := stored.StoredSchema()
	cache, err := datastore.LoadOrStoreDerived(schema, cacheKey, func() *planCache {
		return newCache(schema.CompiledQueryPlanCacheBudget())
	})
	if err != nil {
		return nil, nil, err
	}

	plan, err := cache.load(ctx, key, build)
	if err != nil {
		return nil, nil, err
	}

	// Compilation finishes before publication. Runtime reducers refer to stable
	// hint IDs, but record decisions only in this request's independent ledger.
	return plan.iterator, plan.ledger.Clone(), nil
}

func compile(
	ctx context.Context,
	key Key,
	build func(context.Context) (query.CanonicalOutline, error),
	retain bool,
) (*compiledPlan, error) {
	outline, err := build(ctx)
	if err != nil {
		return nil, err
	}

	if outline.HintLedger == nil {
		outline.HintLedger = planhint.NewLedger(nil)
	}

	iterator, err := outline.Compile()
	if err != nil {
		return nil, err
	}

	plan := &compiledPlan{iterator: iterator, ledger: outline.HintLedger}

	if !retain {
		return plan, nil
	}

	// Outline identities omit embedded path/context payloads. Measure complete
	// iterator wire data instead, and reserve decoded message/map/path overhead.
	// Uncached callers do none of this cache-admission work.
	var wire bytes.Buffer

	if err := iterator.Serialize(&wire); err != nil {
		return nil, err
	}

	cost := int64(
		512 + len(key.SerializedPlan) + len(key.ResourceType) + len(key.Permission) + len(key.Subject.Type) + len(key.Subject.Subrelation),
	)
	cost += safecast.MustConvert[int64](len(outline.CanonicalKeys))*128 + int64(wire.Len())*8
	cost += safecast.MustConvert[int64](len(outline.HintLedger.Hints())) * 384

	for _, canonical := range outline.CanonicalKeys {
		cost += safecast.MustConvert[int64](len(canonical))
	}

	cacheable := true

	if err := query.WalkOutlinePreOrder(outline.Root, func(node query.Outline) error {
		if node.Args == nil {
			return nil
		}

		if node.Args.Caveat != nil {
			cost += messageOverhead(node.Args.Caveat.ProtoReflect())
		}

		for _, path := range node.Args.FixedPaths {
			cost += pathOverhead(&path, &cacheable)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if !cacheable {
		// Arbitrary in-process annotations have no trustworthy byte estimate.
		// They remain executable but cannot enter this bounded cache.
		cost = math.MaxInt64
	}

	plan.cost = cost
	return plan, nil
}

func pathOverhead(path *query.Path, cacheable *bool) int64 {
	cost := int64(512)

	if len(path.Metadata) > 0 {
		*cacheable = false
	}

	if path.Caveat != nil {
		cost += messageOverhead(path.Caveat.ProtoReflect())
	}

	for _, integrity := range path.Integrity {
		cost += messageOverhead(integrity.ProtoReflect())
	}

	for _, excluded := range path.ExcludedSubjects {
		cost += pathOverhead(excluded, cacheable)
	}

	return cost
}

// messageOverhead reserves decoded protobuf objects independently of wire bytes.
// Maps, repeated fields and nested empty messages can allocate substantial heap
// even when their serialization is small. Payload strings are covered by wire cost.
func messageOverhead(message protoreflect.Message) int64 {
	if !message.IsValid() {
		return 0
	}

	cost := int64(128)
	message.Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		cost += 32

		switch {
		case field.IsMap():
			entries := value.Map()
			cost += int64(entries.Len()) * 64

			if field.MapValue().Kind() == protoreflect.MessageKind {
				entries.Range(func(_ protoreflect.MapKey, entry protoreflect.Value) bool {
					cost += messageOverhead(entry.Message())
					return true
				})
			}
		case field.IsList():
			entries := value.List()
			cost += int64(entries.Len()) * 32

			if field.Kind() == protoreflect.MessageKind {
				for index := 0; index < entries.Len(); index++ {
					cost += messageOverhead(entries.Get(index).Message())
				}
			}
		case field.Kind() == protoreflect.MessageKind:
			cost += messageOverhead(value.Message())
		}

		return true
	})

	return cost
}

func (c *planCache) load(ctx context.Context, key Key, build func(context.Context) (query.CanonicalOutline, error)) (*compiledPlan, error) {
	if plan := c.get(key); plan != nil {
		return plan, nil
	}

	plan, _, err := c.flights.Do(ctx, key, func(shared context.Context) (*compiledPlan, error) {
		// Another builder can publish between the initial probe and flight admission.
		if existing := c.get(key); existing != nil {
			return existing, nil
		}

		built, err := compile(shared, key, build, true)
		if err != nil {
			return nil, err
		}

		if err := shared.Err(); err != nil {
			return nil, err
		}

		c.add(key, built)
		return built, nil
	})
	return plan, err
}

func (c *planCache) get(key Key) *compiledPlan {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.entries[key]
	if entry == nil {
		return nil
	}

	c.order.MoveToFront(entry)
	return entry.Value.(cacheEntry).plan
}

func (c *planCache) add(key Key, plan *compiledPlan) {
	if plan.cost > c.budget {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.entries[key] != nil {
		return
	}

	// Admission evicts before publication, keeping the reserved byte allowance
	// bounded even when many different subjects or serialized subplans are used.
	for c.used+plan.cost > c.budget {
		oldest := c.order.Back()
		entry := oldest.Value.(cacheEntry)
		delete(c.entries, entry.key)
		c.order.Remove(oldest)
		c.used -= entry.plan.cost
	}

	c.entries[key] = c.order.PushFront(cacheEntry{key: key, plan: plan})
	c.used += plan.cost
}
