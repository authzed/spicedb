package signals

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"weak"

	"github.com/ccoveille/go-safecast/v2"

	"github.com/authzed/spicedb/pkg/datastore"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type entryKey struct {
	schema   string
	name     signal.Name
	revision string
	key      any
}

type entry struct {
	publicationMu sync.Mutex
	mu            sync.Mutex
	state         signal.State    // GUARDED_BY(mu)
	evidence      signal.Evidence // GUARDED_BY(publicationMu)
	footprint     signal.Footprint
	cost          int64
	used          uint64
}

type schemaState struct {
	ready       chan struct{}
	definitions *core.StoredSchema
	cost        int64
	used        uint64
}

// Owner isolates a bounded learning store for one datalayer/server.
type Owner struct {
	id       uint64
	mu       sync.Mutex
	registry *Registry
	options  Options
	entries  map[entryKey]*entry // GUARDED_BY(mu)
	schemas  map[string]*schemaState
	enabled  map[signal.Name]bool
	bytes    int64
	sequence uint64
}

var (
	epochs          atomic.Uint64
	schemaHandleKey = datastore.NewDerivedCacheKey[*schemaHandles]()
)

type schemaHandles struct {
	mu     sync.Mutex
	owners map[uint64]weak.Pointer[Owner] // GUARDED_BY(mu)
}

func (h *schemaHandles) bind(owner *Owner) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for id, p := range h.owners {
		if p.Value() == nil {
			delete(h.owners, id)
		}
	}

	if _, exists := h.owners[owner.id]; exists {
		return
	}

	for len(h.owners) >= owner.options.MaxSchemaHandles {
		oldest := ^uint64(0)

		for id := range h.owners {
			oldest = min(oldest, id)
		}

		delete(h.owners, oldest)
	}

	h.owners[owner.id] = weak.Make(owner)
}

// NewOwner creates a bounded signal store from a validated registry and learning profile.
func NewOwner(r *Registry, opts Options) (*Owner, error) {
	if r == nil {
		return nil, errors.New("nil signal registry")
	}

	if !r.frozen {
		if err := r.Freeze(); err != nil {
			return nil, err
		}
	}

	opts = opts.defaults()
	if opts.MaxBytes < 1 || opts.MaxEntryBytes < 1 || opts.MaxSchemas < 1 || opts.MaxRevisions < 1 || opts.MaxSchemaHandles < 1 {
		return nil, errors.New("invalid signal storage limits")
	}

	o := &Owner{
		id:       epochs.Add(1),
		registry: r,
		options:  opts,
		entries:  map[entryKey]*entry{},
		schemas:  map[string]*schemaState{},
		enabled:  map[signal.Name]bool{},
	}
	var enable func(signal.Name) error
	enable = func(n signal.Name) error {
		d, ok := r.definitions[n]
		if !ok {
			return fmt.Errorf("unknown enabled signal %q", n)
		}

		if o.enabled[n] {
			return nil
		}

		o.enabled[n] = true

		for _, dep := range d.Dependencies {
			if err := enable(dep); err != nil {
				return err
			}
		}

		return nil
	}

	if opts.Enabled == nil {
		for n := range r.definitions {
			if err := enable(n); err != nil {
				return nil, err
			}
		}
	} else {
		for _, n := range opts.Enabled {
			if err := enable(signal.Name(n)); err != nil {
				return nil, err
			}
		}
	}

	return o, nil
}

// Bind attaches the owner to a loaded schema and returns a revision-scoped view.
func (o *Owner) Bind(s *datastore.ReadOnlyStoredSchema, scope signal.Scope) (*View, error) {
	if s == nil || s.Get().GetV1() == nil || scope.SchemaHash == "" ||
		s.Get().GetV1().SchemaHash != scope.SchemaHash || scope.Revision == nil || scope.Revision == datastore.NoRevision {
		return nil, errors.New("invalid signal schema/revision binding")
	}

	handles, err := datastore.LoadOrStoreDerived(
		s,
		schemaHandleKey,
		func() *schemaHandles { return &schemaHandles{owners: map[uint64]weak.Pointer[Owner]{}} },
	)
	if err != nil {
		return nil, err
	}

	handles.bind(o)
	o.mu.Lock()
	o.sequence++

	if ss, ok := o.schemas[scope.SchemaHash]; ok {
		ss.used = o.sequence
		ready := ss.ready
		o.mu.Unlock()

		if ready != nil {
			<-ready
		}

		return &View{owner: o, scope: scope}, nil
	}

	cost := s.EstimatedSize() + 256
	if cost > o.options.MaxBytes {
		o.mu.Unlock()
		return &View{owner: o, scope: scope}, nil
	}

	donor := o.seedLocked()

	for len(o.schemas) >= o.options.MaxSchemas {
		o.evictSchemaLocked()
	}

	ss := &schemaState{definitions: s.Get(), cost: cost, used: o.sequence, ready: make(chan struct{})}
	o.schemas[scope.SchemaHash] = ss
	o.bytes += cost
	o.trimLocked()
	o.mu.Unlock()
	o.transfer(scope.SchemaHash, ss, donor)
	close(ss.ready)
	return &View{owner: o, scope: scope}, nil
}

// Local returns an isolated view for legacy schema readers without a shared handle.
func (o *Owner) Local(scope signal.Scope) *View {
	local, _ := NewOwner(o.registry, o.options)
	scope.SchemaHash = fmt.Sprintf("local-%d", epochs.Add(1))
	local.schemas[scope.SchemaHash] = &schemaState{}
	return &View{owner: local, scope: scope}
}

// ResidentBytes returns the store's estimated resident storage usage in bytes.
func (o *Owner) ResidentBytes() int64 { o.mu.Lock(); defer o.mu.Unlock(); return o.bytes }

func (o *Owner) dropLocked(k entryKey) {
	if e := o.entries[k]; e != nil {
		o.bytes -= e.cost
		delete(o.entries, k)
	}
}

func (o *Owner) evictSchemaLocked() {
	var oldest string
	seq := ^uint64(0)

	for h, s := range o.schemas {
		if s.used < seq {
			oldest, seq = h, s.used
		}
	}

	if s := o.schemas[oldest]; s != nil {
		o.bytes -= s.cost
		delete(o.schemas, oldest)

		for k := range o.entries {
			if k.schema == oldest {
				o.dropLocked(k)
			}
		}
	}
}

func (o *Owner) trimLocked() {
	for o.bytes > o.options.MaxBytes {
		if len(o.entries) == 0 {
			o.evictSchemaLocked()
			continue
		}

		var oldest entryKey
		seq := ^uint64(0)

		for k, e := range o.entries {
			if e.used < seq {
				oldest, seq = k, e.used
			}
		}

		o.dropLocked(oldest)
	}
}

func (o *Owner) binding(name signal.Name, kt, vt reflect.Type, write bool) (signal.Descriptor, error) {
	d, ok := o.registry.definitions[name]
	if !ok {
		return d, fmt.Errorf("unknown signal %q", name)
	}

	expected := d.ValueType

	if write {
		expected = d.ObservationType
	}

	if kt != d.KeyType || vt != expected {
		return d, fmt.Errorf("wrong signal type binding for %q", name)
	}

	if write && d.Source != "" {
		return d, fmt.Errorf("cannot observe derived signal %q", name)
	}

	return d, nil
}

func (o *Owner) key(scope signal.Scope, d signal.Descriptor, key any) entryKey {
	rev := ""

	if d.Policy == signal.RevisionBound {
		if scope.Revision != nil {
			rev = scope.Revision.String()
		}
	}

	return entryKey{schema: scope.SchemaHash, name: d.Name, revision: rev, key: key}
}

func (o *Owner) touch(k entryKey, create bool, d signal.Descriptor) *entry {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, ok := o.schemas[k.schema]; !ok {
		return nil
	}

	if e := o.entries[k]; e != nil {
		o.sequence++
		e.used = o.sequence
		return e
	}

	if !create {
		return nil
	}

	// Construct private provider state without the shared owner mutex.
	o.mu.Unlock()
	var state signal.State

	if d.Build != nil {
		state = d.Build(o.options.Now)
	} else {
		state = d.New()
	}

	cost := 128 + safecast.MustConvert[int64](len(fmt.Sprint(k.key))) + state.Size()
	o.mu.Lock()

	if _, ok := o.schemas[k.schema]; !ok {
		return nil
	}

	if existing := o.entries[k]; existing != nil {
		o.sequence++
		existing.used = o.sequence
		return existing
	}

	if cost > o.options.MaxEntryBytes {
		return nil
	}

	if k.revision != "" {
		revs := map[string]uint64{}

		for existing, e := range o.entries {
			if existing.schema == k.schema && existing.revision != "" {
				revs[existing.revision] = max(revs[existing.revision], e.used)
			}
		}

		if _, found := revs[k.revision]; !found && len(revs) >= o.options.MaxRevisions {
			old := ""
			seq := ^uint64(0)

			for r, s := range revs {
				if s < seq {
					old, seq = r, s
				}
			}

			for existing := range o.entries {
				if existing.schema == k.schema && existing.revision == old {
					o.dropLocked(existing)
				}
			}
		}
	}

	o.sequence++
	e := &entry{
		state: state,
		used:  o.sequence,
		evidence: signal.Evidence{
			ID:               signal.EvidenceID{Epoch: epochs.Add(1)},
			SourceSchemaHash: k.schema,
			SourceRevision:   k.revision,
			Historical:       d.Policy == signal.CrossRevision,
		},
	}
	e.cost = cost

	if e.cost > o.options.MaxEntryBytes {
		return nil
	}

	o.entries[k] = e
	o.bytes += e.cost
	o.trimLocked()

	if o.entries[k] != e {
		return nil
	}

	return e
}

func (o *Owner) read(scope signal.Scope, d signal.Descriptor, key any) (any, signal.Availability, signal.Evidence, error) {
	if !o.enabled[d.Name] {
		return nil, signal.Unsupported, signal.Evidence{}, nil
	}

	e := o.touch(o.key(scope, d, key), false, d)
	if e == nil {
		return nil, signal.Missing, signal.Evidence{}, nil
	}

	e.mu.Lock()
	s := e.state.Clone()
	ev := e.evidence
	e.mu.Unlock()
	value, availability := s.Value(o.options.Now())
	if q, ok := value.(signal.Quality); ok {
		ev.Samples = q.SignalSamples()
		ev.EffectiveWeight = q.SignalWeight()
	}

	return value, availability, ev, nil
}

func (o *Owner) write(scope signal.Scope, d signal.Descriptor, key, observation any) error {
	if !o.enabled[d.Name] || (d.Accept != nil && !d.Accept(key, observation)) {
		return nil
	}

	k := o.key(scope, d, key)
	e := o.touch(k, true, d)
	if e == nil {
		return nil
	}

	e.publicationMu.Lock()
	defer e.publicationMu.Unlock()
	now := o.options.Now()
	observed := now

	if d.ObservationTime != nil {
		if at := d.ObservationTime(observation); !at.IsZero() {
			observed = at
		}
	}

	e.mu.Lock()

	if err := e.state.Add(observation); err != nil {
		e.mu.Unlock()
		return err
	}

	e.evidence.Samples++
	e.evidence.EffectiveWeight = float64(e.evidence.Samples)

	if e.evidence.FirstObserved.IsZero() || observed.Before(e.evidence.FirstObserved) {
		e.evidence.FirstObserved = observed
	}

	if observed.After(e.evidence.LastObserved) {
		e.evidence.LastObserved = observed
	}

	e.evidence.PublishedAt = now
	e.evidence.ID.Version++
	update := signal.UpdateID{Epoch: e.evidence.ID.Epoch, Sequence: e.evidence.ID.Version}

	if d.Footprint != nil {
		e.footprint = d.Footprint(key)
	}

	cost := 128 + safecast.MustConvert[int64](len(fmt.Sprint(key))) + e.state.Size()
	e.mu.Unlock()
	o.mu.Lock()
	resident := o.entries[k] == e
	if resident {
		if cost > o.options.MaxEntryBytes {
			o.dropLocked(k)
		} else {
			o.bytes += cost - e.cost
			e.cost = cost
			o.trimLocked()
		}
	}

	o.mu.Unlock()

	if resident {
		if d.Stamp != nil {
			observation = d.Stamp(observation, update, observed)
		}

		for _, n := range o.registry.order {
			derived := o.registry.definitions[n]
			if derived.Source == d.Name {
				if err := o.write(scope, derived, key, observation); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
