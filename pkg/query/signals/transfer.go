package signals

import (
	"fmt"
	"maps"
	"slices"

	"github.com/ccoveille/go-safecast/v2"

	caveattypes "github.com/authzed/spicedb/pkg/caveats/types"
	"github.com/authzed/spicedb/pkg/diff"
	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	"github.com/authzed/spicedb/pkg/query/signals/signal"
)

type seedEntry struct {
	key   entryKey
	entry *entry
}
type schemaSeed struct {
	definitions *core.StoredSchema
	entries     []seedEntry
}

// seedLocked chooses a single donor; it makes no claim about revision chronology.
func (o *Owner) seedLocked() schemaSeed {
	var donor string
	seq := uint64(0)

	for h, s := range o.schemas {
		if s.used > seq && s.definitions != nil {
			select {
			case <-s.ready:
				donor, seq = h, s.used
			default:
			}
		}
	}

	if donor == "" {
		return schemaSeed{}
	}

	result := schemaSeed{definitions: o.schemas[donor].definitions}

	for k, e := range o.entries {
		d := o.registry.definitions[k.name]
		if k.schema == donor && d.Policy == signal.CrossRevision && d.Transfer != nil {
			result.entries = append(result.entries, seedEntry{k, e})
		}
	}

	return result
}

func diffable(s *core.StoredSchema) diff.DiffableSchema {
	v := s.GetV1()
	return diff.DiffableSchema{
		ObjectDefinitions: slices.Collect(maps.Values(v.NamespaceDefinitions)),
		CaveatDefinitions: slices.Collect(maps.Values(v.CaveatDefinitions)),
	}
}

func (o *Owner) transfer(hash string, target *schemaState, donor schemaSeed) {
	if donor.definitions == nil {
		return
	}

	changes, err := diff.DiffSchemas(diffable(donor.definitions), diffable(target.definitions), caveattypes.Default.TypeSet)
	if err != nil {
		return
	}

	for _, seed := range donor.entries {
		seed.entry.mu.Lock()
		state := seed.entry.state.Clone()
		ev := seed.entry.evidence
		footprint := seed.entry.footprint
		seed.entry.mu.Unlock()
		d := o.registry.definitions[seed.key.name]
		decision := d.Transfer(signal.SchemaChange{Diff: changes}, footprint)

		switch decision.Action {
		case signal.Retain:
		case signal.Discount:
			if decision.Factor <= 0 || decision.Factor >= 1 {
				continue
			}

			discount, ok := state.(interface{ DiscountEvidence(float64) bool })
			if !ok || !discount.DiscountEvidence(decision.Factor) {
				continue
			}
		default:
			continue
		}

		k := seed.key
		k.schema = hash
		cost := 128 + safecast.MustConvert[int64](len(fmt.Sprint(k.key))) + state.Size()
		if cost > o.options.MaxEntryBytes {
			continue
		}

		next := &entry{state: state, evidence: ev, footprint: footprint, cost: cost}
		o.mu.Lock()

		if o.schemas[hash] == target && o.entries[k] == nil {
			o.sequence++
			next.used = o.sequence
			o.entries[k] = next
			o.bytes += cost
			o.trimLocked()
		}

		o.mu.Unlock()
	}
}
