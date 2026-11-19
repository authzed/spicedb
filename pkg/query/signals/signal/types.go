// Package signal defines typed observations and estimates for planning signals.
package signal

import (
	"time"

	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/diff"
)

type (

	// Name identifies a registered signal provider.
	Name string

	// RevisionPolicy controls whether evidence is isolated to an exact datastore revision.
	RevisionPolicy uint8
)

const (

	// RevisionBound isolates evidence to the selected datastore revision.
	RevisionBound RevisionPolicy = iota

	// CrossRevision allows evidence to be shared across revisions within a schema.
	CrossRevision
)

// Availability describes whether an estimate can be used.
type Availability uint8

const (

	// Available indicates that an estimate is usable.
	Available Availability = iota

	// Missing indicates that no usable observations exist.
	Missing

	// Expired indicates that evidence has aged below the usable threshold.
	Expired

	// Evicted indicates that evidence was removed by storage limits.
	Evicted

	// Unsupported indicates that the requested signal or population is unsupported.
	Unsupported
)

// Scope binds evidence to a schema and selected datastore revision.
type Scope struct {
	// Revision is the request-selected datastore revision.
	Revision datastore.Revision

	// SchemaHash identifies the schema used by the request.
	SchemaHash string
}

type (

	// EvidenceID identifies a published snapshot within a source epoch.
	EvidenceID struct {
		// Epoch identifies the source accumulator incarnation.
		Epoch uint64

		// Version is the publication sequence within the epoch.
		Version uint64
	}

	// Evidence describes the provenance and age of an estimate.
	Evidence struct {
		// ID identifies the published evidence snapshot.
		ID EvidenceID

		// Samples counts observations represented by the estimate.
		Samples uint64

		// EffectiveWeight is the weight remaining after aging and discounting.
		EffectiveWeight float64

		// FirstObserved is the earliest observation time represented by the evidence.
		FirstObserved time.Time

		// LastObserved is the latest observation time represented by the evidence.
		LastObserved time.Time

		// PublishedAt is the time the evidence was published.
		PublishedAt time.Time

		// SourceRevision identifies the source revision when available.
		SourceRevision string

		// SourceSchemaHash identifies the schema where the evidence originated.
		SourceSchemaHash string

		// Historical reports whether evidence was derived across revisions.
		Historical bool
	}
)

// Estimate holds a typed value, its availability, and its provenance.
type Estimate[V any] struct {
	// Value is the typed estimate returned by the provider.
	Value V

	// Availability describes whether an estimate can be used.
	Availability Availability

	// Evidence describes the provenance and age of an estimate.
	Evidence Evidence
}

// IsAvailable reports whether the estimate contains a usable value.
func (e Estimate[V]) IsAvailable() bool {
	return e.Availability == Available
}

type (

	// Element identifies a relation or permission in a schema definition.
	Element struct {
		// Namespace is the schema definition name.
		Namespace string

		// Relation is the relation or permission name.
		Relation string
	}

	// Footprint lists the schema dependencies of an evidence population.
	Footprint struct {
		// Elements lists relation and permission dependencies.
		Elements []Element

		// Caveats lists caveat definition dependencies.
		Caveats []string
	}
)

type (

	// SchemaChange contains the diff used to assess evidence compatibility.
	SchemaChange struct {
		// Diff describes changes between the source and destination schemas.
		Diff *diff.SchemaDiff
	}

	// TransferAction specifies how evidence crosses a schema change.
	TransferAction uint8
)

const (

	// Reset discards evidence during schema transfer.
	Reset TransferAction = iota

	// Retain preserves compatible evidence without changing its age.
	Retain

	// Discount preserves evidence with reduced weight.
	Discount
)

// TransferDecision describes a provider's schema compatibility decision.
type TransferDecision struct {
	// Action selects whether to reset, retain, or discount evidence.
	Action TransferAction

	// Factor is the multiplier applied when discounting evidence.
	Factor float64

	// Reason explains the transfer decision.
	Reason string
}

// Unchanged retains evidence only when its transitive schema dependencies did not change.
func Unchanged(change SchemaChange, f Footprint) TransferDecision {
	if change.Diff == nil {
		return TransferDecision{Action: Reset, Reason: "unknown-change"}
	}

	if len(f.Elements) == 0 && len(f.Caveats) == 0 {
		return TransferDecision{Action: Reset, Reason: "unknown-dependencies"}
	}

	d := change.Diff

	for _, e := range f.Elements {
		for _, n := range d.RemovedNamespaces {
			if n == e.Namespace {
				return TransferDecision{Action: Reset, Reason: "removed-definition"}
			}
		}

		if nd, ok := d.ChangedNamespaces[e.Namespace]; ok {
			for _, delta := range nd.Deltas() {
				switch string(delta.Type) {
				case "namespace-comments-changed", "changed-permission-comment", "changed-relation-comment":
					continue
				case "added-relation", "removed-relation", "added-permission", "removed-permission",
					"changed-permission-implementation", "legacy-changed-relation-implementation",
					"relation-allowed-type-added", "relation-allowed-type-removed":
				default:
					return TransferDecision{Action: Reset, Reason: "unknown-change-kind"}
				}

				if delta.RelationName == "" || delta.RelationName == e.Relation {
					return TransferDecision{Action: Reset, Reason: "changed-dependency"}
				}
			}
		}
	}

	for _, name := range f.Caveats {
		for _, n := range d.RemovedCaveats {
			if n == name {
				return TransferDecision{Action: Reset, Reason: "removed-caveat"}
			}
		}

		if cd, ok := d.ChangedCaveats[name]; ok {
			for _, delta := range cd.Deltas() {
				if string(delta.Type) != "caveat-comments-changed" {
					return TransferDecision{Action: Reset, Reason: "changed-caveat"}
				}
			}
		}
	}

	return TransferDecision{Action: Retain, Factor: 1, Reason: "unchanged-dependencies"}
}

// UpdateID identifies an accepted source increment, not a cumulative snapshot.
type UpdateID struct {
	// Epoch identifies the source accumulator incarnation.
	Epoch uint64

	// Sequence identifies the accepted increment within its source epoch.
	Sequence uint64
}

// Discountable supports schema-transfer weighting without changing evidence age.
type Discountable interface {
	// DiscountEvidence reduces existing evidence weight without renewing its age.
	DiscountEvidence(float64)
}
