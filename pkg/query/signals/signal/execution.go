package signal

import "encoding/json"

// Operation identifies the execution operation represented by evidence.
type Operation uint8

const (

	// Check identifies a membership check.
	Check Operation = iota + 1

	// IterSubjects identifies subject enumeration.
	IterSubjects

	// IterResources identifies resource enumeration.
	IterResources
)

// Terminal describes how an invocation ended.
type Terminal uint8

const (

	// Complete indicates that the invocation completed normally.
	Complete Terminal = iota

	// ConsumerStopped indicates that enumeration ended before exhaustion.
	ConsumerStopped

	// Canceled indicates that the invocation's context was canceled.
	Canceled

	// Errored indicates that the invocation ended with an error.
	Errored
)

// ExecutionKey identifies an observed execution population independently of dispatch keys.
type ExecutionKey struct {
	// Identity is the full structural identity of the observed subtree.
	Identity string

	// Operation identifies the execution operation represented by evidence.
	Operation Operation

	// Filter identifies the type and relation filters for this population.
	Filter string

	// Mode distinguishes single and batched invocation populations.
	Mode string

	// Strategy identifies the execution strategy used by this population.
	Strategy string

	// Role distinguishes sender wrappers from receiver bodies.
	Role string

	// Dependencies encodes the subtree's transitive schema footprint for compatibility checks.
	Dependencies string
}

// Footprint decodes the schema dependencies recorded in the execution key.
func (k ExecutionKey) Footprint() Footprint {
	var f Footprint
	if err := json.Unmarshal([]byte(k.Dependencies), &f); err != nil {
		return Footprint{}
	}

	return f
}

// EncodeFootprint encodes schema dependencies for use in a comparable execution key.
func EncodeFootprint(f Footprint) string { b, _ := json.Marshal(f); return string(b) }

// Quality provides sample counts and effective weight for evidence metadata.
type Quality interface {
	// SignalSamples returns the number of observations represented by the estimate.
	SignalSamples() uint64

	// SignalWeight returns the effective observation weight of the estimate.
	SignalWeight() float64
}
