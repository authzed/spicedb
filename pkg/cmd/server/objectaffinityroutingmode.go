package server

import "fmt"

// ObjectAffinityRoutingMode controls whether cross-node dispatch is routed by
// resource object.
type ObjectAffinityRoutingMode string

const (
	// ObjectAffinityRoutingModeDisabled routes dispatch by full request hash.
	// This is the default.
	ObjectAffinityRoutingModeDisabled ObjectAffinityRoutingMode = "disabled"

	// ObjectAffinityRoutingModeEnabled routes dispatch by the ring owner of the
	// resource object.
	ObjectAffinityRoutingModeEnabled ObjectAffinityRoutingMode = "enabled"
)

// ParseObjectAffinityRoutingMode converts a string to an ObjectAffinityRoutingMode.
// An empty string means disabled. Returns an error if the string is invalid.
func ParseObjectAffinityRoutingMode(s string) (ObjectAffinityRoutingMode, error) {
	switch ObjectAffinityRoutingMode(s) {
	case "", ObjectAffinityRoutingModeDisabled:
		return ObjectAffinityRoutingModeDisabled, nil
	case ObjectAffinityRoutingModeEnabled:
		return ObjectAffinityRoutingModeEnabled, nil
	default:
		return ObjectAffinityRoutingModeDisabled, fmt.Errorf(
			"invalid object affinity routing mode %q, must be one of: disabled, enabled", s)
	}
}
