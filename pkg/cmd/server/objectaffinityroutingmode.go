package server

import (
	"fmt"
	"math"
)

// ObjectAffinityRoutingMode selects whether cross-node dispatch routes by
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
// An empty string means disabled. It returns an error for an invalid string.
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

// validateObjectSpread returns an error for a negative or NaN spread share or latency factor.
// It returns a warning for each spread setting that has no effect.
func validateObjectSpread(mode ObjectAffinityRoutingMode, share float64, spread uint8, latencyFactor float64) ([]string, error) {
	if share < 0 || math.IsNaN(share) {
		return nil, fmt.Errorf("experimental-dispatch-object-spread-share must not be negative, got %v", share)
	}

	if latencyFactor < 0 || math.IsNaN(latencyFactor) {
		return nil, fmt.Errorf("experimental-dispatch-object-spread-latency-factor must not be negative, got %v", latencyFactor)
	}

	if share == 0 {
		return nil, nil
	}

	var warnings []string
	if mode != ObjectAffinityRoutingModeEnabled {
		warnings = append(warnings,
			"experimental-dispatch-object-spread-share has no effect because experimental-object-affinity-routing is disabled")
	}

	if spread < 2 {
		warnings = append(warnings,
			"experimental-dispatch-object-spread-share has no effect because experimental-dispatch-object-spread is less than 2")
	}
	return warnings, nil
}
