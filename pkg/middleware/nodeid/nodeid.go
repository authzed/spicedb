package nodeid

import (
	"encoding/hex"
	"os"

	"github.com/cespare/xxhash/v2"
	"github.com/rs/zerolog/log"
)

const spiceDBPrefix = "spicedb:"

var computedNodeID string

func init() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Warn().Err(err).Msg("failed to get hostname, using an empty node ID")
		return
	}

	// Hash the hostname to get the final default node ID.
	hasher := xxhash.New()
	if _, err := hasher.WriteString(hostname); err != nil {
		log.Warn().Err(err).Msg("failed to hash hostname, using an empty node ID")
		return
	}

	computedNodeID = spiceDBPrefix + hex.EncodeToString(hasher.Sum(nil))
}

// Get returns the hostname.
func Get() string {
	return computedNodeID
}
