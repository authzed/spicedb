//go:build !wasm

package v1

import (
	"encoding/hex"
	"maps"
	"slices"
	"sort"

	"github.com/cespare/xxhash/v2"
)

func computeAPICallHash(apiName string, arguments map[string]string) (string, error) {
	hasher := xxhash.New()
	_, err := hasher.WriteString(apiName)
	if err != nil {
		return "", err
	}

	_, err = hasher.WriteString(":")
	if err != nil {
		return "", err
	}

	keys := slices.Collect(maps.Keys(arguments))
	sort.Strings(keys)

	for _, key := range keys {
		_, err = hasher.WriteString(key)
		if err != nil {
			return "", err
		}

		_, err = hasher.WriteString(":")
		if err != nil {
			return "", err
		}

		_, err = hasher.WriteString(arguments[key])
		if err != nil {
			return "", err
		}

		_, err = hasher.WriteString(";")
		if err != nil {
			return "", err
		}
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}
