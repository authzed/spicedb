package v1

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"
	"sort"
)

func computeAPICallHash(apiName string, arguments map[string]string) (string, error) {
	h := sha256.New()

	_, err := h.Write([]byte(apiName))
	if err != nil {
		return "", err
	}

	_, err = h.Write([]byte(":"))
	if err != nil {
		return "", err
	}

	keys := slices.Collect(maps.Keys(arguments))
	sort.Strings(keys)

	for _, key := range keys {
		_, err = h.Write([]byte(key))
		if err != nil {
			return "", err
		}

		_, err = h.Write([]byte(":"))
		if err != nil {
			return "", err
		}

		_, err = h.Write([]byte(arguments[key]))
		if err != nil {
			return "", err
		}

		_, err = h.Write([]byte(";"))
		if err != nil {
			return "", err
		}
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}
