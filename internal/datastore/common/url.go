package common

import (
	"errors"
	"net/url"
)

// MetricsIDFromURL extracts the metrics ID from a given datastore URL.
func MetricsIDFromURL(dsURL string) (string, error) {
	if dsURL == "" {
		return "", errors.New("datastore URL is empty")
	}

	u, err := url.Parse(dsURL)
	if err != nil {
		return "", errors.New("could not parse datastore URL")
	}
	return u.Host + u.Path, nil
}
