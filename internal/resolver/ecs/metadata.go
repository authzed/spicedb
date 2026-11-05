package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	metadataEnvVar       = "ECS_CONTAINER_METADATA_URI_V4"
	metadataTimeout      = 2 * time.Second
	maxMetadataBodyBytes = 1 << 20
)

var metadataClient = &http.Client{Timeout: metadataTimeout}

type taskMetadata struct {
	TaskARN    string `json:"TaskARN"`
	Containers []struct {
		Networks []struct {
			IPv4Addresses []string `json:"IPv4Addresses"`
		} `json:"Networks"`
	} `json:"Containers"`
}

func taskMetadataSelf(ctx context.Context) (Self, error) {
	base := os.Getenv(metadataEnvVar)
	if base == "" {
		return Self{}, fmt.Errorf("%s is not set, the ecs resolver must run inside an ECS task", metadataEnvVar)
	}
	return fetchSelf(ctx, metadataClient, base+"/task")
}

// fetchSelf reads the task metadata document at url. The URL is the metadata
// endpoint that the ECS agent injects into the task, not a value any caller of
// SpiceDB can influence.
func fetchSelf(ctx context.Context, client *http.Client, url string) (Self, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // G704: the URL comes from the ECS agent, see above.
	if err != nil {
		return Self{}, fmt.Errorf("building task metadata request: %w", err)
	}

	resp, err := client.Do(req) //nolint:gosec // G704: the URL comes from the ECS agent, see above.
	if err != nil {
		return Self{}, fmt.Errorf("fetching task metadata: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return Self{}, fmt.Errorf("fetching task metadata: unexpected status %s", resp.Status)
	}

	var md taskMetadata
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxMetadataBodyBytes)).Decode(&md); err != nil {
		return Self{}, fmt.Errorf("decoding task metadata: %w", err)
	}

	for _, container := range md.Containers {
		for _, network := range container.Networks {
			for _, ip := range network.IPv4Addresses {
				if ip != "" {
					return Self{TaskARN: md.TaskARN, IP: ip}, nil
				}
			}
		}
	}
	return Self{}, errors.New("task metadata has no IPv4 address")
}
