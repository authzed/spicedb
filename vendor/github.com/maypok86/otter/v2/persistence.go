// Copyright (c) 2025 Alexey Mayshev and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otter

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// LoadCacheFromFile loads cache data from the given filePath.
//
// See SaveCacheToFile for saving cache data to file.
func LoadCacheFromFile[K comparable, V any](c *Cache[K, V], filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("otter: open file %s: %w", filePath, err)
	}
	//nolint:errcheck // it's ok
	defer file.Close()

	return LoadCacheFrom(c, file)
}

// LoadCacheFrom loads cache data from the given [io.Reader].
//
// See SaveCacheToFile for saving cache data to file.
func LoadCacheFrom[K comparable, V any](c *Cache[K, V], r io.Reader) error {
	dec := gob.NewDecoder(r)

	var savedMaximum uint64
	if err := dec.Decode(&savedMaximum); err != nil {
		return fmt.Errorf("otter: decode maximum: %w", err)
	}

	maximum := min(savedMaximum, c.GetMaximum())
	maximum2 := maximum / 4
	maximum1 := 2 * maximum2
	size := uint64(0)
	for size < maximum {
		var entry Entry[K, V]
		if err := dec.Decode(&entry); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("otter: decode entry: %w", err)
		}

		nowNano := c.cache.clock.NowNano()
		if c.cache.withExpiration && entry.ExpiresAtNano < nowNano {
			continue
		}
		c.Set(entry.Key, entry.Value)
		if c.cache.withExpiration && entry.ExpiresAtNano != unreachableExpiresAt {
			expiresAfter := max(1, time.Duration(entry.ExpiresAtNano-nowNano))
			c.SetExpiresAfter(entry.Key, expiresAfter)
		}
		if c.cache.withRefresh && entry.RefreshableAtNano != unreachableRefreshableAt {
			refreshableAfter := max(1, time.Duration(entry.RefreshableAtNano-nowNano))
			c.SetRefreshableAfter(entry.Key, refreshableAfter)
		}
		size += uint64(entry.Weight)

		if size <= maximum2 {
			c.GetIfPresent(entry.Key)
			c.GetIfPresent(entry.Key)
			continue
		}
		if size <= maximum1 {
			c.GetIfPresent(entry.Key)
			continue
		}
	}

	return nil
}

// SaveCacheToFile atomically saves cache data to the given filePath.
//
// SaveCacheToFile may be called concurrently with other operations on the cache.
//
// The saved data may be loaded with LoadCacheFromFile.
//
// WARNING: Beware that this operation is performed within the eviction policy's exclusive lock.
// While the operation is in progress further eviction maintenance will be halted.
func SaveCacheToFile[K comparable, V any](c *Cache[K, V], filePath string) error {
	// Create dir if it doesn't exist.
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("otter: stat %s: %w", dir, err)
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("otter: create dir %s: %w", dir, err)
		}
	}

	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("otter: create file %s: %w", filePath, err)
	}
	//nolint:errcheck // it's ok
	defer file.Close()

	return SaveCacheTo(c, file)
}

// SaveCacheTo atomically saves cache data to the given [io.Writer].
//
// SaveCacheToFile may be called concurrently with other operations on the cache.
//
// The saved data may be loaded with LoadCacheFrom.
//
// WARNING: Beware that this operation is performed within the eviction policy's exclusive lock.
// While the operation is in progress further eviction maintenance will be halted.
func SaveCacheTo[K comparable, V any](c *Cache[K, V], w io.Writer) error {
	enc := gob.NewEncoder(w)

	maximum := c.GetMaximum()
	if err := enc.Encode(maximum); err != nil {
		return fmt.Errorf("otter: encode maximum: %w", err)
	}

	size := uint64(0)
	for entry := range c.Hottest() {
		if size >= maximum {
			break
		}

		if err := enc.Encode(entry); err != nil {
			return fmt.Errorf("otter: encode entry: %w", err)
		}

		size += uint64(entry.Weight)
	}

	return nil
}
