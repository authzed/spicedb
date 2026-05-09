/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	"sync"
)

// channelEndpoint represents a routable server endpoint.
type channelEndpoint interface {
	Address() string
	IsHealthy() bool
}

// channelEndpointCache caches endpoints by server address.
type channelEndpointCache interface {
	Get(ctx context.Context, address string) channelEndpoint
	ClientFor(ep channelEndpoint) spannerClient
	Close() error
}

type passthroughChannelEndpoint struct {
	address string
}

func (e *passthroughChannelEndpoint) Address() string {
	return e.address
}

func (*passthroughChannelEndpoint) IsHealthy() bool {
	return true
}

type passthroughChannelEndpointCache struct {
	mu        sync.Mutex
	endpoints map[string]*passthroughChannelEndpoint
}

func newPassthroughChannelEndpointCache() *passthroughChannelEndpointCache {
	return &passthroughChannelEndpointCache{endpoints: make(map[string]*passthroughChannelEndpoint)}
}

func (c *passthroughChannelEndpointCache) Get(_ context.Context, address string) channelEndpoint {
	c.mu.Lock()
	defer c.mu.Unlock()
	if endpoint, ok := c.endpoints[address]; ok {
		return endpoint
	}
	endpoint := &passthroughChannelEndpoint{address: address}
	c.endpoints[address] = endpoint
	return endpoint
}

func (c *passthroughChannelEndpointCache) ClientFor(_ channelEndpoint) spannerClient {
	return nil
}

func (c *passthroughChannelEndpointCache) Close() error {
	return nil
}
