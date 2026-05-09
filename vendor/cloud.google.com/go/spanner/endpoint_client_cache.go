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
	"sync/atomic"
)

// grpcChannelEndpoint is a channelEndpoint backed by a real gRPC connection.
type grpcChannelEndpoint struct {
	address string
	client  spannerClient
	healthy atomic.Bool
}

func (e *grpcChannelEndpoint) Address() string {
	return e.address
}

func (e *grpcChannelEndpoint) IsHealthy() bool {
	return e.healthy.Load()
}

// endpointClientCache implements channelEndpointCache with actual gRPC
// connections to specific server addresses.
type endpointClientCache struct {
	mu            sync.RWMutex
	endpoints     map[string]*grpcChannelEndpoint
	inflight      map[string]*endpointClientCreation
	clientFactory func(ctx context.Context, address string) (spannerClient, error)
	closed        bool
}

type endpointClientCreation struct {
	done chan struct{}
	ep   channelEndpoint
}

func newEndpointClientCache(clientFactory func(ctx context.Context, address string) (spannerClient, error)) *endpointClientCache {
	return &endpointClientCache{
		endpoints:     make(map[string]*grpcChannelEndpoint),
		inflight:      make(map[string]*endpointClientCreation),
		clientFactory: clientFactory,
	}
}

// Get returns a channelEndpoint for the given address, creating a new gRPC
// connection if one does not already exist. Channel creation is coordinated
// per address so slow dials do not block unrelated cache access.
func (c *endpointClientCache) Get(ctx context.Context, address string) channelEndpoint {
	if ctx == nil {
		ctx = context.Background()
	}
	// Fast path: read lock.
	c.mu.RLock()
	if ep, ok := c.endpoints[address]; ok {
		c.mu.RUnlock()
		return ep
	}
	c.mu.RUnlock()

	c.mu.Lock()
	if ep, ok := c.endpoints[address]; ok {
		c.mu.Unlock()
		return ep
	}
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if creation, ok := c.inflight[address]; ok {
		c.mu.Unlock()
		select {
		case <-creation.done:
			return creation.ep
		case <-ctx.Done():
			return nil
		}
	}
	creation := &endpointClientCreation{done: make(chan struct{})}
	c.inflight[address] = creation
	c.mu.Unlock()

	client, err := c.clientFactory(ctx, address)

	c.mu.Lock()
	delete(c.inflight, address)
	if err == nil && !c.closed {
		ep := &grpcChannelEndpoint{
			address: address,
			client:  client,
		}
		ep.healthy.Store(true)
		c.endpoints[address] = ep
		creation.ep = ep
	}
	shouldCloseClient := c.closed && client != nil
	close(creation.done)
	c.mu.Unlock()

	if shouldCloseClient {
		_ = client.Close()
	}
	return creation.ep
}

// ClientFor resolves a channelEndpoint to the underlying spannerClient.
func (c *endpointClientCache) ClientFor(ep channelEndpoint) spannerClient {
	if ep == nil {
		return nil
	}
	gep, ok := ep.(*grpcChannelEndpoint)
	if !ok {
		return nil
	}
	return gep.client
}

// Close shuts down all cached gRPC connections.
func (c *endpointClientCache) Close() error {
	c.mu.Lock()
	c.closed = true
	defer c.mu.Unlock()
	var firstErr error
	for addr, ep := range c.endpoints {
		if ep.client != nil {
			if err := ep.client.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(c.endpoints, addr)
	}
	return firstErr
}
