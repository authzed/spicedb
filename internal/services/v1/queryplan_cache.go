package v1

import (
	"context"

	"resenje.org/singleflight"

	"github.com/authzed/spicedb/pkg/cache"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	"github.com/authzed/spicedb/pkg/query"
	schema "github.com/authzed/spicedb/pkg/schema/v2"
)

type queryPlanCache struct {
	schemas      cache.Cache[cache.StringKey, *schema.Schema]
	schemaGroup  singleflight.Group[cache.StringKey, *schema.Schema]
	outlines     cache.Cache[cache.StringKey, query.CanonicalOutline]
	outlineGroup singleflight.Group[cache.StringKey, query.CanonicalOutline]
}

func newQueryPlanCache() *queryPlanCache {
	// MaxCost must account for the per-entry weight added by the cache layer,
	// which includes the key string length on top of the caller-supplied cost
	// of 1. Schema keys are ~64-char hashes; outline keys are ~86+ chars.
	schemas, err := cache.NewStandardCache[cache.StringKey, *schema.Schema](&cache.Config{
		MaxCost: 16 * 100,
	})
	if err != nil {
		panic("failed to create query plan schema cache: " + err.Error())
	}

	outlines, err := cache.NewStandardCache[cache.StringKey, query.CanonicalOutline](&cache.Config{
		MaxCost: 256 * 100,
	})
	if err != nil {
		panic("failed to create query plan outline cache: " + err.Error())
	}

	return &queryPlanCache{
		schemas:  schemas,
		outlines: outlines,
	}
}

func (c *queryPlanCache) getOrBuildOutline(
	ctx context.Context,
	reader datalayer.RevisionedReader,
	schemaHash datalayer.SchemaHash,
	resourceType string,
	permission string,
) (query.CanonicalOutline, error) {
	if schemaHash.IsBypassSentinel() {
		return c.buildOutline(ctx, reader, schemaHash, resourceType, permission)
	}

	outlineKey := cache.StringKey(string(schemaHash) + ":" + resourceType + "#" + permission)
	if co, ok := c.outlines.Get(outlineKey); ok {
		return co, nil
	}

	co, _, err := c.outlineGroup.Do(ctx, outlineKey, func(ctx context.Context) (query.CanonicalOutline, error) {
		if co, ok := c.outlines.Get(outlineKey); ok {
			return co, nil
		}
		co, err := c.buildOutline(ctx, reader, schemaHash, resourceType, permission)
		if err != nil {
			return query.CanonicalOutline{}, err
		}
		c.outlines.Set(outlineKey, co, 1)
		return co, nil
	})
	if err != nil {
		return query.CanonicalOutline{}, err
	}

	return co, nil
}

func (c *queryPlanCache) buildOutline(
	ctx context.Context,
	reader datalayer.RevisionedReader,
	schemaHash datalayer.SchemaHash,
	resourceType string,
	permission string,
) (query.CanonicalOutline, error) {
	fullSchema, err := c.getOrBuildSchema(ctx, reader, schemaHash)
	if err != nil {
		return query.CanonicalOutline{}, err
	}
	return query.BuildOutlineFromSchema(fullSchema, resourceType, permission)
}

func (c *queryPlanCache) getOrBuildSchema(
	ctx context.Context,
	reader datalayer.RevisionedReader,
	schemaHash datalayer.SchemaHash,
) (*schema.Schema, error) {
	if schemaHash.IsBypassSentinel() {
		return c.loadSchema(ctx, reader)
	}

	schemaKey := cache.StringKey(schemaHash)
	if s, ok := c.schemas.Get(schemaKey); ok {
		return s, nil
	}

	s, _, err := c.schemaGroup.Do(ctx, schemaKey, func(ctx context.Context) (*schema.Schema, error) {
		if s, ok := c.schemas.Get(schemaKey); ok {
			return s, nil
		}
		s, err := c.loadSchema(ctx, reader)
		if err != nil {
			return nil, err
		}
		c.schemas.Set(schemaKey, s, 1)
		return s, nil
	})
	if err != nil {
		return nil, err
	}

	return s, nil
}

func (c *queryPlanCache) loadSchema(ctx context.Context, reader datalayer.RevisionedReader) (*schema.Schema, error) {
	sr, err := reader.ReadSchema(ctx)
	if err != nil {
		return nil, err
	}

	namespaces, err := sr.ListAllTypeDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	caveats, err := sr.ListAllCaveatDefinitions(ctx)
	if err != nil {
		return nil, err
	}

	return schema.BuildSchemaFromDefinitions(
		datastore.DefinitionsOf(namespaces),
		datastore.DefinitionsOf(caveats),
	)
}

func (c *queryPlanCache) Close() {
	c.schemas.Close()
	c.outlines.Close()
}
