package keys

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/util"

	"github.com/stretchr/testify/require"

	core "github.com/authzed/spicedb/pkg/proto/core/v1"
	v1 "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestKeyPrefixOverlap(t *testing.T) {
	encountered := map[string]struct{}{}
	for _, prefix := range cachePrefixes {
		_, ok := encountered[string(prefix)]
		require.False(t, ok)
		encountered[string(prefix)] = struct{}{}
	}
}

var (
	ONR = tuple.ObjectAndRelation
	RR  = tuple.RelationReference
)

func TestStableCacheKeys(t *testing.T) {
	tcs := []struct {
		name      string
		createKey func() DispatchCacheKey
		expected  string
	}{
		{
			"basic check",
			func() DispatchCacheKey {
				return checkRequestToKey(&v1.DispatchCheckRequest{
					ResourceRelation: RR("document", "view"),
					ResourceIds:      []string{"foo", "bar"},
					Subject:          ONR("user", "tom", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"e09cbca18290f7afae01",
		},
		{
			"basic check with canonical ordering",
			func() DispatchCacheKey {
				return checkRequestToKey(&v1.DispatchCheckRequest{
					ResourceRelation: RR("document", "view"),
					ResourceIds:      []string{"bar", "foo"},
					Subject:          ONR("user", "tom", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"e09cbca18290f7afae01",
		},
		{
			"different check",
			func() DispatchCacheKey {
				return checkRequestToKey(&v1.DispatchCheckRequest{
					ResourceRelation: RR("document", "edit"),
					ResourceIds:      []string{"foo"},
					Subject:          ONR("user", "sarah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "123456",
					},
				}, computeBothHashes)
			},
			"d586cee091f9e591c301",
		},
		{
			"canonical check",
			func() DispatchCacheKey {
				key, _ := checkRequestToKeyWithCanonical(&v1.DispatchCheckRequest{
					ResourceRelation: RR("document", "view"),
					ResourceIds:      []string{"foo", "bar"},
					Subject:          ONR("user", "tom", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, "view")
				return key
			},
			"a1ebd1d6a7a8b18fff01",
		},
		{
			"expand",
			func() DispatchCacheKey {
				return expandRequestToKey(&v1.DispatchExpandRequest{
					ResourceAndRelation: ONR("document", "foo", "view"),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"8afff68e91a7cbb3ef01",
		},
		{
			"lookup resources",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"87b8e4dcf893f4abd701",
		},
		{
			"lookup resources with nil context",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: nil,
				}, computeBothHashes)
			},
			"87b8e4dcf893f4abd701",
		},
		{
			"lookup resources with empty context",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{})
						return v
					}(),
				}, computeBothHashes)
			},
			"87b8e4dcf893f4abd701",
		},
		{
			"lookup resources with context",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": 1,
							"bar": true,
						})
						return v
					}(),
				}, computeBothHashes)
			},
			"8a9bd5bba3ba9cde9301",
		},
		{
			"lookup resources with different context",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": 2,
							"bar": true,
						})
						return v
					}(),
				}, computeBothHashes)
			},
			"f6db868dc194c19ade01",
		},
		{
			"lookup resources with escaped string",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": "this is an `escaped` string\nhi",
						})
						return v
					}(),
				}, computeBothHashes)
			},
			"f98bb6f7fce8eb9ecc01",
		},
		{
			"lookup resources with nested context",
			func() DispatchCacheKey {
				return lookupRequestToKey(&v1.DispatchLookupRequest{
					ObjectRelation: RR("document", "view"),
					Subject:        ONR("user", "mariah", "..."),
					Limit:          10,
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": 1,
							"bar": map[string]any{
								"meh": "hiya",
								"baz": "yo",
							},
						})
						return v
					}(),
				}, computeBothHashes)
			},
			"e0d8d0e099d68b96fa01",
		},
		{
			"reachable resources",
			func() DispatchCacheKey {
				return reachableResourcesRequestToKey(&v1.DispatchReachableResourcesRequest{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah", "tom"},
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"e8848b9dd68f93a6c801",
		},
		{
			"lookup subjects",
			func() DispatchCacheKey {
				return lookupSubjectsRequestToKey(&v1.DispatchLookupSubjectsRequest{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					ResourceIds:      []string{"mariah", "tom"},
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"d699c5b5d3a6dfade601",
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			key := tc.createKey()
			require.Equal(t, tc.expected, hex.EncodeToString(key.StableSumAsBytes()))
		})
	}
}

type generatorFunc func(
	resourceIds []string,
	subjectIds []string,
	resourceRelation *core.RelationReference,
	subjectRelation *core.RelationReference,
	revision *v1.ResolverMeta,
) (DispatchCacheKey, []string)

var generatorFuncs = map[string]generatorFunc{
	// Check.
	string(checkViaRelationPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return checkRequestToKey(&v1.DispatchCheckRequest{
				ResourceRelation: resourceRelation,
				ResourceIds:      resourceIds,
				Subject:          ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				Metadata:         metadata,
			}, computeBothHashes), []string{
				resourceRelation.Namespace,
				resourceRelation.Relation,
				subjectRelation.Namespace,
				subjectIds[0],
				subjectRelation.Relation,
			}
	},

	// Canonical Check.
	string(checkViaCanonicalPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		key, _ := checkRequestToKeyWithCanonical(&v1.DispatchCheckRequest{
			ResourceRelation: resourceRelation,
			ResourceIds:      resourceIds,
			Subject:          ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
			Metadata:         metadata,
		}, resourceRelation.Relation)
		return key, append([]string{
			resourceRelation.Namespace,
			resourceRelation.Relation,
			subjectRelation.Namespace,
			subjectIds[0],
			subjectRelation.Relation,
		}, resourceIds...)
	},

	// Lookup Resources.
	string(lookupPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return lookupRequestToKey(&v1.DispatchLookupRequest{
				ObjectRelation: resourceRelation,
				Subject:        ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				Metadata:       metadata,
			}, computeBothHashes), []string{
				resourceRelation.Namespace,
				resourceRelation.Relation,
				subjectRelation.Namespace,
				subjectIds[0],
				subjectRelation.Relation,
			}
	},

	// Expand.
	string(expandPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return expandRequestToKey(&v1.DispatchExpandRequest{
				ResourceAndRelation: ONR(resourceRelation.Namespace, resourceIds[0], resourceRelation.Relation),
				Metadata:            metadata,
			}, computeBothHashes), []string{
				resourceRelation.Namespace,
				resourceIds[0],
				resourceRelation.Relation,
			}
	},

	// Reachable Resources.
	string(reachableResourcesPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return reachableResourcesRequestToKey(&v1.DispatchReachableResourcesRequest{
				ResourceRelation: resourceRelation,
				SubjectRelation:  subjectRelation,
				SubjectIds:       subjectIds,
				Metadata:         metadata,
			}, computeBothHashes), append([]string{
				resourceRelation.Namespace,
				resourceRelation.Relation,
				subjectRelation.Namespace,
				subjectRelation.Relation,
			}, subjectIds...)
	},

	// Lookup Subjects.
	string(lookupSubjectsPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return lookupSubjectsRequestToKey(&v1.DispatchLookupSubjectsRequest{
				ResourceRelation: resourceRelation,
				SubjectRelation:  subjectRelation,
				ResourceIds:      resourceIds,
				Metadata:         metadata,
			}, computeBothHashes), append([]string{
				resourceRelation.Namespace,
				resourceRelation.Relation,
				subjectRelation.Namespace,
				subjectRelation.Relation,
			}, resourceIds...)
	},
}

func TestCacheKeyNoOverlap(t *testing.T) {
	allResourceIds := [][]string{
		{"1"},
		{"1", "2"},
		{"1", "2", "3"},
		{"hi"},
	}

	allSubjectIds := [][]string{
		{"tom"},
		{"mariah", "tom"},
		{"sarah", "mariah", "tom"},
	}

	resourceRelations := []*core.RelationReference{
		RR("document", "view"),
		RR("document", "viewer"),
		RR("document", "edit"),
		RR("folder", "view"),
	}

	subjectRelations := []*core.RelationReference{
		RR("user", "..."),
		RR("user", "token"),
		RR("folder", "parent"),
		RR("group", "member"),
	}

	revisions := []string{"1234", "4567", "1235"}

	dataCombinationSeen := util.NewSet[string]()
	stableCacheKeysSeen := util.NewSet[string]()
	unstableCacheKeysSeen := util.NewSet[uint64]()

	// Ensure all key functions are generated.
	require.Equal(t, len(generatorFuncs), len(cachePrefixes))

	for _, resourceIds := range allResourceIds {
		resourceIds := resourceIds
		t.Run(strings.Join(resourceIds, ","), func(t *testing.T) {
			for _, subjectIds := range allSubjectIds {
				subjectIds := subjectIds
				t.Run(strings.Join(subjectIds, ","), func(t *testing.T) {
					for _, resourceRelation := range resourceRelations {
						resourceRelation := resourceRelation
						t.Run(tuple.StringRR(resourceRelation), func(t *testing.T) {
							for _, subjectRelation := range subjectRelations {
								subjectRelation := subjectRelation
								t.Run(tuple.StringRR(subjectRelation), func(t *testing.T) {
									for _, revision := range revisions {
										revision := revision
										t.Run(revision, func(t *testing.T) {
											metadata := &v1.ResolverMeta{
												AtRevision: revision,
											}

											for prefix, f := range generatorFuncs {
												prefix := prefix
												f := f
												t.Run(prefix, func(t *testing.T) {
													generated, usedData := f(resourceIds, subjectIds, resourceRelation, subjectRelation, metadata)
													usedDataString := fmt.Sprintf("%s:%s", prefix, strings.Join(usedData, ","))
													if dataCombinationSeen.Add(usedDataString) {
														require.True(t, stableCacheKeysSeen.Add(hex.EncodeToString((generated.StableSumAsBytes()))))
														require.True(t, unstableCacheKeysSeen.Add(generated.processSpecificSum))
													}
												})
											}
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func TestComputeOnlyStableHash(t *testing.T) {
	result := checkRequestToKey(&v1.DispatchCheckRequest{
		ResourceRelation: RR("document", "view"),
		ResourceIds:      []string{"foo", "bar"},
		Subject:          ONR("user", "tom", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
		},
	}, computeOnlyStableHash)

	require.Equal(t, uint64(0), result.processSpecificSum)
}

func TestComputeContextHash(t *testing.T) {
	result := lookupRequestToKey(&v1.DispatchLookupRequest{
		ObjectRelation: RR("document", "view"),
		Subject:        ONR("user", "mariah", "..."),
		Limit:          10,
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
		},
		Context: func() *structpb.Struct {
			v, _ := structpb.NewStruct(map[string]any{
				"null": nil,
				"list": []any{
					1, true, "3",
				},
				"nested": map[string]any{
					"a": "hi",
					"b": "hello",
					"c": 123,
				},
			})
			return v
		}(),
	}, computeBothHashes)

	require.Equal(t, "82b4a3a3c5e3ecf1df01", hex.EncodeToString(result.StableSumAsBytes()))
}
