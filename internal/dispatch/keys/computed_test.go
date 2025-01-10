package keys

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/genutil/mapz"
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
	ONR = tuple.CoreONR
	RR  = tuple.CoreRR
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
			"expand different resource",
			func() DispatchCacheKey {
				return expandRequestToKey(&v1.DispatchExpandRequest{
					ResourceAndRelation: ONR("document", "foo2", "view"),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"9dd1e0c9cba88edc6b",
		},
		{
			"expand different revision",
			func() DispatchCacheKey {
				return expandRequestToKey(&v1.DispatchExpandRequest{
					ResourceAndRelation: ONR("document", "foo2", "view"),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1235",
					},
				}, computeBothHashes)
			},
			"f1b396da87bdeae2bd01",
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
		{
			"lookup resources 2",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
			},
			"f49fbafef9c489abe601",
		},
		{
			"lookup resources 2 with zero limit",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalLimit: 0,
				}, computeBothHashes)
			},
			"f49fbafef9c489abe601",
		},
		{
			"lookup resources 2 with non-zero limit",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalLimit: 42,
				}, computeBothHashes)
			},
			"ea88adb1c1dfa6ebab01",
		},
		{
			"lookup resources 2 with nil context",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: nil,
				}, computeBothHashes)
			},
			"f49fbafef9c489abe601",
		},
		{
			"lookup resources 2 with empty context",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{})
						return v
					}(),
				}, computeBothHashes)
			},
			"f49fbafef9c489abe601",
		},
		{
			"lookup resources 2 with context",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
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
			"bbd78884eff9edbebd01",
		},
		{
			"lookup resources 2 with different context",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
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
			"94d0faa5feaaa4dc17",
		},
		{
			"lookup resources 2 with escaped string",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
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
			"cdffc895cb9299b9da01",
		},
		{
			"lookup resources 2 with nested context",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
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
			"84d8d38ff9fadbb36d",
		},
		{
			"lookup resources 2 with empty cursor",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{},
				}, computeBothHashes)
			},
			"f49fbafef9c489abe601",
		},
		{
			"lookup resources 2 with non-empty cursor",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo"},
					},
				}, computeBothHashes)
			},
			"f09894c0d9a0b8ff7f",
		},
		{
			"lookup resources 2 with different cursor",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				}, computeBothHashes)
			},
			"badfd7c59cdbcef671",
		},
		{
			"lookup resources 2 with different terminal subject",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "sarah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				}, computeBothHashes)
			},
			"8787b685e9cea993a901",
		},
		{
			"lookup resources 2 with different subject IDs",
			func() DispatchCacheKey {
				return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah", "tom"},
					TerminalSubject:  ONR("user", "sarah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				}, computeBothHashes)
			},
			"f4d6d884eaa5e4b4ed01",
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

	// Lookup Resources 2.
	string(lookupPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
				ResourceRelation: resourceRelation,
				SubjectRelation:  subjectRelation,
				SubjectIds:       subjectIds,
				TerminalSubject:  ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				Metadata:         metadata,
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

	dataCombinationSeen := mapz.NewSet[string]()
	stableCacheKeysSeen := mapz.NewSet[string]()
	unstableCacheKeysSeen := mapz.NewSet[uint64]()

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
						t.Run(tuple.StringCoreRR(resourceRelation), func(t *testing.T) {
							for _, subjectRelation := range subjectRelations {
								subjectRelation := subjectRelation
								t.Run(tuple.StringCoreRR(subjectRelation), func(t *testing.T) {
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
	result := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"mariah"},
		TerminalSubject:  ONR("user", "mariah", "..."),
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

	require.Equal(t, "81aab1c790f0be947d", hex.EncodeToString(result.StableSumAsBytes()))
}
