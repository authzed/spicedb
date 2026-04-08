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
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
				}, computeBothHashes)
				return key
			},
			"9884bbb3acd3b3ca1a",
		},
		{
			"lookup resources 2 with zero limit",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalLimit: 0,
				}, computeBothHashes)
				return key
			},
			"9884bbb3acd3b3ca1a",
		},
		{
			"lookup resources 2 with non-zero limit",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalLimit: 42,
				}, computeBothHashes)
				return key
			},
			"dba285cdd9caeef36e",
		},
		{
			"lookup resources 2 with nil context",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					Context: nil,
				}, computeBothHashes)
				return key
			},
			"9884bbb3acd3b3ca1a",
		},
		{
			"lookup resources 2 with empty context",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"9884bbb3acd3b3ca1a",
		},
		{
			"lookup resources 2 with context",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"a3dad09ce9d690b78401",
		},
		{
			"lookup resources 2 with different context",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"f6d4bc92bae9e9d64b",
		},
		{
			"lookup resources 2 with escaped string",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"a0aebfb9a8abd1b802",
		},
		{
			"lookup resources 2 with nested context",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"8e8ddfd8affeecc918",
		},
		{
			"lookup resources 2 with empty cursor",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
					ResourceRelation: RR("document", "view"),
					SubjectRelation:  RR("user", "..."),
					SubjectIds:       []string{"mariah"},
					TerminalSubject:  ONR("user", "mariah", "..."),
					Metadata: &v1.ResolverMeta{
						AtRevision: "1234",
					},
					OptionalCursor: &v1.Cursor{},
				}, computeBothHashes)
				return key
			},
			"9884bbb3acd3b3ca1a",
		},
		{
			"lookup resources 2 with non-empty cursor",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"9e82ddefb6ccbfd6aa01",
		},
		{
			"lookup resources 2 with different cursor",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"e593e789a89a9acd13",
		},
		{
			"lookup resources 2 with different terminal subject",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"f6cf8df7bdc7959520",
		},
		{
			"lookup resources 2 with different subject IDs",
			func() DispatchCacheKey {
				key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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
				return key
			},
			"de839ec8eea2f7bf19",
		},
	}

	for _, tc := range tcs {
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
		key, _ := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
			ResourceRelation: resourceRelation,
			SubjectRelation:  subjectRelation,
			SubjectIds:       subjectIds,
			TerminalSubject:  ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
			Metadata:         metadata,
		}, computeBothHashes)
		return key, []string{
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

	// Plan Check.
	string(planCheckPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return planCheckRequestToKey(&v1.DispatchQueryPlanRequest{
				CanonicalKey: resourceRelation.Relation,
				Resource:     ONR(resourceRelation.Namespace, resourceIds[0], resourceRelation.Relation),
				Subject:      ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				PlanContext:  &v1.PlanContext{Revision: metadata.AtRevision},
			}, computeBothHashes), []string{
				resourceRelation.Relation,
				resourceRelation.Namespace,
				resourceIds[0],
				subjectRelation.Namespace,
				subjectIds[0],
				subjectRelation.Relation,
			}
	},

	// Plan Lookup Resources.
	string(planLookupResourcesPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return planLookupResourcesRequestToKey(&v1.DispatchQueryPlanRequest{
				CanonicalKey: resourceRelation.Relation,
				Subject:      ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				PlanContext:  &v1.PlanContext{Revision: metadata.AtRevision},
			}, computeBothHashes), []string{
				resourceRelation.Relation,
				subjectRelation.Namespace,
				subjectIds[0],
				subjectRelation.Relation,
			}
	},

	// Plan Lookup Subjects.
	string(planLookupSubjectsPrefix): func(
		resourceIds []string,
		subjectIds []string,
		resourceRelation *core.RelationReference,
		subjectRelation *core.RelationReference,
		metadata *v1.ResolverMeta,
	) (DispatchCacheKey, []string) {
		return planLookupSubjectsRequestToKey(&v1.DispatchQueryPlanRequest{
				CanonicalKey: resourceRelation.Relation,
				Resource:     ONR(resourceRelation.Namespace, resourceIds[0], resourceRelation.Relation),
				PlanContext:  &v1.PlanContext{Revision: metadata.AtRevision},
			}, computeBothHashes), []string{
				resourceRelation.Relation,
				resourceRelation.Namespace,
				resourceIds[0],
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
	require.Len(t, cachePrefixes, len(generatorFuncs))

	for _, resourceIds := range allResourceIds {
		t.Run(strings.Join(resourceIds, ","), func(t *testing.T) {
			for _, subjectIds := range allSubjectIds {
				t.Run(strings.Join(subjectIds, ","), func(t *testing.T) {
					for _, resourceRelation := range resourceRelations {
						t.Run(tuple.StringCoreRR(resourceRelation), func(t *testing.T) {
							for _, subjectRelation := range subjectRelations {
								t.Run(tuple.StringCoreRR(subjectRelation), func(t *testing.T) {
									for _, revision := range revisions {
										t.Run(revision, func(t *testing.T) {
											metadata := &v1.ResolverMeta{
												AtRevision: revision,
											}

											for prefix, f := range generatorFuncs {
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
	result, err := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
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

	require.NoError(t, err)
	require.Equal(t, "e49efdc8e1d99daca601", hex.EncodeToString(result.StableSumAsBytes()))
}
