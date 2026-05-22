package keys

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/authzed/spicedb/pkg/datalayer"
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalLimit: 0,
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalLimit: 42,
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: nil,
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{})
						return v
					}(),
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": 1,
							"bar": true,
						})
						return v
					}(),
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": 2,
							"bar": true,
						})
						return v
					}(),
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					Context: func() *structpb.Struct {
						v, _ := structpb.NewStruct(map[string]any{
							"foo": "this is an `escaped` string\nhi",
						})
						return v
					}(),
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
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
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo"},
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				})
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
						SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
					},
					OptionalCursor: &v1.Cursor{
						Sections: []string{"foo", "bar"},
					},
				})
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
			}), []string{
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
		})
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
			}), []string{
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
				Resource: ONR(resourceRelation.Namespace, resourceIds[0], resourceRelation.Relation),
				Subject:  ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				Plan:     []byte(resourceRelation.Relation),
				PlanContext: &v1.PlanContext{
					Revision: metadata.AtRevision,
				},
			}), []string{
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
				Subject: ONR(subjectRelation.Namespace, subjectIds[0], subjectRelation.Relation),
				Plan:    []byte(resourceRelation.Relation),
				PlanContext: &v1.PlanContext{
					Revision: metadata.AtRevision,
				},
			}), []string{
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
				Resource: ONR(resourceRelation.Namespace, resourceIds[0], resourceRelation.Relation),
				Plan:     []byte(resourceRelation.Relation),
				PlanContext: &v1.PlanContext{
					Revision: metadata.AtRevision,
				},
			}), []string{
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
			}), append([]string{
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
												SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
											}

											for prefix, f := range generatorFuncs {
												t.Run(prefix, func(t *testing.T) {
													generated, usedData := f(resourceIds, subjectIds, resourceRelation, subjectRelation, metadata)
													usedDataString := fmt.Sprintf("%s:%s", prefix, strings.Join(usedData, ","))
													if dataCombinationSeen.Add(usedDataString) {
														require.True(t, stableCacheKeysSeen.Add(hex.EncodeToString((generated.StableSumAsBytes()))))
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

func TestComputeContextHash(t *testing.T) {
	result, err := lookupResourcesRequest2ToKey(&v1.DispatchLookupResources2Request{
		ResourceRelation: RR("document", "view"),
		SubjectRelation:  RR("user", "..."),
		SubjectIds:       []string{"mariah"},
		TerminalSubject:  ONR("user", "mariah", "..."),
		Metadata: &v1.ResolverMeta{
			AtRevision: "1234",
			SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
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
	})

	require.NoError(t, err)
	require.Equal(t, "e49efdc8e1d99daca601", hex.EncodeToString(result.StableSumAsBytes()))
}

func TestCheckRequestKeyIncludesCheckHints(t *testing.T) {
	baseReq := func() *v1.DispatchCheckRequest {
		return &v1.DispatchCheckRequest{
			ResourceRelation: RR("document", "reader"),
			ResourceIds:      []string{"firstdoc"},
			Subject:          ONR("user", "caveatedreader", "..."),
			ResultsSetting:   v1.DispatchCheckRequest_REQUIRE_ALL_RESULTS,
			Metadata: &v1.ResolverMeta{
				AtRevision: "1234",
				SchemaHash: []byte(datalayer.NoSchemaHashForTesting),
			},
		}
	}

	memberHint := func() []*v1.CheckHint {
		return []*v1.CheckHint{{
			Resource: ONR("document", "firstdoc", "reader"),
			Subject:  ONR("user", "caveatedreader", "..."),
			Result:   &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_MEMBER},
		}}
	}
	caveatedHint := func() []*v1.CheckHint {
		return []*v1.CheckHint{{
			Resource: ONR("document", "firstdoc", "reader"),
			Subject:  ONR("user", "caveatedreader", "..."),
			Result:   &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_CAVEATED_MEMBER},
		}}
	}
	twoHints := func() []*v1.CheckHint {
		return []*v1.CheckHint{
			{
				Resource: ONR("document", "firstdoc", "reader"),
				Subject:  ONR("user", "caveatedreader", "..."),
				Result:   &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_MEMBER},
			},
			{
				Resource: ONR("document", "firstdoc", "writer"),
				Subject:  ONR("user", "caveatedreader", "..."),
				Result:   &v1.ResourceCheckResult{Membership: v1.ResourceCheckResult_CAVEATED_MEMBER},
			},
		}
	}
	twoHintsReversed := func() []*v1.CheckHint {
		return []*v1.CheckHint{twoHints()[1], twoHints()[0]}
	}

	noHints := baseReq()

	withMember := baseReq()
	withMember.CheckHints = memberHint()

	withCaveated := baseReq()
	withCaveated.CheckHints = caveatedHint()

	withTwoHints := baseReq()
	withTwoHints.CheckHints = twoHints()

	withTwoHintsReversed := baseReq()
	withTwoHintsReversed.CheckHints = twoHintsReversed()

	t.Run("relation key", func(t *testing.T) {
		require.NotEqual(t, checkRequestToKey(noHints), checkRequestToKey(withMember), "a hint-influenced check must not share a cache key with a hint-free check")
		require.NotEqual(t, checkRequestToKey(withMember), checkRequestToKey(withCaveated), "check hints with different results must produce different cache keys")
		require.Equal(t, checkRequestToKey(withTwoHints), checkRequestToKey(withTwoHintsReversed), "the order of check hints must not influence the cache key")
	})

	t.Run("canonical key", func(t *testing.T) {
		keyNo, err := checkRequestToKeyWithCanonical(noHints, "reader")
		require.NoError(t, err)
		keyMember, err := checkRequestToKeyWithCanonical(withMember, "reader")
		require.NoError(t, err)
		keyCaveated, err := checkRequestToKeyWithCanonical(withCaveated, "reader")
		require.NoError(t, err)
		keyTwoHints, err := checkRequestToKeyWithCanonical(withTwoHints, "writer")
		require.NoError(t, err)
		keyTwoHintsReversed, err := checkRequestToKeyWithCanonical(withTwoHintsReversed, "writer")
		require.NoError(t, err)

		require.NotEqual(t, keyNo, keyMember, "a hint-influenced check must not share a canonical cache key with a hint-free check")
		require.NotEqual(t, keyMember, keyCaveated, "check hints with different results must produce different canonical cache keys")
		require.Equal(t, keyTwoHints, keyTwoHintsReversed, "the order of check hints must not influence the canonical cache key")
	})
}
