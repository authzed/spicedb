package graph

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/caveats"
	"github.com/authzed/spicedb/pkg/tuple"
)

var caveatForTesting = caveats.CaveatForTesting

func TestCheckDispatchSet(t *testing.T) {
	tcs := []struct {
		name              string
		relationships     []tuple.Relationship
		dispatchChunkSize uint16
		expectedChunks    []checkDispatchChunk
		expectedMappings  map[string][]resourceIDAndCaveat
	}{
		{
			"basic",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
			},
			100,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"basic chunking",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"3"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"different subject types",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:1#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:2#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:3#member"),
			},
			100,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "anothertype", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"different subject types mixed",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:1#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:3#member"),
			},
			100,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "anothertype", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"different subject types with chunking",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:1#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:2#member"),
				tuple.MustParse("document:somedoc#viewer@anothertype:3#member"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"3"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "anothertype", Relation: "member"},
					resourceIds:        []string{"1", "2"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "anothertype", Relation: "member"},
					resourceIds:        []string{"3"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:1#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"anothertype:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"some caveated members",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member[somecaveat]"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
			},
			100,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2", "3"},
					hasIncomingCaveats: true,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: caveatForTesting("somecaveat")},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
			},
		},
		{
			"caveated members combined when chunking",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member[somecaveat]"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:somedoc#viewer@group:3#member"),
				tuple.MustParse("document:somedoc#viewer@group:4#member[somecaveat]"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"2", "3"},
					hasIncomingCaveats: false,
				},
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "4"},
					hasIncomingCaveats: true,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: caveatForTesting("somecaveat")},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:3#member": {
					{resourceID: "somedoc", caveat: nil},
				},
				"group:4#member": {
					{resourceID: "somedoc", caveat: caveatForTesting("somecaveat")},
				},
			},
		},
		{
			"different resources leading to the same subject",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member"),
				tuple.MustParse("document:anotherdoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:anotherdoc#viewer@group:2#member"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2"},
					hasIncomingCaveats: false,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: nil},
					{resourceID: "anotherdoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
					{resourceID: "anotherdoc", caveat: nil},
				},
			},
		},
		{
			"different resources leading to the same subject with caveats",
			[]tuple.Relationship{
				tuple.MustParse("document:somedoc#viewer@group:1#member[somecaveat]"),
				tuple.MustParse("document:anotherdoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:2#member"),
				tuple.MustParse("document:anotherdoc#viewer@group:2#member[somecaveat]"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1", "2"},
					hasIncomingCaveats: true,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: caveatForTesting("somecaveat")},
					{resourceID: "anotherdoc", caveat: nil},
				},
				"group:2#member": {
					{resourceID: "somedoc", caveat: nil},
					{resourceID: "anotherdoc", caveat: caveatForTesting("somecaveat")},
				},
			},
		},
		{
			"different resource leading to the same subject with caveats",
			[]tuple.Relationship{
				tuple.MustParse("document:anotherdoc#viewer@group:1#member"),
				tuple.MustParse("document:thirddoc#viewer@group:1#member"),
				tuple.MustParse("document:somedoc#viewer@group:1#member[somecaveat]"),
			},
			2,
			[]checkDispatchChunk{
				{
					resourceType:       tuple.RelationReference{ObjectType: "group", Relation: "member"},
					resourceIds:        []string{"1"},
					hasIncomingCaveats: true,
				},
			},
			map[string][]resourceIDAndCaveat{
				"group:1#member": {
					{resourceID: "somedoc", caveat: caveatForTesting("somecaveat")},
					{resourceID: "anotherdoc", caveat: nil},
					{resourceID: "thirddoc", caveat: nil},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			set := newCheckDispatchSet()
			for _, rel := range tc.relationships {
				set.addForRelationship(rel)
			}

			chunks := set.dispatchChunks(tc.dispatchChunkSize)
			for _, c := range chunks {
				sort.Strings(c.resourceIds)
			}

			require.ElementsMatch(t, tc.expectedChunks, chunks, "difference in expected chunks. found: %v", chunks)

			for subjectString, expectedMappings := range tc.expectedMappings {
				parsed, err := tuple.ParseSubjectONR(subjectString)
				require.NoError(t, err)
				require.NotNil(t, parsed)

				mappings := set.mappingsForSubject(parsed.ObjectType, parsed.ObjectID, parsed.Relation)
				require.ElementsMatch(t, expectedMappings, mappings)
			}
		})
	}
}
