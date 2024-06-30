//go:build steelthread
// +build steelthread

package steelthreadtesting

import v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

type steelThreadTestCase struct {
	name       string
	datafile   string
	operations []steelThreadOperationCase
}

type steelThreadOperationCase struct {
	name            string
	operationName   string
	arguments       map[string]any
	resultsFileName string
}

type stOperation func(parameters map[string]any, client v1.PermissionsServiceClient) (any, error)

var steelThreadTestCases = []steelThreadTestCase{
	{
		name:     "basic lookup subjects",
		datafile: "basic-document.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "uncursored lookup subjects for somedoc",
				operationName: "lookupSubjects",
				arguments: map[string]any{
					"resource_type":      "document",
					"resource_object_id": "somedoc",
					"permission":         "view",
					"subject_type":       "user",
				},
			},
			{
				name:          "uncursored lookup subjects for public doc",
				operationName: "lookupSubjects",
				arguments: map[string]any{
					"resource_type":      "document",
					"resource_object_id": "publicdoc",
					"permission":         "view",
					"subject_type":       "user",
				},
			},
		},
	},
	{
		name:     "lookup subjects intersection",
		datafile: "document-with-intersect.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "uncursored lookup subjects for somedoc",
				operationName: "lookupSubjects",
				arguments: map[string]any{
					"resource_type":      "document",
					"resource_object_id": "somedoc",
					"permission":         "view",
					"subject_type":       "user",
				},
			},
		},
	},
	{
		name:     "basic lookup resources",
		datafile: "document-with-many-resources.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "uncursored lookup resources for fred",
				operationName: "lookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
				},
				resultsFileName: "basic-lookup-resources-uncursored-lookup-resources-for-fred-results.yaml",
			},
			{
				name:          "cursored lookup resources for fred, page size 5",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         5,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-5-results.yaml",
			},
			{
				name:          "cursored lookup resources for fred, page size 16",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         16,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-16-results.yaml",
			},
			{
				name:          "cursored lookup resources for fred, page size 53",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         53,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-53-results.yaml",
			},
			{
				name:          "cursored lookup resources for fred, page size 54",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         54,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-54-results.yaml",
			},
			{
				name:          "cursored lookup resources for fred, page size 100",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         100,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-100-results.yaml",
			},
			{
				name:          "uncursored indirect lookup resources for fred",
				operationName: "lookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
				},
				resultsFileName: "basic-lookup-resources-uncursored-lookup-resources-for-fred-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for fred, page size 5",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         5,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-5-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for fred, page size 16",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         16,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-16-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for fred, page size 53",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         53,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-53-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for fred, page size 54",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         54,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-54-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for fred, page size 100",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         100,
				},
				resultsFileName: "basic-lookup-resources-cursored-lookup-resources-for-fred-page-size-100-results.yaml",
			},
		},
	},
}
