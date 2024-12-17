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
				resultsFileName: "indirect-lookup-resources-cursored-lookup-resources-for-fred-page-size-5-results.yaml",
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
				resultsFileName: "indirect-lookup-resources-cursored-lookup-resources-for-fred-page-size-16-results.yaml",
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
				resultsFileName: "indirect-lookup-resources-cursored-lookup-resources-for-fred-page-size-53-results.yaml",
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
				resultsFileName: "indirect-lookup-resources-cursored-lookup-resources-for-fred-page-size-54-results.yaml",
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
				resultsFileName: "indirect-lookup-resources-cursored-lookup-resources-for-fred-page-size-100-results.yaml",
			},
			{
				name:          "indirect without other permission, page size 5",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "vsb",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         5,
				},
				resultsFileName: "indirect-lookup-resources-indirect-without-other-permission-page-size-5-results.yaml",
			},
			{
				name:          "indirect without other permission, page size 16",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "vsb",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         16,
				},
				resultsFileName: "indirect-lookup-resources-indirect-without-other-permission-page-size-16-results.yaml",
			},
			{
				name:          "vsb_plus_nil, page size 16",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "vsb_plus_nil",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         16,
				},
				resultsFileName: "basic-lookup-resources-vsb-plus-nil-page-size-16-results.yaml",
			},
			{
				name:          "edit, page size 16",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "edit",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         16,
				},
				resultsFileName: "basic-lookup-resources-edit-page-size-16-results.yaml",
			},
		},
	},
	{
		name:     "lookup subjects intersection arrow",
		datafile: "document-with-intersect-arrow.yaml",
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
		name:     "lookup resources with intersection",
		datafile: "document-with-intersect-resources.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "uncursored lookup resources for user:fred",
				operationName: "lookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
				},
				resultsFileName: "lookup-resources-with-intersection-uncursored-indirect-lookup-resources-for-user-fred-results.yaml",
			},
			{
				name:          "uncursored indirect lookup resources for user:fred",
				operationName: "lookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
				},
				resultsFileName: "lookup-resources-with-intersection-uncursored-indirect-lookup-resources-for-user-fred-results.yaml",
			},
			{
				name:          "cursored lookup resources for user:fred",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         18,
				},
				resultsFileName: "lookup-resources-with-intersection-cursored-lookup-resources-for-user-fred-results.yaml",
			},
			{
				name:          "cursored indirect lookup resources for user:fred",
				operationName: "cursoredLookupResources",
				arguments: map[string]any{
					"resource_type":     "document",
					"permission":        "indirect_view",
					"subject_type":      "user",
					"subject_object_id": "fred",
					"page_size":         18,
				},
				resultsFileName: "lookup-resources-with-intersection-cursored-lookup-resources-for-user-fred-results.yaml",
			},
		},
	},
	{
		name:     "basic import export",
		datafile: "document-with-a-few-relationships.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "import export with no limit and no filter",
				operationName: "bulkImportExportRelationships",
				arguments: map[string]any{
					"rels_file": "basic-import-export-relationships.txt",
				},
				resultsFileName: "basic-import-export-results.yaml",
			},
		},
	},
	{
		name:     "basic import export with optional limit",
		datafile: "document-with-a-few-relationships.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "import export with limit and no filter",
				operationName: "bulkImportExportRelationships",
				arguments: map[string]any{
					"rels_file":      "basic-import-export-relationships.txt",
					"optional_limit": 3,
				},
				resultsFileName: "basic-import-export-results.yaml",
			},
		},
	},
	{
		name:     "basic import export with filter",
		datafile: "document-with-a-few-relationships.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "import export with filter filter",
				operationName: "bulkImportExportRelationships",
				arguments: map[string]any{
					"rels_file":     "basic-import-export-relationships.txt",
					"resource_type": "document",
				},
				resultsFileName: "basic-import-export-results.yaml",
			},
		},
	},
	{
		name:     "basic import export with object ID filter",
		datafile: "document-with-a-few-relationships.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "import export with filter filter",
				operationName: "bulkImportExportRelationships",
				arguments: map[string]any{
					"rels_file":                 "basic-import-export-relationships.txt",
					"filter_resource_id_prefix": "doc-1",
				},
				resultsFileName: "filtered-import-export-results.yaml",
			},
		},
	},
	{
		name:     "basic bulk checks",
		datafile: "document-with-a-few-relationships.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "basic bulk checks",
				operationName: "bulkCheckPermissions",
				arguments: map[string]any{
					"check_requests": []string{
						"document:doc-1#view@user:user-0",
						"document:doc-1#view@user:user-1",
						"document:doc-1#view@user:user-2",
						"document:doc-2#view@user:user-0",
						"document:doc-2#view@user:user-1",
						"document:doc-2#view@user:user-2",
						"document:doc-3#view@user:user-0",
						"document:doc-3#view@user:user-1",
						"document:doc-3#view@user:user-2",
					},
				},
			},
		},
	},
	{
		name:     "bulk checks with traits",
		datafile: "document-with-traits.yaml",
		operations: []steelThreadOperationCase{
			{
				name:          "bulk checks",
				operationName: "bulkCheckPermissions",
				arguments: map[string]any{
					"check_requests": []string{
						"document:firstdoc#view@user:tom",
						"document:firstdoc#view@user:fred",
						"document:seconddoc#view@user:tom",
						"document:seconddoc#view@user:fred",
						`document:seconddoc#view@user:tom[unused:{"somecondition": 41}]`,
						`document:seconddoc#view@user:fred[unused:{"somecondition": 41}]`,
						`document:seconddoc#view@user:tom[unused:{"somecondition": 42}]`,
						`document:seconddoc#view@user:fred[unused:{"somecondition": 42}]`,
						"document:thirddoc#view@user:tom",
						"document:thirddoc#view@user:fred",
						`document:thirddoc#view@user:tom[unused:{"somecondition": 41}]`,
						`document:thirddoc#view@user:fred[unused:{"somecondition": 41}]`,
						`document:thirddoc#view@user:tom[unused:{"somecondition": 42}]`,
						`document:thirddoc#view@user:fred[unused:{"somecondition": 42}]`,
					},
				},
			},
		},
	},
}
