package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/authzed/spicedb/pkg/REDACTEDapi/api"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSharing(t *testing.T) {
	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Check for non-existent share.
	resp, err := srv.LookupShared(context.Background(), &api.LookupShareRequest{
		ShareReference: "someref",
	})
	require.NoError(err)
	require.Equal(api.LookupShareResponse_UNKNOWN_REFERENCE, resp.Status)

	// Add a share resource.
	sresp, err := srv.Share(context.Background(), &api.ShareRequest{
		NamespaceConfigs: []string{"foo", "bar"},
		RelationTuples:   "rt",
		ValidationYaml:   "vy",
		AssertionsYaml:   "ay",
	})
	require.NoError(err)

	// Lookup again.
	lresp, err := srv.LookupShared(context.Background(), &api.LookupShareRequest{
		ShareReference: sresp.ShareReference,
	})
	require.NoError(err)
	require.Equal(api.LookupShareResponse_VALID_REFERENCE, lresp.Status)
	require.Equal([]string{"foo", "bar"}, lresp.NamespaceConfigs)
}

func TestEditCheck(t *testing.T) {
	type testCase struct {
		name            string
		namespaces      []*api.NamespaceContext
		tuples          []*api.RelationTuple
		checkTuples     []*api.RelationTuple
		expectedError   *api.ValidationError
		expectedResults []*api.EditCheckResult
	}

	tests := []testCase{
		{
			"invalid namespace",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: foo\"",
				},
			},
			[]*api.RelationTuple{},
			[]*api.RelationTuple{},
			&api.ValidationError{
				Message: "invalid value for string type: foo",
				Kind:    api.ValidationError_NAMESPACE_CONFIG_ISSUE,
				Source:  api.ValidationError_NAMESPACE_CONFIG,
				Line:    1,
				Column:  7,
			},
			[]*api.EditCheckResult{},
		},
		{
			"valid namespace",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*api.RelationTuple{},
			[]*api.RelationTuple{},
			nil,
			[]*api.EditCheckResult{},
		},
		{
			"valid checks",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: `name: "user"`,
				},
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: `name: "somenamespace"

					relation {
						name: "somerel"
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
			},
			[]*api.RelationTuple{
				tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
				tuple.Scan("somenamespace:someobj#somerel@user:anotheruser#..."),
			},
			nil,
			[]*api.EditCheckResult{
				&api.EditCheckResult{
					Tuple:    tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
					IsMember: true,
				},
				&api.EditCheckResult{
					Tuple:    tuple.Scan("somenamespace:someobj#somerel@user:anotheruser#..."),
					IsMember: false,
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			store := NewInMemoryShareStore("flavored")
			srv := NewDeveloperServer(store)

			resp, err := srv.EditCheck(context.Background(), &api.EditCheckRequest{
				Context: &api.RequestContext{
					Namespaces: tc.namespaces,
					Tuples:     tc.tuples,
				},
				CheckTuples: tc.checkTuples,
			})
			require.NoError(err)

			if tc.expectedError != nil {
				require.Equal(tc.expectedError, resp.ContextNamespaces[0].Errors[0])
			} else {
				for _, ni := range resp.ContextNamespaces {
					require.Equal(0, len(ni.Errors), "Found error(s): %v", ni.Errors)
				}
			}
		})
	}
}

func TestValidate(t *testing.T) {
	type testCase struct {
		name                   string
		namespaces             []*api.NamespaceContext
		tuples                 []*api.RelationTuple
		validationYaml         string
		assertionsYaml         string
		expectedError          *api.ValidationError
		expectedValidationYaml string
	}

	tests := []testCase{
		{
			"invalid namespace",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: foo\"",
				},
			},
			[]*api.RelationTuple{},
			"",
			"",
			&api.ValidationError{
				Message: "invalid value for string type: foo",
				Kind:    api.ValidationError_NAMESPACE_CONFIG_ISSUE,
				Source:  api.ValidationError_NAMESPACE_CONFIG,
				Line:    1,
				Column:  7,
			},
			"",
		},
		{
			"valid namespace",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*api.RelationTuple{},
			"",
			"",
			nil,
			"{}\n",
		},
		{
			"invalid validation yaml",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*api.RelationTuple{},
			`asdkjhgasd`,
			"",
			&api.ValidationError{
				Message: "cannot unmarshal !!str `asdkjhgasd` into validationfile.ValidationMap",
				Kind:    api.ValidationError_PARSE_ERROR,
				Source:  api.ValidationError_VALIDATION_YAML,
				Line:    1,
			},
			"",
		},
		{
			"invalid assertions yaml",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*api.RelationTuple{},
			"",
			`asdhasjdkhjasd`,
			&api.ValidationError{
				Message: "cannot unmarshal !!str `asdhasj...` into validationfile.Assertions",
				Kind:    api.ValidationError_PARSE_ERROR,
				Source:  api.ValidationError_ASSERTION,
				Line:    1,
			},
			"",
		},
		{
			"assertion true failure",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"

					relation {
						name: "viewer"
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			"",
			`assertTrue:
- document:somedoc#viewer@user:jake#...`,
			&api.ValidationError{
				Message:  "Expected relation or permission document:somedoc#viewer@user:jake#... to exist",
				Kind:     api.ValidationError_ASSERTION_FAILED,
				Source:   api.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jake#...",
			},
			"{}\n",
		},
		{
			"assertion false failure",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"

					relation {
						name: "viewer"
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "Expected relation or permission document:somedoc#viewer@user:jimmy#... to not exist",
				Kind:     api.ValidationError_ASSERTION_FAILED,
				Source:   api.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jimmy#...",
			},
			"{}\n",
		},
		{
			"assertion invalid relation",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"`,
				},
			},
			[]*api.RelationTuple{},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "Unknown relation in check document:somedoc#viewer@user:jimmy#...",
				Kind:     api.ValidationError_UNKNOWN_RELATION,
				Source:   api.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jimmy#...",
			},
			"{}\n",
		},
		{
			"missing subject",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, subject `user:jimmy#...` found but missing from specified",
				Kind:     api.ValidationError_EXTRA_TUPLE_FOUND,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: "document:somedoc#viewer",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"extra subject",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>"
- "[user:jake#...] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, missing expected subject `user:jake#...`",
				Kind:     api.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: "user:jake#...",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"parse error in validation",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, invalid subject: user",
				Kind:     api.ValidationError_PARSE_ERROR,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: "[user]",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"parse error in validation relationships",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:som>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, invalid object and relation: document:som",
				Kind:     api.ValidationError_PARSE_ERROR,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: "<document:som>",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"different relations",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, found different relationships for subject `user:jimmy#...`: Specified: `<document:somedoc#viewer>`, Computed: `<document:somedoc#writer>`",
				Kind:     api.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: `[user:jimmy#...] is <document:somedoc#viewer>`,
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"full valid",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
				tuple.Scan("document:somedoc#viewer@user:jake#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>"
- "[user:jake#...] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy#...
- document:somedoc#viewer@user:jimmy#...
- document:somedoc#viewer@user:jake#...
assertFalse:
- document:somedoc#writer@user:jake#...
`,
			nil,
			`document:somedoc#viewer:
- '[user:jake#...] is <document:somedoc#viewer>'
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"muiltipath",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>/<document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy#...
`,
			nil,
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"muiltipath missing relationship",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: `name: "document"
					
					relation {
						name: "writer"
					}

					relation {
						name: "viewer"
						userset_rewrite {
							union {
								child { _this {} }
								child {
									computed_userset { relation: "writer" }
								}
							}
						}
					}`,
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy#...
`,
			&api.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, found different relationships for subject `user:jimmy#...`: Specified: `<document:somedoc#writer>`, Computed: `<document:somedoc#viewer>/<document:somedoc#writer>`",
				Kind:     api.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   api.ValidationError_VALIDATION_YAML,
				Metadata: `[user:jimmy#...] is <document:somedoc#writer>`,
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"invalid namespace on tuple",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			``,
			``,
			&api.ValidationError{
				Message:  "Unknown namespace in check document:somedoc#writer@user:jimmy#...",
				Kind:     api.ValidationError_UNKNOWN_NAMESPACE,
				Source:   api.ValidationError_VALIDATION_TUPLE,
				Metadata: `document:somedoc#writer@user:jimmy#...`,
			},
			``,
		},
		{
			"invalid relation on tuple",
			[]*api.NamespaceContext{
				&api.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&api.NamespaceContext{
					Handle: "document",
					Config: "name: \"document\"",
				},
			},
			[]*api.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			``,
			``,
			&api.ValidationError{
				Message:  "Unknown relation in check document:somedoc#writer@user:jimmy#...",
				Kind:     api.ValidationError_UNKNOWN_RELATION,
				Source:   api.ValidationError_VALIDATION_TUPLE,
				Metadata: `document:somedoc#writer@user:jimmy#...`,
			},
			``,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)

			store := NewInMemoryShareStore("flavored")
			srv := NewDeveloperServer(store)

			resp, err := srv.Validate(context.Background(), &api.ValidateRequest{
				Context: &api.RequestContext{
					Namespaces: tc.namespaces,
					Tuples:     tc.tuples,
				},
				ValidationYaml:       tc.validationYaml,
				AssertionsYaml:       tc.assertionsYaml,
				UpdateValidationYaml: true,
			})
			require.NoError(err)

			if tc.expectedError != nil {
				if len(resp.ValidationErrors) > 0 {
					require.Equal(tc.expectedError, resp.ValidationErrors[0])
				} else {
					require.Equal(tc.expectedError, resp.ContextNamespaces[0].Errors[0])
				}
			} else {
				for _, ni := range resp.ContextNamespaces {
					require.Equal(0, len(ni.Errors), "Found error(s): %v", ni.Errors)
				}

				require.Equal(0, len(resp.ValidationErrors), "Found error(s): %v", resp.ValidationErrors)
			}

			require.Equal(tc.expectedValidationYaml, resp.UpdatedValidationYaml)
		})
	}
}
