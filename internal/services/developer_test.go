package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v0 "github.com/authzed/spicedb/pkg/proto/authzed/api/v0"
	"github.com/authzed/spicedb/pkg/tuple"
)

func TestSharing(t *testing.T) {
	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	srv := NewDeveloperServer(store)

	// Check for non-existent share.
	resp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: "someref",
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_UNKNOWN_REFERENCE, resp.Status)

	// Add a share resource.
	sresp, err := srv.Share(context.Background(), &v0.ShareRequest{
		NamespaceConfigs: []string{"foo", "bar"},
		RelationTuples:   "rt",
		ValidationYaml:   "vy",
		AssertionsYaml:   "ay",
	})
	require.NoError(err)

	// Lookup again.
	lresp, err := srv.LookupShared(context.Background(), &v0.LookupShareRequest{
		ShareReference: sresp.ShareReference,
	})
	require.NoError(err)
	require.Equal(v0.LookupShareResponse_VALID_REFERENCE, lresp.Status)
	require.Equal([]string{"foo", "bar"}, lresp.NamespaceConfigs)
}

func TestEditCheck(t *testing.T) {
	type testCase struct {
		name            string
		namespaces      []*v0.NamespaceContext
		tuples          []*v0.RelationTuple
		checkTuples     []*v0.RelationTuple
		expectedError   *v0.ValidationError
		expectedResults []*v0.EditCheckResult
	}

	tests := []testCase{
		{
			"invalid namespace",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: foo\"",
				},
			},
			[]*v0.RelationTuple{},
			[]*v0.RelationTuple{},
			&v0.ValidationError{
				Message: "invalid value for string type: foo",
				Kind:    v0.ValidationError_NAMESPACE_CONFIG_ISSUE,
				Source:  v0.ValidationError_NAMESPACE_CONFIG,
				Line:    1,
				Column:  7,
			},
			[]*v0.EditCheckResult{},
		},
		{
			"valid namespace",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*v0.RelationTuple{},
			[]*v0.RelationTuple{},
			nil,
			[]*v0.EditCheckResult{},
		},
		{
			"valid checks",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: `name: "user"`,
				},
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: `name: "somenamespace"

					relation {
						name: "somerel"
					}`,
				},
			},
			[]*v0.RelationTuple{
				tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
			},
			[]*v0.RelationTuple{
				tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
				tuple.Scan("somenamespace:someobj#somerel@user:anotheruser#..."),
			},
			nil,
			[]*v0.EditCheckResult{
				&v0.EditCheckResult{
					Tuple:    tuple.Scan("somenamespace:someobj#somerel@user:foo#..."),
					IsMember: true,
				},
				&v0.EditCheckResult{
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

			resp, err := srv.EditCheck(context.Background(), &v0.EditCheckRequest{
				Context: &v0.RequestContext{
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
		namespaces             []*v0.NamespaceContext
		tuples                 []*v0.RelationTuple
		validationYaml         string
		assertionsYaml         string
		expectedError          *v0.ValidationError
		expectedValidationYaml string
	}

	tests := []testCase{
		{
			"invalid namespace",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: foo\"",
				},
			},
			[]*v0.RelationTuple{},
			"",
			"",
			&v0.ValidationError{
				Message: "invalid value for string type: foo",
				Kind:    v0.ValidationError_NAMESPACE_CONFIG_ISSUE,
				Source:  v0.ValidationError_NAMESPACE_CONFIG,
				Line:    1,
				Column:  7,
			},
			"",
		},
		{
			"valid namespace",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*v0.RelationTuple{},
			"",
			"",
			nil,
			"{}\n",
		},
		{
			"invalid validation yaml",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*v0.RelationTuple{},
			`asdkjhgasd`,
			"",
			&v0.ValidationError{
				Message: "cannot unmarshal !!str `asdkjhgasd` into validationfile.ValidationMap",
				Kind:    v0.ValidationError_PARSE_ERROR,
				Source:  v0.ValidationError_VALIDATION_YAML,
				Line:    1,
			},
			"",
		},
		{
			"invalid assertions yaml",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "somenamespace",
					Config: "name: \"somenamespace\"",
				},
			},
			[]*v0.RelationTuple{},
			"",
			`asdhasjdkhjasd`,
			&v0.ValidationError{
				Message: "cannot unmarshal !!str `asdhasj...` into validationfile.Assertions",
				Kind:    v0.ValidationError_PARSE_ERROR,
				Source:  v0.ValidationError_ASSERTION,
				Line:    1,
			},
			"",
		},
		{
			"assertion true failure",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
					Handle: "document",
					Config: `name: "document"

					relation {
						name: "viewer"
					}`,
				},
			},
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			"",
			`assertTrue:
- document:somedoc#viewer@user:jake#...`,
			&v0.ValidationError{
				Message:  "Expected relation or permission document:somedoc#viewer@user:jake#... to exist",
				Kind:     v0.ValidationError_ASSERTION_FAILED,
				Source:   v0.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jake#...",
			},
			"{}\n",
		},
		{
			"assertion false failure",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
					Handle: "document",
					Config: `name: "document"

					relation {
						name: "viewer"
					}`,
				},
			},
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "Expected relation or permission document:somedoc#viewer@user:jimmy#... to not exist",
				Kind:     v0.ValidationError_ASSERTION_FAILED,
				Source:   v0.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jimmy#...",
			},
			"{}\n",
		},
		{
			"assertion invalid relation",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
					Handle: "document",
					Config: `name: "document"`,
				},
			},
			[]*v0.RelationTuple{},
			"",
			`assertFalse:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "relation/permission `viewer` not found under namespace `document`",
				Kind:     v0.ValidationError_UNKNOWN_RELATION,
				Source:   v0.ValidationError_ASSERTION,
				Metadata: "document:somedoc#viewer@user:jimmy#...",
			},
			"{}\n",
		},
		{
			"missing subject",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, subject `user:jimmy#...` found but missing from specified",
				Kind:     v0.ValidationError_EXTRA_TUPLE_FOUND,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: "document:somedoc#viewer",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"extra subject",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>"
- "[user:jake#...] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, missing expected subject `user:jake#...`",
				Kind:     v0.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: "user:jake#...",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"parse error in validation",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, invalid subject: user",
				Kind:     v0.ValidationError_PARSE_ERROR,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: "[user]",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"parse error in validation relationships",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:som>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, invalid object and relation: document:som",
				Kind:     v0.ValidationError_PARSE_ERROR,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: "<document:som>",
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"different relations",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#viewer>"`,
			`assertTrue:
- document:somedoc#viewer@user:jimmy#...`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, found different relationships for subject `user:jimmy#...`: Specified: `<document:somedoc#viewer>`, Computed: `<document:somedoc#writer>`",
				Kind:     v0.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: `[user:jimmy#...] is <document:somedoc#viewer>`,
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#writer>'
`,
		},
		{
			"full valid",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
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
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
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
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
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
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
				tuple.Scan("document:somedoc#viewer@user:jimmy#..."),
			},
			`"document:somedoc#viewer":
- "[user:jimmy#...] is <document:somedoc#writer>"`,
			`assertTrue:
- document:somedoc#writer@user:jimmy#...
`,
			&v0.ValidationError{
				Message:  "For object and permission/relation `document:somedoc#viewer`, found different relationships for subject `user:jimmy#...`: Specified: `<document:somedoc#writer>`, Computed: `<document:somedoc#viewer>/<document:somedoc#writer>`",
				Kind:     v0.ValidationError_MISSING_EXPECTED_TUPLE,
				Source:   v0.ValidationError_VALIDATION_YAML,
				Metadata: `[user:jimmy#...] is <document:somedoc#writer>`,
			},
			`document:somedoc#viewer:
- '[user:jimmy#...] is <document:somedoc#viewer>/<document:somedoc#writer>'
`,
		},
		{
			"invalid namespace on tuple",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
			},
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			``,
			``,
			&v0.ValidationError{
				Message:  "namespace `document` not found",
				Kind:     v0.ValidationError_UNKNOWN_NAMESPACE,
				Source:   v0.ValidationError_VALIDATION_TUPLE,
				Metadata: `document:somedoc#writer@user:jimmy#...`,
			},
			``,
		},
		{
			"invalid relation on tuple",
			[]*v0.NamespaceContext{
				&v0.NamespaceContext{
					Handle: "user",
					Config: "name: \"user\"",
				},
				&v0.NamespaceContext{
					Handle: "document",
					Config: "name: \"document\"",
				},
			},
			[]*v0.RelationTuple{
				tuple.Scan("document:somedoc#writer@user:jimmy#..."),
			},
			``,
			``,
			&v0.ValidationError{
				Message:  "relation/permission `writer` not found under namespace `document`",
				Kind:     v0.ValidationError_UNKNOWN_RELATION,
				Source:   v0.ValidationError_VALIDATION_TUPLE,
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

			resp, err := srv.Validate(context.Background(), &v0.ValidateRequest{
				Context: &v0.RequestContext{
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
