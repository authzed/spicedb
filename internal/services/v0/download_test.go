package v0

import (
	"context"
	"net/http"
	"testing"

	v0 "github.com/authzed/authzed-go/proto/authzed/api/v0"
	"github.com/stretchr/testify/require"
)

func TestDeveloperDownload(t *testing.T) {
	require := require.New(t)

	store := NewInMemoryShareStore("flavored")
	devsrv := NewDeveloperServer(store)
	handler := downloadHandler(store)

	sresp, err := devsrv.Share(context.Background(), &v0.ShareRequest{
		Schema:            "s",
		RelationshipsYaml: "ry",
		ValidationYaml:    "vy",
		AssertionsYaml:    "ay",
	})
	require.NoError(err)

	tests := []struct {
		name         string
		method       string
		ref          string
		expectedCode int
		expectedBody string
	}{
		{
			name:         "valid",
			method:       http.MethodGet,
			ref:          sresp.ShareReference,
			expectedCode: http.StatusOK,
			expectedBody: `schema: s
relationships: ry
validation: vy
assertions: ay
`,
		},
		{
			name:         "invalid ref",
			method:       http.MethodGet,
			ref:          "nope",
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "ref with /",
			method:       http.MethodGet,
			ref:          "ref/hasslash",
			expectedCode: http.StatusBadRequest,
			expectedBody: "ref may not contain a '/'",
		},
		{
			name:         "invalid params",
			method:       http.MethodGet,
			ref:          "",
			expectedCode: http.StatusBadRequest,
			expectedBody: "ref is missing",
		},
		{
			name:         "invalid method",
			method:       http.MethodPost,
			ref:          "ref",
			expectedCode: http.StatusMethodNotAllowed,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			url := downloadPath + test.ref
			require.HTTPStatusCode(handler, test.method, url, nil, test.expectedCode)
			if len(test.expectedBody) > 0 {
				require.HTTPBodyContains(handler, test.method, url, nil, test.expectedBody)
			}
		})
	}
}
