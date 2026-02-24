package cursor

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datalayer"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
	impl "github.com/authzed/spicedb/pkg/proto/impl/v1"
	"github.com/authzed/spicedb/pkg/zedtoken"
)

var (
	revision1 = revisions.NewForTransactionID(1)
	revision2 = revisions.NewForTransactionID(2)
)

func TestEncodeDecode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		revision datastore.Revision
		sections []string
		hash     string
	}{
		{
			"empty",
			revision1,
			nil,
			"somehash",
		},
		{
			"basic",
			revision1,
			[]string{"a", "b", "c"},
			"another",
		},
		{
			"basic with different revision",
			revision2,
			[]string{"a", "b", "c"},
			"another",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			encoded, err := EncodeFromDispatchCursor(&dispatch.Cursor{
				Sections: tc.sections,
			}, tc.hash, tc.revision, datalayer.NoSchemaHashForLegacyCursor, map[string]string{"some": "flag"})
			require.NoError(err)
			require.NotNil(encoded)

			decoded, flags, err := DecodeToDispatchCursor(encoded, tc.hash)
			require.NoError(err)
			require.NotNil(decoded)
			require.Equal(map[string]string{"some": "flag"}, flags)

			require.Equal(tc.sections, decoded.Sections)

			decodedRev, _, _, err := DecodeToDispatchRevisionAndSchemaHash(t.Context(), encoded, revisions.CommonDecoder{
				Kind: revisions.TransactionID,
			})
			require.NoError(err)
			require.NotNil(decodedRev)
			require.Equal(tc.revision, decodedRev)
		})
	}
}

func TestDecode(t *testing.T) {
	for _, testCase := range []struct {
		name             string
		token            string
		expectedRevision datastore.Revision
		expectedSections []string
		expectedHash     string
		expectError      bool
	}{
		{ //nolint:gosec  // this is a test file and test logic
			name:             "invalid",
			token:            "abc",
			expectedRevision: datastore.NoRevision,
			expectedSections: []string{},
			expectedHash:     "",
			expectError:      true,
		},
		{ //nolint:gosec  // this is a test file and test logic
			name:             "empty",
			token:            "Cg0KATEaCHNvbWVoYXNo",
			expectedRevision: revision1,
			expectedSections: nil,
			expectedHash:     "somehash",
			expectError:      false,
		},
		{ //nolint:gosec  // this is a test file and test logic
			name:             "basic",
			token:            "ChUKATESAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision1,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "another",
			expectError:      false,
		},
		{ //nolint:gosec  // this is a test file and test logic
			name:             "basic with wrong hash",
			token:            "ChUKATESAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision1,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "wrong",
			expectError:      true,
		},
		{ //nolint:gosec  // this is a test file and test logic
			name:             "basic with different revision",
			token:            "ChUKATISAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision2,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "another",
			expectError:      false,
		},
	} {
		testName := fmt.Sprintf("%s(%s)=>%s", testCase.name, testCase.token, testCase.expectedRevision)
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)

			decoded, _, err := DecodeToDispatchCursor(&v1.Cursor{
				Token: testCase.token,
			}, testCase.expectedHash)

			if testCase.expectError {
				require.Error(err)
				return
			}

			require.NoError(err)
			require.NotNil(decoded)
			require.Equal(testCase.expectedSections, decoded.Sections)

			decodedRev, _, _, err := DecodeToDispatchRevisionAndSchemaHash(t.Context(), &v1.Cursor{
				Token: testCase.token,
			}, revisions.CommonDecoder{
				Kind: revisions.TransactionID,
			})

			require.NoError(err)
			require.True(
				testCase.expectedRevision.Equal(decodedRev),
				"%s != %s",
				testCase.expectedRevision,
				decodedRev,
			)
		})
	}
}

func TestDecodeToDispatchRevisionAndSchemaHashWithDatastoreID(t *testing.T) {
	require := require.New(t)

	// Encode a cursor that includes both a DatastoreUniqueId and a SchemaHash.
	encoded, err := Encode(&impl.DecodedCursor{
		VersionOneof: &impl.DecodedCursor_V1{
			V1: &impl.V1Cursor{
				Revision:              revision1.String(),
				DispatchVersion:       1,
				Sections:              []string{"a", "b"},
				CallAndParametersHash: "testhash",
				DatastoreUniqueId:     "testdsid",
				SchemaHash:            []byte("myschema123"),
			},
		},
	})
	require.NoError(err)

	decodedRev, schemaHash, status, err := DecodeToDispatchRevisionAndSchemaHash(
		t.Context(),
		encoded,
		revisions.CommonDecoder{
			Kind:              revisions.TransactionID,
			DatastoreUniqueID: "testdsid",
		},
	)
	require.NoError(err)
	require.Equal(zedtoken.StatusValid, status)
	require.True(revision1.Equal(decodedRev))
	require.Equal(datalayer.SchemaHash("myschema123"), schemaHash)
}

func TestDecodeToDispatchRevisionAndSchemaHashMismatchedDatastoreID(t *testing.T) {
	require := require.New(t)

	encoded, err := Encode(&impl.DecodedCursor{
		VersionOneof: &impl.DecodedCursor_V1{
			V1: &impl.V1Cursor{
				Revision:          revision1.String(),
				DispatchVersion:   1,
				DatastoreUniqueId: "otherid",
				SchemaHash:        []byte("myschema123"),
			},
		},
	})
	require.NoError(err)

	_, schemaHash, status, err := DecodeToDispatchRevisionAndSchemaHash(
		t.Context(),
		encoded,
		revisions.CommonDecoder{
			Kind:              revisions.TransactionID,
			DatastoreUniqueID: "testdsid",
		},
	)
	require.NoError(err)
	require.Equal(zedtoken.StatusMismatchedDatastoreID, status)
	require.Equal(datalayer.NoSchemaHashForLegacyCursor, schemaHash)
}
