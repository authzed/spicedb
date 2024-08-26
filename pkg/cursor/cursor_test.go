package cursor

import (
	"fmt"
	"testing"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
	"github.com/stretchr/testify/require"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
	dispatch "github.com/authzed/spicedb/pkg/proto/dispatch/v1"
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			require := require.New(t)
			encoded, err := EncodeFromDispatchCursor(&dispatch.Cursor{
				Sections: tc.sections,
			}, tc.hash, tc.revision, map[string]string{"some": "flag"})
			require.NoError(err)
			require.NotNil(encoded)

			decoded, flags, err := DecodeToDispatchCursor(encoded, tc.hash)
			require.NoError(err)
			require.NotNil(decoded)
			require.Equal(map[string]string{"some": "flag"}, flags)

			require.Equal(tc.sections, decoded.Sections)

			decodedRev, err := DecodeToDispatchRevision(encoded, revisions.CommonDecoder{
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
		{
			name:             "invalid",
			token:            "abc",
			expectedRevision: datastore.NoRevision,
			expectedSections: []string{},
			expectedHash:     "",
			expectError:      true,
		},
		{
			name:             "empty",
			token:            "Cg0KATEaCHNvbWVoYXNo",
			expectedRevision: revision1,
			expectedSections: nil,
			expectedHash:     "somehash",
			expectError:      false,
		},
		{
			name:             "basic",
			token:            "ChUKATESAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision1,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "another",
			expectError:      false,
		},
		{
			name:             "basic with wrong hash",
			token:            "ChUKATESAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision1,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "wrong",
			expectError:      true,
		},
		{
			name:             "basic with different revision",
			token:            "ChUKATISAWESAWISAWMaB2Fub3RoZXI=",
			expectedRevision: revision2,
			expectedSections: []string{"a", "b", "c"},
			expectedHash:     "another",
			expectError:      false,
		},
	} {
		testCase := testCase
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

			decodedRev, err := DecodeToDispatchRevision(&v1.Cursor{
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
