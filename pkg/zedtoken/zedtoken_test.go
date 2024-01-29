package zedtoken

import (
	"fmt"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"

	"github.com/authzed/spicedb/internal/datastore/revisions"
	"github.com/authzed/spicedb/pkg/datastore"
)

var encodeRevisionTests = []datastore.Revision{
	revisions.NewForTransactionID(1),
	revisions.NewForTransactionID(2),
	revisions.NewForTransactionID(4),
	revisions.NewForTransactionID(8),
	revisions.NewForTransactionID(16),
	revisions.NewForTransactionID(128),
	revisions.NewForTransactionID(256),
	revisions.NewForTransactionID(1621538189028928000),
}

func mustHLC(str string) datastore.Revision {
	rev, err := revisions.HLCRevisionFromString(str)
	if err != nil {
		panic(err)
	}
	return rev
}

var encodeHLCRevisionTests = []datastore.Revision{
	mustHLC("1235"),
	mustHLC("1234.0000000001"),
}

func TestZedTokenEncode(t *testing.T) {
	for _, rev := range encodeRevisionTests {
		rev := rev
		t.Run(rev.String(), func(t *testing.T) {
			require := require.New(t)
			encoded := MustNewFromRevisionForTesting(rev)

			decoded, _, err := DecodeRevision(encoded, revisions.CommonDecoder{
				Kind: revisions.TransactionID,
			})
			require.NoError(err)
			require.True(rev.Equal(decoded))
		})
	}
}

func TestZedTokenEncodeHLC(t *testing.T) {
	for _, rev := range encodeHLCRevisionTests {
		rev := rev
		t.Run(rev.String(), func(t *testing.T) {
			require := require.New(t)
			encoded := MustNewFromRevisionForTesting(rev)

			decoded, _, err := DecodeRevision(encoded, revisions.CommonDecoder{
				Kind: revisions.HybridLogicalClock,
			})
			require.NoError(err)
			require.True(rev.Equal(decoded))
		})
	}
}

var decodeTests = []struct {
	format            string
	token             string
	datastoreUniqueID string
	expectedRevision  datastore.Revision
	expectedStatus    TokenStatus
	expectError       bool
}{
	{
		format:           "invalid",
		token:            "abc",
		expectedRevision: datastore.NoRevision,
		expectedStatus:   StatusUnknown,
		expectError:      true,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAA==",
		expectedRevision: revisions.NewForTransactionID(0),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAggB",
		expectedRevision: revisions.NewForTransactionID(1),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAggC",
		expectedRevision: revisions.NewForTransactionID(2),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAESAwiAAg==",
		expectedRevision: revisions.NewForTransactionID(256),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 Zookie",
		token:            "CAIaAwoBMA==",
		expectedRevision: revisions.NewForTransactionID(0),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBMQ==",
		expectedRevision: revisions.NewForTransactionID(1),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBMg==",
		expectedRevision: revisions.NewForTransactionID(2),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:           "V1 ZedToken",
		token:            "CAIaAwoBNA==",
		expectedRevision: revisions.NewForTransactionID(4),
		expectedStatus:   StatusLegacyEmptyDatastoreID,
		expectError:      false,
	},
	{
		format:            "V1 ZedToken with matching datastore unique ID",
		token:             "Gg4KAjQyEghzb21ldW5pcQ==",
		datastoreUniqueID: "someuniqueid",
		expectedRevision:  revisions.NewForTransactionID(42),
		expectedStatus:    StatusValid,
		expectError:       false,
	},
	{
		format:            "V1 ZedToken with mismatched datastore unique ID",
		token:             "Gg4KAjQyEghzb21ldW5pcQ==",
		datastoreUniqueID: "anotheruniqueid",
		expectedRevision:  revisions.NewForTransactionID(42),
		expectedStatus:    StatusMismatchedDatastoreID,
		expectError:       false,
	},
}

func TestDecode(t *testing.T) {
	for _, testCase := range decodeTests {
		testCase := testCase
		testName := fmt.Sprintf("%s(%s)=>%s", testCase.format, testCase.token, testCase.expectedRevision)
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)

			decoded, status, err := DecodeRevision(&v1.ZedToken{
				Token: testCase.token,
			}, revisions.CommonDecoder{
				DatastoreUniqueID: testCase.datastoreUniqueID,
				Kind:              revisions.TransactionID,
			})
			if testCase.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(testCase.expectedStatus, status)
				require.True(
					testCase.expectedRevision.Equal(decoded),
					"%s != %s",
					testCase.expectedRevision,
					decoded,
				)
			}
		})
	}
}

var hlcDecodeTests = []struct {
	format            string
	token             string
	datastoreUniqueID string
	expectedRevision  datastore.Revision
	expectedStatus    TokenStatus
	expectError       bool
}{
	{
		format:         "V1 ZedToken",
		token:          "CAIaFQoTMTYyMTUzODE4OTAyODkyODAwMA==",
		expectedStatus: StatusLegacyEmptyDatastoreID,
		expectedRevision: func() datastore.Revision {
			r, err := revisions.NewForHLC(decimal.NewFromInt(1621538189028928000))
			if err != nil {
				panic(err)
			}
			return r
		}(),
	},
	{
		format:         "V1 ZedToken",
		token:          "GiAKHjE2OTM1NDA5NDAzNzMwNDU3MjcuMDAwMDAwMDAwMQ==",
		expectedStatus: StatusLegacyEmptyDatastoreID,
		expectedRevision: (func() datastore.Revision {
			v, err := decimal.NewFromString("1693540940373045727.0000000001")
			if err != nil {
				panic(err)
			}
			r, err := revisions.NewForHLC(v)
			if err != nil {
				panic(err)
			}
			return r
		})(),
		expectError: false,
	},
	{
		format:            "V1 ZedToken with matching datastore unique ID",
		token:             "GioKHjE2OTM1NDA5NDAzNzMwNDU3MjcuMDAwMDAwMDAwMRIINjM0OWFhZjI=",
		datastoreUniqueID: "6349aaf2-37cd-47b9-84e8-fe5fa6e2dead",
		expectedStatus:    StatusValid,
		expectedRevision: (func() datastore.Revision {
			v, err := decimal.NewFromString("1693540940373045727.0000000001")
			if err != nil {
				panic(err)
			}
			r, err := revisions.NewForHLC(v)
			if err != nil {
				panic(err)
			}
			return r
		})(),
		expectError: false,
	},
	{
		format:            "V1 ZedToken with mismatched datastore unique ID",
		token:             "GioKHjE2OTM1NDA5NDAzNzMwNDU3MjcuMDAwMDAwMDAwMRIINjM0OWFhZjI=",
		datastoreUniqueID: "arrrg-6349aaf2-37cd-47b9-84e8-fe5fa6e2dead",
		expectedStatus:    StatusMismatchedDatastoreID,
		expectedRevision: (func() datastore.Revision {
			v, err := decimal.NewFromString("1693540940373045727.0000000001")
			if err != nil {
				panic(err)
			}

			r, err := revisions.NewForHLC(v)
			if err != nil {
				panic(err)
			}

			return r
		})(),
		expectError: false,
	},
}

func TestHLCDecode(t *testing.T) {
	for _, testCase := range hlcDecodeTests {
		testCase := testCase
		testName := fmt.Sprintf("%s(%s)=>%s", testCase.format, testCase.token, testCase.expectedRevision)
		t.Run(testName, func(t *testing.T) {
			require := require.New(t)

			decoded, status, err := DecodeRevision(&v1.ZedToken{
				Token: testCase.token,
			}, revisions.CommonDecoder{
				DatastoreUniqueID: testCase.datastoreUniqueID,
				Kind:              revisions.HybridLogicalClock,
			})
			if testCase.expectError {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(testCase.expectedStatus, status)
				require.True(
					testCase.expectedRevision.Equal(decoded),
					"%s != %s",
					testCase.expectedRevision,
					decoded,
				)
			}
		})
	}
}
