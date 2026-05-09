//go:build cgo
// +build cgo

package pg_query

import (
	"google.golang.org/protobuf/proto"

	"github.com/pganalyze/pg_query_go/v6/parser"
)

func Scan(input string) (result *ScanResult, err error) {
	protobufScan, err := parser.ScanToProtobuf(input)
	if err != nil {
		return
	}
	result = &ScanResult{}
	err = proto.Unmarshal(protobufScan, result)
	return
}

// ParseToJSON - Parses the given SQL statement into a parse tree (JSON format)
func ParseToJSON(input string) (result string, err error) {
	return parser.ParseToJSON(input)
}

// Parse the given SQL statement into a parse tree (Go struct format)
func Parse(input string) (tree *ParseResult, err error) {
	protobufTree, err := parser.ParseToProtobuf(input)
	if err != nil {
		return
	}

	tree = &ParseResult{}
	err = proto.Unmarshal(protobufTree, tree)
	return
}

// Deparses a given Go parse tree into a SQL statement
func Deparse(tree *ParseResult) (output string, err error) {
	protobufTree, err := proto.Marshal(tree)
	if err != nil {
		return
	}

	output, err = parser.DeparseFromProtobuf(protobufTree)
	return
}

// ParsePlPgSqlToJSON - Parses the given PL/pgSQL function statement into a parse tree (JSON format)
func ParsePlPgSqlToJSON(input string) (result string, err error) {
	return parser.ParsePlPgSqlToJSON(input)
}

// Normalize the passed SQL statement to replace constant values with $n parameter references
func Normalize(input string) (result string, err error) {
	return parser.Normalize(input)
}

// Normalize the passed utility statement to replace constant values with $n parameter references
func NormalizeUtility(input string) (result string, err error) {
	return parser.NormalizeUtility(input)
}

// Fingerprint - Fingerprint the passed SQL statement to a hex string
func Fingerprint(input string) (result string, err error) {
	return parser.FingerprintToHexStr(input)
}

// FingerprintToUInt64 - Fingerprint the passed SQL statement to a uint64
func FingerprintToUInt64(input string) (result uint64, err error) {
	return parser.FingerprintToUInt64(input)
}

// HashXXH3_64 - Helper method to run XXH3 hash function (64-bit variant) on the given bytes, with the specified seed
func HashXXH3_64(input []byte, seed uint64) (result uint64) {
	return parser.HashXXH3_64(input, seed)
}

func SplitWithScanner(input string, trimSpace bool) (result []string, err error) {
	return parser.SplitWithScanner(input, trimSpace)
}

func SplitWithParser(input string, trimSpace bool) (result []string, err error) {
	return parser.SplitWithParser(input, trimSpace)
}
