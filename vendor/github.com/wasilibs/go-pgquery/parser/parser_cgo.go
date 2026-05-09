//go:build pgquery_cgo || tinygo

package parser

import pganalyze "github.com/pganalyze/pg_query_go/v6/parser"

type Error = pganalyze.Error

// ParseToJSON - Parses the given SQL statement into a parse tree (JSON format)
func ParseToJSON(input string) (result string, err error) {
	return pganalyze.ParseToJSON(input)
}

// Scans the given SQL statement into a protobuf ScanResult
func ScanToProtobuf(input string) (result []byte, err error) {
	return pganalyze.ScanToProtobuf(input)
}

// ParseToProtobuf - Parses the given SQL statement into a parse tree (Protobuf format)
func ParseToProtobuf(input string) (result []byte, err error) {
	return pganalyze.ParseToProtobuf(input)
}

// DeparseFromProtobuf - Deparses the given Protobuf format parse tree into a SQL statement
func DeparseFromProtobuf(input []byte) (result string, err error) {
	return pganalyze.DeparseFromProtobuf(input)
}

// ParsePlPgSqlToJSON - Parses the given PL/pgSQL function statement into a parse tree (JSON format)
func ParsePlPgSqlToJSON(input string) (result string, err error) {
	return pganalyze.ParsePlPgSqlToJSON(input)
}

// Normalize the passed SQL statement to replace constant values with ? characters
func Normalize(input string) (result string, err error) {
	return pganalyze.Normalize(input)
}

// FingerprintToUInt64 - Fingerprint the passed SQL statement using the C extension and returns result as uint64
func FingerprintToUInt64(input string) (result uint64, err error) {
	return pganalyze.FingerprintToUInt64(input)
}

// FingerprintToHexStr - Fingerprint the passed SQL statement using the C extension and returns result as hex string
func FingerprintToHexStr(input string) (result string, err error) {
	return pganalyze.FingerprintToHexStr(input)
}

// HashXXH3_64 - Helper method to run XXH3 hash function (64-bit variant) on the given bytes, with the specified seed
func HashXXH3_64(input []byte, seed uint64) (result uint64) {
	return pganalyze.HashXXH3_64(input, seed)
}
