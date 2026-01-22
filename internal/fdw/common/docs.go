// Package common provides error handling utilities for the FDW package.
//
// This package defines error constructors that wrap errors with appropriate
// Postgres error codes and severity levels for proper client error reporting.
// It also provides conversions from gRPC status codes to Postgres error codes.
package common
