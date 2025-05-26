// Package cursorediterator provides a series of specialized iterator builders that support
// construction of a tree of iterators, returning standard Go Seq2 iterators which wrap all
// the complexity.
//
// This is useful for building paginated APIs, where the client can request
// a specific page of results, and the server can return a cursor to the next page,
// while automatically handling all the complexities of context cancellation,
// error handling, and cursor management.
//
// Take a look at integration_test.go for a full example of how to use
// this package in practice.
package cursorediterator
