// This package provides the structures, interfaces, and functions for a sort of Lego-set to build query trees.
// It should depend on as few other packages as possible, as in the longer future, these can be passed around a lot, and refactoring to reduce import loops is a pain.
//
// The underlying philosophy of a query plan is that we build query trees out of iterators.
// Iterators are nodes in the tree and represent a logical set of valid relations that match or don't match.
//
// Take the example schema:
//
//	definition foo {
//	  relation bar: foo
//	}
//
// For example, the simplest set is that of `bar` -- all relationships written directly with `bar` as the Relation type, `foo:a#bar@foo:b#...`
//
// But by combining different operations on these sets, we can invent arbitrary permissions, using standard set operations like And and Or, along with
// a few special ones that come from relational algebra, like Arrow (as a form of the Join operation).
package query
