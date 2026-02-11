# Query Package

The `query` package implements SpiceDB's query plan system for evaluating permissions and relationships. It provides a tree-based iterator architecture for efficient graph traversal and permission checking.

## Overview

Query plans are built from schema definitions and executed to answer three fundamental questions:

1. **Check**: Does a specific subject have access to a resource?
2. **IterSubjects**: Which subjects have access to a given resource?
3. **IterResources**: Which resources can a given subject access?

Each iterator in the tree implements these three methods, allowing complex permission queries to be evaluated by composing simple operations.

## Iterator Types

Iterators are organized into several categories based on their role in the query plan:

| Iterator | Description |
|----------|-------------|
| **LEAF ITERATORS** | **Data source iterators with no subiterators** |
| DatastoreIterator | Queries the datastore for stored relationships. The fundamental data source for all queries. |
| FixedIterator | Contains pre-computed paths. Used in testing and as frontier in recursive queries. |
| SelfIterator | Produces reflexive relations (object→object). Used for `self` keyword checks. |
| RecursiveSentinelIterator | Placeholder marking recursion points during tree construction. Replaced at runtime. |
| | |
| **UNARY ITERATORS** | **Wrap a single child iterator** |
| AliasIterator | Rewrites the relation field of paths. Maps computed permissions to their defining relation. |
| | |
| **BINARY ITERATORS** | **Combine two child iterators** |
| ArrowIterator | Graph walk operator (`→`). Chains two iterators for path composition. |
| IntersectionArrowIterator | Conditional intersection. Implements `all()` semantics - ALL left subjects must satisfy right. |
| ExclusionIterator | Set difference (`-`). Removes excluded paths from main set. |
| | |
| **N-ARY ITERATORS** | **Combine multiple child iterators** |
| UnionIterator | Logical OR (`\|`). Concatenates results from all children with deduplication. |
| IntersectionIterator | Logical AND (`&`). Intersects results from all children. |
| | |
| **FILTER ITERATORS** | **Apply filtering conditions to results** |
| CaveatIterator | Evaluates caveat conditions on paths. Filters results based on runtime context. |
| | |
| **CONTROL FLOW ITERATORS** | **Manage execution flow and recursion** |
| RecursiveIterator | Manages recursive schema definitions with and represents the transitive closure operation. |

## Key Concepts

### Iterator Interface

All iterators implement the `Iterator` interface with these core methods:

```go
type Iterator interface {
    // Evaluation methods
    CheckImpl(ctx *Context, resources []Object, subject ObjectAndRelation) (PathSeq, error)
    IterSubjectsImpl(ctx *Context, resource Object, filterSubjectType ObjectType) (PathSeq, error)
    IterResourcesImpl(ctx *Context, subject ObjectAndRelation, filterResourceType ObjectType) (PathSeq, error)

    // Tree navigation
    Clone() Iterator
    Subiterators() []Iterator
    ReplaceSubiterators(newSubs []Iterator) (Iterator, error)

    // Metadata
    ID() string
    Explain() Explain
    ResourceType() ([]ObjectType, error)
    SubjectTypes() ([]ObjectType, error)
}
```

### Path Sequences

Results are returned as `PathSeq` (Go 1.23 iterators - `iter.Seq2[Path, error]`), allowing lazy evaluation and streaming of results without materializing entire result sets in memory.

### Context

The `Context` type carries execution state including:

- Datastore reader
- Optional trace logger for debugging
- Execution depth tracking
- Analysis/statistics collection

## Usage Example

```go
// Build a query plan from schema
it, err := BuildIteratorFromSchema(schema, "document", "viewer")

// Create execution context
ctx := NewContext(datastoreReader)

// Execute a check query
resources := []Object{NewObject("document", "doc1")}
subject := NewObject("user", "alice").WithEllipses()

pathSeq, err := ctx.Check(it, resources, subject)
for path, err := range pathSeq {
    if err != nil {
        return err
    }
    // Process path...
}
```
