// schema/v2 provides a convenient Go representation of SpiceDB's schema definitions,
// built on top of the raw protocol buffer types from core.v1.
//
// This package converts low-level protobuf structures (NamespaceDefinition, CaveatDefinition)
// into ergonomic Schema views that are easier to reason about and work with programatically.
// The basic types map very closely to the schema language itself.
//
// The proto remains the source of truth; this makes traversing the schema to build queries or determine reachability much easier.
package schema
