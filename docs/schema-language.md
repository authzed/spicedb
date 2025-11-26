# SpiceDB Schema Language Guide

## Identifier Naming Conventions

### Private/Internal Identifiers

SpiceDB supports using underscore (`_`) as a prefix for identifiers to establish a convention for marking definitions as "private" or "internal". This is particularly useful for:

- **Synthetic permissions**: Permissions that exist only to compose other permissions
- **Internal relations**: Relations not meant to be directly referenced by application code
- **Implementation details**: Parts of your schema that may change without affecting the public API

### Examples

```zed
definition document {
    // Public relation - exposed to application code
    relation viewer: user
    
    // Private relation - used internally for permission composition
    relation _internal_viewer: user
    
    // Public permission using private components
    permission view = viewer + _internal_viewer
}

definition _internal_resource {
    // This entire resource type is marked as internal
    relation owner: user
}
```

### Best Practices

1. **Use underscore prefix for synthetic permissions**:
   ```zed
   definition resource {
       relation owner: user
       relation editor: user
       
       // Private synthetic permission
       permission _can_write = owner + editor
       
       // Public permissions built on private ones
       permission edit = _can_write
       permission delete = owner
   }
   ```

2. **Mark implementation details as private**:
   ```zed
   definition folder {
       relation parent: folder
       relation viewer: user
       
       // Private permission for traversal logic
       permission _parent_view = parent->view
       
       // Public permission combining direct and inherited access
       permission view = viewer + _parent_view
   }
   ```

3. **Future module system compatibility**: When SpiceDB introduces a module system for composing schemas via libraries, underscore-prefixed identifiers will help clearly distinguish between public and private APIs.

### Technical Details

- Identifiers can begin with either a letter (`a-z`) or underscore (`_`)
- After the first character, identifiers can contain letters, numbers, and underscores
- Identifiers must end with an alphanumeric character (not an underscore)
- Valid: `_private`, `_internal_relation`, `_helper123`
- Invalid: `_trailing_` (must end with alphanumeric), `123_start` (must start with letter or underscore)
- Note: `__double` is technically valid by the regex pattern but discouraged for readability

This naming convention is enforced at the schema level and is compatible with all SpiceDB operations and queries.