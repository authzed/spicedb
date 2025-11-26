# Private Identifiers with Underscore Prefix

SpiceDB now supports using underscore (`_`) as a prefix for relation and permission names to indicate that they are private or internal. This follows a common programming convention where underscore-prefixed identifiers signify private/internal implementation details.

## Use Cases

### 1. Internal Relations

Use underscore-prefixed relations for internal state that shouldn't be directly referenced by external systems:

```zed
definition document {
    // Public relations
    relation viewer: user
    relation editor: user
    
    // Private/internal relations
    relation _deleted: user  // Internal flag for soft-deleted documents
    relation _system_admin: user  // Internal admin access
    
    // Public permissions use the private relations
    permission view = viewer + editor - _deleted
    permission edit = editor + _system_admin - _deleted
    permission admin = _system_admin
}
```

### 2. Synthetic Permissions

Underscore prefix is particularly useful for synthetic permissions that exist only to support arrow operations:

```zed
definition organization {
    relation member: user
    relation admin: user
    
    // Private synthetic permission for arrow operations
    permission _base_member = member + admin
}

definition project {
    relation parent: organization
    
    // Use the private permission via arrow
    permission member = parent->_base_member
}
```

### 3. Implementation Details

Hide complex permission logic behind private permissions:

```zed
definition repository {
    relation owner: user | team#member
    relation contributor: user | team#member
    relation reader: user | team#member
    
    // Private permissions for internal logic
    permission _can_read = reader + contributor + owner
    permission _can_write = contributor + owner
    permission _can_admin = owner
    
    // Public permissions with clean interface
    permission read = _can_read
    permission write = _can_write
    permission admin = _can_admin
}
```

## Naming Rules

- Identifiers can start with lowercase letter `a-z` OR underscore `_`
- After the first character, they can contain `a-z`, `0-9`, or `_`
- Must be 2-64 characters long
- Must end with `a-z` or `0-9` (not underscore)

### Valid Examples

- `_private`
- `_internal_state`
- `_synthetic_permission`
- `_temp_access`

### Invalid Examples

- `__double` (still valid, but discouraged)
- `_` (too short)
- `_private_` (cannot end with underscore)
- `Private` (cannot start with uppercase)

## Best Practices

1. **Consistency**: Use underscore prefix consistently across your schema for all private identifiers
2. **Documentation**: Document why a relation/permission is private
3. **Module Systems**: When schema modules are implemented, private identifiers will not be exported
4. **Avoid Direct Access**: Client applications should not reference underscore-prefixed identifiers
5. **Migration**: When making a public identifier private, create a migration path for existing references

## Future Compatibility

The underscore prefix convention will integrate with future features:

- **Module System**: Private identifiers won't be exported from modules
- **Schema Validation**: Tools can warn when external schemas reference private identifiers
- **Documentation**: API documentation generators can hide or mark private identifiers

## Example Schema

Here's a complete example showing proper use of private identifiers:

```zed
definition user {}

definition team {
    relation member: user
    relation admin: user
    
    // Private permission for internal use
    permission _all_members = member + admin
}

definition organization {
    relation team: team
    relation direct_member: user
    
    // Private permission that combines different member types
    permission _internal_members = direct_member + team->_all_members
    
    // Public API
    permission member = _internal_members
}

definition document {
    relation org: organization
    relation viewer: user | organization#member
    relation editor: user | organization#member
    
    // Private state
    relation _archived: user  // Who archived this document
    relation _system_locked: user  // System-level lock
    
    // Private permissions for complex logic
    permission _base_view = viewer + editor + org->member
    permission _is_restricted = _archived + _system_locked
    
    // Public permissions
    permission view = _base_view - _is_restricted
    permission edit = editor - _is_restricted
    permission restore = _archived  // Only who archived can restore
}
```

This schema demonstrates:

- Private relations for internal state (`_archived`, `_system_locked`)
- Private permissions for complex logic (`_base_view`, `_is_restricted`)
- Clean public API that hides implementation details
- Use of private identifiers across relationships
