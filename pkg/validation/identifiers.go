package validation

import (
	"errors"
	"fmt"
	"regexp"
)

const identifier = "[a-z][a-z0-9_]{2,62}[a-z0-9]"

var (
	relationNameRegex        = regexp.MustCompile(fmt.Sprintf("^(\\.\\.\\.|%s)$", identifier))
	namespaceRegex           = regexp.MustCompile(fmt.Sprintf("^(%s/)?%s$", identifier, identifier))
	namespaceWithTenantRegex = regexp.MustCompile(fmt.Sprintf("^%s/%s$", identifier, identifier))

	ErrInvalidRelationName  = errors.New("invalid relation name")
	ErrInvalidNamespaceName = errors.New("invalid namespace name")
)

// RelationName validates that the string provided is a valid relation name.
func RelationName(name string) error {
	matched := relationNameRegex.MatchString(name)
	if !matched {
		return ErrInvalidRelationName
	}

	return nil
}

// NamespaceName validates that the string provided is a valid namespace name.
func NamespaceName(name string) error {
	matched := namespaceRegex.MatchString(name)
	if !matched {
		return ErrInvalidNamespaceName
	}

	return nil
}

// NamespaceNameWithTenant validates that the string provided is a valid namespace
// name and contains a tenant component.
func NamespaceNameWithTenant(name string) error {
	matched := namespaceWithTenantRegex.MatchString(name)
	if !matched {
		return ErrInvalidNamespaceName
	}

	return nil
}
