package validation

import (
	"errors"
	"fmt"
	"regexp"
)

const identifier = "[a-z][a-z0-9_]{2,62}[a-z0-9]"

var (
	// RelationNameRegex is the regular expression used to validate the names of relations.
	RelationNameRegex = regexp.MustCompile(fmt.Sprintf("^(\\.\\.\\.|%s)$", identifier))

	// NamespaceRegex is the regular expression used to validate namespace names.
	NamespaceRegex = regexp.MustCompile(fmt.Sprintf("^(%s/)?%s$", identifier, identifier))

	// NamespaceRegex is the regular expression used to validate namespace names
	// that require tenant slugs.
	NamespaceWithTenantRegex = regexp.MustCompile(fmt.Sprintf("^(%s)/(%s)$", identifier, identifier))

	ErrInvalidRelationName  = errors.New("invalid relation name")
	ErrInvalidNamespaceName = errors.New("invalid namespace name")
)

// RelationName validates that the string provided is a valid relation name.
func RelationName(name string) error {
	matched := RelationNameRegex.MatchString(name)
	if !matched {
		return ErrInvalidRelationName
	}

	return nil
}

// NamespaceName validates that the string provided is a valid namespace name.
func NamespaceName(name string) error {
	matched := NamespaceRegex.MatchString(name)
	if !matched {
		return ErrInvalidNamespaceName
	}

	return nil
}

// NamespaceNameWithTenant validates that the string provided is a valid namespace
// name and contains a tenant component.
func NamespaceNameWithTenant(name string) error {
	matched := NamespaceWithTenantRegex.MatchString(name)
	if !matched {
		return ErrInvalidNamespaceName
	}

	return nil
}
