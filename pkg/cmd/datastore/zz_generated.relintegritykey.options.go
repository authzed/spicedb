// Code generated by github.com/ecordell/optgen. DO NOT EDIT.
package datastore

import (
	defaults "github.com/creasty/defaults"
	helpers "github.com/ecordell/optgen/helpers"
)

type RelIntegrityKeyOption func(r *RelIntegrityKey)

// NewRelIntegrityKeyWithOptions creates a new RelIntegrityKey with the passed in options set
func NewRelIntegrityKeyWithOptions(opts ...RelIntegrityKeyOption) *RelIntegrityKey {
	r := &RelIntegrityKey{}
	for _, o := range opts {
		o(r)
	}
	return r
}

// NewRelIntegrityKeyWithOptionsAndDefaults creates a new RelIntegrityKey with the passed in options set starting from the defaults
func NewRelIntegrityKeyWithOptionsAndDefaults(opts ...RelIntegrityKeyOption) *RelIntegrityKey {
	r := &RelIntegrityKey{}
	defaults.MustSet(r)
	for _, o := range opts {
		o(r)
	}
	return r
}

// ToOption returns a new RelIntegrityKeyOption that sets the values from the passed in RelIntegrityKey
func (r *RelIntegrityKey) ToOption() RelIntegrityKeyOption {
	return func(to *RelIntegrityKey) {
		to.KeyID = r.KeyID
		to.KeyFilename = r.KeyFilename
	}
}

// DebugMap returns a map form of RelIntegrityKey for debugging
func (r RelIntegrityKey) DebugMap() map[string]any {
	debugMap := map[string]any{}
	debugMap["KeyID"] = helpers.DebugValue(r.KeyID, false)
	debugMap["KeyFilename"] = helpers.DebugValue(r.KeyFilename, false)
	return debugMap
}

// RelIntegrityKeyWithOptions configures an existing RelIntegrityKey with the passed in options set
func RelIntegrityKeyWithOptions(r *RelIntegrityKey, opts ...RelIntegrityKeyOption) *RelIntegrityKey {
	for _, o := range opts {
		o(r)
	}
	return r
}

// WithOptions configures the receiver RelIntegrityKey with the passed in options set
func (r *RelIntegrityKey) WithOptions(opts ...RelIntegrityKeyOption) *RelIntegrityKey {
	for _, o := range opts {
		o(r)
	}
	return r
}

// WithKeyID returns an option that can set KeyID on a RelIntegrityKey
func WithKeyID(keyID string) RelIntegrityKeyOption {
	return func(r *RelIntegrityKey) {
		r.KeyID = keyID
	}
}

// WithKeyFilename returns an option that can set KeyFilename on a RelIntegrityKey
func WithKeyFilename(keyFilename string) RelIntegrityKeyOption {
	return func(r *RelIntegrityKey) {
		r.KeyFilename = keyFilename
	}
}
