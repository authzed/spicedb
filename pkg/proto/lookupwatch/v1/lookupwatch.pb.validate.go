// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: lookupwatch/v1/lookupwatch.proto

package lookupwatchv1

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"

	v1 "github.com/authzed/authzed-go/proto/authzed/api/v1"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
	_ = sort.Sort

	_ = v1.CheckPermissionResponse_Permissionship(0)
)

// Validate checks the field values on WatchAccessibleResourcesRequest with the
// rules defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *WatchAccessibleResourcesRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on WatchAccessibleResourcesRequest with
// the rules defined in the proto definition for this message. If any rules
// are violated, the result is a list of violation errors wrapped in
// WatchAccessibleResourcesRequestMultiError, or nil if none found.
func (m *WatchAccessibleResourcesRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *WatchAccessibleResourcesRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for ResourceObjectType

	// no validation rules for Permission

	// no validation rules for SubjectObjectType

	// no validation rules for OptionalSubjectRelation

	if all {
		switch v := interface{}(m.GetOptionalStartTimestamp()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, WatchAccessibleResourcesRequestValidationError{
					field:  "OptionalStartTimestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, WatchAccessibleResourcesRequestValidationError{
					field:  "OptionalStartTimestamp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetOptionalStartTimestamp()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return WatchAccessibleResourcesRequestValidationError{
				field:  "OptionalStartTimestamp",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return WatchAccessibleResourcesRequestMultiError(errors)
	}

	return nil
}

// WatchAccessibleResourcesRequestMultiError is an error wrapping multiple
// validation errors returned by WatchAccessibleResourcesRequest.ValidateAll()
// if the designated constraints aren't met.
type WatchAccessibleResourcesRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m WatchAccessibleResourcesRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m WatchAccessibleResourcesRequestMultiError) AllErrors() []error { return m }

// WatchAccessibleResourcesRequestValidationError is the validation error
// returned by WatchAccessibleResourcesRequest.Validate if the designated
// constraints aren't met.
type WatchAccessibleResourcesRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e WatchAccessibleResourcesRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e WatchAccessibleResourcesRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e WatchAccessibleResourcesRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e WatchAccessibleResourcesRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e WatchAccessibleResourcesRequestValidationError) ErrorName() string {
	return "WatchAccessibleResourcesRequestValidationError"
}

// Error satisfies the builtin error interface
func (e WatchAccessibleResourcesRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sWatchAccessibleResourcesRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = WatchAccessibleResourcesRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = WatchAccessibleResourcesRequestValidationError{}

// Validate checks the field values on PermissionUpdate with the rules defined
// in the proto definition for this message. If any rules are violated, the
// first error encountered is returned, or nil if there are no violations.
func (m *PermissionUpdate) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on PermissionUpdate with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// PermissionUpdateMultiError, or nil if none found.
func (m *PermissionUpdate) ValidateAll() error {
	return m.validate(true)
}

func (m *PermissionUpdate) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetSubject()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, PermissionUpdateValidationError{
					field:  "Subject",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, PermissionUpdateValidationError{
					field:  "Subject",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSubject()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return PermissionUpdateValidationError{
				field:  "Subject",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetResource()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, PermissionUpdateValidationError{
					field:  "Resource",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, PermissionUpdateValidationError{
					field:  "Resource",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetResource()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return PermissionUpdateValidationError{
				field:  "Resource",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for UpdatedPermission

	if len(errors) > 0 {
		return PermissionUpdateMultiError(errors)
	}

	return nil
}

// PermissionUpdateMultiError is an error wrapping multiple validation errors
// returned by PermissionUpdate.ValidateAll() if the designated constraints
// aren't met.
type PermissionUpdateMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m PermissionUpdateMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m PermissionUpdateMultiError) AllErrors() []error { return m }

// PermissionUpdateValidationError is the validation error returned by
// PermissionUpdate.Validate if the designated constraints aren't met.
type PermissionUpdateValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e PermissionUpdateValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e PermissionUpdateValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e PermissionUpdateValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e PermissionUpdateValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e PermissionUpdateValidationError) ErrorName() string { return "PermissionUpdateValidationError" }

// Error satisfies the builtin error interface
func (e PermissionUpdateValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sPermissionUpdate.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = PermissionUpdateValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = PermissionUpdateValidationError{}

// Validate checks the field values on WatchAccessibleResourcesResponse with
// the rules defined in the proto definition for this message. If any rules
// are violated, the first error encountered is returned, or nil if there are
// no violations.
func (m *WatchAccessibleResourcesResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on WatchAccessibleResourcesResponse with
// the rules defined in the proto definition for this message. If any rules
// are violated, the result is a list of violation errors wrapped in
// WatchAccessibleResourcesResponseMultiError, or nil if none found.
func (m *WatchAccessibleResourcesResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *WatchAccessibleResourcesResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	for idx, item := range m.GetUpdates() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, WatchAccessibleResourcesResponseValidationError{
						field:  fmt.Sprintf("Updates[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, WatchAccessibleResourcesResponseValidationError{
						field:  fmt.Sprintf("Updates[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return WatchAccessibleResourcesResponseValidationError{
					field:  fmt.Sprintf("Updates[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if all {
		switch v := interface{}(m.GetChangesThrough()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, WatchAccessibleResourcesResponseValidationError{
					field:  "ChangesThrough",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, WatchAccessibleResourcesResponseValidationError{
					field:  "ChangesThrough",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetChangesThrough()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return WatchAccessibleResourcesResponseValidationError{
				field:  "ChangesThrough",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return WatchAccessibleResourcesResponseMultiError(errors)
	}

	return nil
}

// WatchAccessibleResourcesResponseMultiError is an error wrapping multiple
// validation errors returned by
// WatchAccessibleResourcesResponse.ValidateAll() if the designated
// constraints aren't met.
type WatchAccessibleResourcesResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m WatchAccessibleResourcesResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m WatchAccessibleResourcesResponseMultiError) AllErrors() []error { return m }

// WatchAccessibleResourcesResponseValidationError is the validation error
// returned by WatchAccessibleResourcesResponse.Validate if the designated
// constraints aren't met.
type WatchAccessibleResourcesResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e WatchAccessibleResourcesResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e WatchAccessibleResourcesResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e WatchAccessibleResourcesResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e WatchAccessibleResourcesResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e WatchAccessibleResourcesResponseValidationError) ErrorName() string {
	return "WatchAccessibleResourcesResponseValidationError"
}

// Error satisfies the builtin error interface
func (e WatchAccessibleResourcesResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sWatchAccessibleResourcesResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = WatchAccessibleResourcesResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = WatchAccessibleResourcesResponseValidationError{}
