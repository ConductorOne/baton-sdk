// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: c1/transport/svc/transport/v1/transport.proto

package v1

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
)

// Validate checks the field values on Request with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Request) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Request with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in RequestMultiError, or nil if none found.
func (m *Request) ValidateAll() error {
	return m.validate(true)
}

func (m *Request) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Method

	if all {
		switch v := interface{}(m.GetReq()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, RequestValidationError{
					field:  "Req",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, RequestValidationError{
					field:  "Req",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetReq()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return RequestValidationError{
				field:  "Req",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetHeaders()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, RequestValidationError{
					field:  "Headers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, RequestValidationError{
					field:  "Headers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetHeaders()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return RequestValidationError{
				field:  "Headers",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return RequestMultiError(errors)
	}

	return nil
}

// RequestMultiError is an error wrapping multiple validation errors returned
// by Request.ValidateAll() if the designated constraints aren't met.
type RequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m RequestMultiError) Error() string {
	msgs := make([]string, 0, len(m))
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m RequestMultiError) AllErrors() []error { return m }

// RequestValidationError is the validation error returned by Request.Validate
// if the designated constraints aren't met.
type RequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e RequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e RequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e RequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e RequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e RequestValidationError) ErrorName() string { return "RequestValidationError" }

// Error satisfies the builtin error interface
func (e RequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = RequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = RequestValidationError{}

// Validate checks the field values on Response with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Response) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Response with the rules defined in
// the proto definition for this message. If any rules are violated, the
// result is a list of violation errors wrapped in ResponseMultiError, or nil
// if none found.
func (m *Response) ValidateAll() error {
	return m.validate(true)
}

func (m *Response) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetResp()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Resp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Resp",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetResp()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResponseValidationError{
				field:  "Resp",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetStatus()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Status",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Status",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetStatus()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResponseValidationError{
				field:  "Status",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetHeaders()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Headers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Headers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetHeaders()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResponseValidationError{
				field:  "Headers",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetTrailers()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Trailers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ResponseValidationError{
					field:  "Trailers",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetTrailers()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ResponseValidationError{
				field:  "Trailers",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return ResponseMultiError(errors)
	}

	return nil
}

// ResponseMultiError is an error wrapping multiple validation errors returned
// by Response.ValidateAll() if the designated constraints aren't met.
type ResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ResponseMultiError) Error() string {
	msgs := make([]string, 0, len(m))
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ResponseMultiError) AllErrors() []error { return m }

// ResponseValidationError is the validation error returned by
// Response.Validate if the designated constraints aren't met.
type ResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ResponseValidationError) ErrorName() string { return "ResponseValidationError" }

// Error satisfies the builtin error interface
func (e ResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ResponseValidationError{}
