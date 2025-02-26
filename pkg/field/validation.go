package field

import (
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"

	v1_conf "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	"github.com/conductorone/baton-sdk/pkg/ustrings"
)

type ErrConfigurationMissingFields struct {
	errors []error
}

func ValidateIntRules(r *v1_conf.Int64Rules, vInt int, name string) error {
	if r == nil {
		return nil
	}
	v := int64(vInt)
	if r.IsRequired && v == 0 {
		return fmt.Errorf("field %s of type int is marked as required but it has a zero-value", name)
	}

	if !r.ValidateEmpty && v == 0 {
		return nil
	}
	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("field %s: expected %v but got %v", name, *r.Eq, v)
	}
	if r.Lt != nil && v >= *r.Lt {
		return fmt.Errorf("field %s: value must be less than %d but got %d", name, *r.Lt, v)
	}
	if r.Lte != nil && v > *r.Lte {
		return fmt.Errorf("field %s: value must be less than or equal to %d but got %d", name, *r.Lte, v)
	}
	if r.Gt != nil && v <= *r.Gt {
		return fmt.Errorf("field %s: value must be greater than %d but got %d", name, *r.Gt, v)
	}
	if r.Gte != nil && v < *r.Gte {
		return fmt.Errorf("field %s: value must be greater than or equal to %d but got %d", name, *r.Gte, v)
	}
	if r.In != nil {
		found := false
		for _, val := range r.In {
			if v == val {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field %s: value must be one of %v but got %d", name, r.In, v)
		}
	}
	if r.NotIn != nil {
		for _, val := range r.NotIn {
			if v == val {
				return fmt.Errorf("field %s: value must not be one of %v but got %d", name, r.NotIn, v)
			}
		}
	}
	return nil
}

func ValidateBoolRules(r *v1_conf.BoolRules, v bool, name string) error {
	if r == nil {
		return nil
	}
	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("expected %v but got %v", *r.Eq, v)
	}
	return nil
}

// ValidateHostname checks if the given string is a valid hostname or IP address.
func validateHostname(hostname string) bool {
	// Check if it's an IP address
	if net.ParseIP(hostname) != nil {
		return true
	}

	// Check if it follows hostname rules
	return isValidDomain(hostname)
}

// isValidDomain checks if a hostname follows domain name rules.
func isValidDomain(hostname string) bool {
	if len(hostname) == 0 || len(hostname) > 253 {
		return false
	}

	// Hostname must not start or end with a dot
	if hostname[0] == '.' || hostname[len(hostname)-1] == '.' {
		return false
	}

	// Split by dots
	labels := strings.Split(hostname, ".")

	for _, label := range labels {
		if len(label) == 0 || len(label) > 63 {
			return false
		}

		// Each label must start & end with a letter or digit (RFC 1035)
		if !isAlphaNumeric(label[0]) || !isAlphaNumeric(label[len(label)-1]) {
			return false
		}

		// Labels can only contain letters, digits, or hyphens
		for i := range label {
			if !(isAlphaNumeric(label[i]) || label[i] == '-') {
				return false
			}
		}
	}

	return true
}

// isAlphaNumeric checks if a byte is a letter or digit.
func isAlphaNumeric(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9')
}

func ValidateStringRules(r *v1_conf.StringRules, v string, name string) error {
	if r == nil {
		return nil
	}

	if r.IsRequired && v == "" {
		return fmt.Errorf("field %s of type string is marked as required but it has a zero-value", name)
	}

	if !r.ValidateEmpty && v == "" {
		return nil
	}

	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("field %s: expected '%v' but got '%v'", name, *r.Eq, v)
	}
	if r.Len != nil && uint64(len(v)) != *r.Len {
		return fmt.Errorf("field %s: value must be exactly %d characters long but got %d", name, *r.Len, len(v))
	}
	if r.MinLen != nil && uint64(len(v)) < *r.MinLen {
		return fmt.Errorf("field %s: value must be at least %d characters long but got %d", name, *r.MinLen, len(v))
	}
	if r.MaxLen != nil && uint64(len(v)) > *r.MaxLen {
		return fmt.Errorf("field %s: value must be at most %d characters long but got %d", name, *r.MaxLen, len(v))
	}
	if r.Pattern != nil {
		pattern, err := regexp.CompilePOSIX(*r.Pattern)
		if err != nil {
			return fmt.Errorf("field %s: invalid pattern: %w", name, err)
		}
		if !pattern.MatchString(v) {
			return fmt.Errorf("field %s: value must match pattern %s but got '%s'", name, pattern.String(), v)
		}
	}
	if r.Prefix != nil && !strings.HasPrefix(v, *r.Prefix) {
		return fmt.Errorf("field %s: value must have prefix '%s' but got '%s'", name, *r.Prefix, v)
	}
	if r.Suffix != nil && !strings.HasSuffix(v, *r.Suffix) {
		return fmt.Errorf("field %s: value must have suffix '%s' but got '%s'", name, *r.Suffix, v)
	}
	if r.Contains != nil && !strings.Contains(v, *r.Contains) {
		return fmt.Errorf("field %s: value must contain '%s' but got '%s'", name, *r.Contains, v)
	}
	if r.In != nil {
		found := false
		for _, val := range r.In {
			if v == val {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("field %s: value must be one of %v but got '%s'", name, r.In, v)
		}
	}
	if r.NotIn != nil {
		for _, val := range r.NotIn {
			if v == val {
				return fmt.Errorf("field %s: value must not be one of %v but got '%s'", name, r.NotIn, v)
			}
		}
	}
	if r.WellKnown == v1_conf.WellKnownString_WELL_KNOWN_STRING_UNSPECIFIED {
		return nil
	}

	switch r.WellKnown {
	case v1_conf.WellKnownString_WELL_KNOWN_STRING_EMAIL:
		_, err := mail.ParseAddress(v)
		if err != nil {
			return fmt.Errorf("field %s: value must be a valid email address but got '%s'", name, v)
		}

	case v1_conf.WellKnownString_WELL_KNOWN_STRING_URI:
		_, err := url.ParseRequestURI(v)
		if err != nil {
			return fmt.Errorf("field %s: value must be a valid URL but got '%s'", name, v)
		}

	case v1_conf.WellKnownString_WELL_KNOWN_STRING_UUID:
		_, err := uuid.Parse(v)
		if err != nil {
			return fmt.Errorf("field %s: value must be a valid UUID but got '%s'", name, v)
		}

	case v1_conf.WellKnownString_WELL_KNOWN_STRING_HOSTNAME:
		if !validateHostname(v) {
			return fmt.Errorf("field %s: value must be a valid hostname but got '%s'", name, v)
		}
	case v1_conf.WellKnownString_WELL_KNOWN_STRING_IP:
		if net.ParseIP(v) == nil {
			return fmt.Errorf("field %s: value must be a valid IP address but got '%s'", name, v)
		}
	case v1_conf.WellKnownString_WELL_KNOWN_STRING_IPV4:
		ip := net.ParseIP(v)
		if ip == nil || ip.To4() == nil {
			return fmt.Errorf("field %s: value must be a valid IPv4 address but got '%s'", name, v)
		}
	case v1_conf.WellKnownString_WELL_KNOWN_STRING_IPV6:
		ip := net.ParseIP(v)
		if ip == nil || ip.To16() == nil {
			return fmt.Errorf("field %s: value must be a valid IPv6 address but got '%s'", name, v)
		}

	default:
		return fmt.Errorf("field %s: unknown well-known validation rule: %T", name, r.WellKnown)
	}

	return nil
}

type WellKnownStrings string

const (
	WellKnownEmailString    WellKnownStrings = "EMAIL"
	WellKnownHostnameString WellKnownStrings = "HOSTNAME"
	WellKnownIPString       WellKnownStrings = "IP"
	WellKnownIpv4String     WellKnownStrings = "IPv4"
	WellKnownIpv6String     WellKnownStrings = "IPv6"
	WellKnownURIString      WellKnownStrings = "URI"
	WellKnownUUIDString     WellKnownStrings = "UUID"
)

func ValidateRepeatedStringRules(r *v1_conf.RepeatedStringRules, v []string, name string) error {
	if r == nil {
		return nil
	}
	if r.IsRequired && len(v) == 0 {
		return fmt.Errorf("field %s of type []string is marked as required but it has a zero-value", name)
	}

	if !r.ValidateEmpty && len(v) == 0 {
		return nil
	}

	if r.MinItems != nil && uint64(len(v)) < *r.MinItems {
		return fmt.Errorf("field %s: value must have at least %d items but got %d", name, *r.MinItems, len(v))
	}
	if r.MaxItems != nil && uint64(len(v)) > *r.MaxItems {
		return fmt.Errorf("field %s: value must have at most %d items but got %d", name, *r.MaxItems, len(v))
	}
	if r.Unique {
		uniqueValues := make(map[string]struct{})
		for _, item := range v {
			if _, exists := uniqueValues[item]; exists {
				return fmt.Errorf("field %s: value must not contain duplicate items but got multiple \"%s\"", name, item)
			}
			uniqueValues[item] = struct{}{}
		}
	}
	if r.ItemRules == nil {
		return nil
	}

	for i, item := range v {
		if err := ValidateStringRules(r.ItemRules, item, strconv.Itoa(i)); err != nil {
			return fmt.Errorf("field %s invalid item at %w", name, err)
		}
	}
	return nil
}

func (e *ErrConfigurationMissingFields) Error() string {
	var messages []string

	for _, err := range e.errors {
		messages = append(messages, err.Error())
	}

	return fmt.Sprintf("errors found:\n%s", strings.Join(messages, "\n"))
}

func (e *ErrConfigurationMissingFields) Push(err error) {
	e.errors = append(e.errors, err)
}

type Configurable interface {
	GetString(key string) string
	GetBool(key string) bool
	GetInt(key string) int
	GetStringSlice(key string) []string
}

// Validate perform validation of field requirement and constraints
// relationships after the configuration is read.
// We don't check the following:
//   - if sets of fields are mutually exclusive and required
//     together at the same time
func Validate(c Configuration, v Configurable) error {
	present := make(map[string]int)
	validationErrors := &ErrConfigurationMissingFields{}

	for _, f := range c.Fields {
		// Note: the viper methods are actually casting
		//   internal strings into the desired type.
		var isPresent bool
		var validationError error
		switch f.Variant {
		case StringVariant:
			isPresent, validationError = ValidateField(&f, v.GetString(f.FieldName))
		case BoolVariant:
			isPresent, validationError = ValidateField(&f, v.GetBool(f.FieldName))
		case IntVariant:
			isPresent, validationError = ValidateField(&f, v.GetInt(f.FieldName))
		case StringSliceVariant:
			isPresent, validationError = ValidateField(&f, v.GetStringSlice(f.FieldName))
		default:
			return fmt.Errorf("unknown field type %s", f.Variant)
		}
		if validationError != nil {
			validationErrors.Push(validationError)
		}

		if isPresent {
			present[f.FieldName] = 1
		}
	}

	if len(validationErrors.errors) > 0 {
		return validationErrors
	}

	return validateConstraints(present, c.Constraints)
}

func validateConstraints(fieldsPresent map[string]int, relationships []SchemaFieldRelationship) error {
	for _, relationship := range relationships {
		var present int
		for _, f := range relationship.Fields {
			present += fieldsPresent[f.FieldName]
		}

		var expected int
		for _, e := range relationship.ExpectedFields {
			expected += fieldsPresent[e.FieldName]
		}

		switch relationship.Kind {
		case MutuallyExclusive:
			if present > 1 {
				return makeMutuallyExclusiveError(fieldsPresent, relationship)
			}
		case RequiredTogether:
			if present > 0 && present < len(relationship.Fields) {
				return makeNeededTogetherError(fieldsPresent, relationship)
			}
		case AtLeastOne:
			if present == 0 {
				return makeAtLeastOneError(fieldsPresent, relationship)
			}
		case Dependents:
			if present > 0 && expected != len(relationship.ExpectedFields) {
				return makeDependentFieldsError(fieldsPresent, relationship)
			}
		default:
			return fmt.Errorf("invalid relationship constraint")
		}
	}

	return nil
}

func nice(elements []string) string {
	return ustrings.OxfordizeList(
		elements,
		ustrings.WithInnerWrappers(ustrings.SingleQuotes),
		ustrings.WithOuterWrappers(ustrings.Parentheses),
	)
}

func makeMutuallyExclusiveError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 1 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"fields marked as mutually exclusive were set: %s",
		nice(found),
	)
}

func makeNeededTogetherError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 0 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"fields marked as needed together are missing: %s",
		nice(found),
	)
}

func makeAtLeastOneError(fields map[string]int, relation SchemaFieldRelationship) error {
	var found []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 0 {
			found = append(found, f.FieldName)
		}
	}

	return fmt.Errorf(
		"at least one field was expected, any of: %s",
		nice(found),
	)
}

func makeDependentFieldsError(fields map[string]int, relation SchemaFieldRelationship) error {
	var notfoundExpected []string
	for _, n := range relation.ExpectedFields {
		if fields[n.FieldName] == 0 {
			notfoundExpected = append(notfoundExpected, n.FieldName)
		}
	}

	var foundDependent []string
	for _, f := range relation.Fields {
		if fields[f.FieldName] == 1 {
			foundDependent = append(foundDependent, f.FieldName)
		}
	}

	return fmt.Errorf(
		"set fields %s are dependent on %s being set",
		nice(foundDependent),
		nice(notfoundExpected),
	)
}
