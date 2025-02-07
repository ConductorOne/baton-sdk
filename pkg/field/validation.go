package field

import (
	"fmt"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/conductorone/baton-sdk/pkg/ustrings"
	"github.com/spf13/viper"
)

type ErrConfigurationMissingFields struct {
	errors []error
}

type IntRules struct {
	Required *bool   `json:"Required,omitempty"`
	Eq       *int64  `json:"Eq,omitempty"`
	Lt       *int64  `json:"Lt,omitempty"`
	Lte      *int64  `json:"Lte,omitempty"`
	Gt       *int64  `json:"Gt,omitempty"`
	Gte      *int64  `json:"Gte,omitempty"`
	In       []int64 `json:"In,omitempty"`
	NotIn    []int64 `json:"NotIn,omitempty"`
}

func (r *IntRules) Validate(v int64) error {
	if r == nil {
		return nil
	}
	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("expected %v but got %v", *r.Eq, v)
	}
	if r.Lt != nil && v >= *r.Lt {
		return fmt.Errorf("value must be less than %d but got %d", *r.Lt, v)
	}
	if r.Lte != nil && v > *r.Lte {
		return fmt.Errorf("value must be less than or equal to %d but got %d", *r.Lte, v)
	}
	if r.Gt != nil && v <= *r.Gt {
		return fmt.Errorf("value must be greater than %d but got %d", *r.Gt, v)
	}
	if r.Gte != nil && v < *r.Gte {
		return fmt.Errorf("value must be greater than or equal to %d but got %d", *r.Gte, v)
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
			return fmt.Errorf("value must be one of %v but got %d", r.In, v)
		}
	}
	if r.NotIn != nil {
		for _, val := range r.NotIn {
			if v == val {
				return fmt.Errorf("value must not be one of %v but got %d", r.NotIn, v)
			}
		}
	}
	return nil
}

type UintRules struct {
	Required *bool    `json:"Required,omitempty"`
	Eq       *uint64  `json:"Eq,omitempty"`
	Lt       *uint64  `json:"Lt,omitempty"`
	Lte      *uint64  `json:"Lte,omitempty"`
	Gt       *uint64  `json:"Gt,omitempty"`
	Gte      *uint64  `json:"Gte,omitempty"`
	In       []uint64 `json:"In,omitempty"`
	NotIn    []uint64 `json:"NotIn,omitempty"`
}

func (r *UintRules) Validate(v uint64) error {
	if r == nil {
		return nil
	}
	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("expected %v but got %v", *r.Eq, v)
	}
	if r.Lt != nil && v >= *r.Lt {
		return fmt.Errorf("value must be less than %d but got %d", *r.Lt, v)
	}
	if r.Lte != nil && v > *r.Lte {
		return fmt.Errorf("value must be less than or equal to %d but got %d", *r.Lte, v)
	}
	if r.Gt != nil && v <= *r.Gt {
		return fmt.Errorf("value must be greater than %d but got %d", *r.Gt, v)
	}
	if r.Gte != nil && v < *r.Gte {
		return fmt.Errorf("value must be greater than or equal to %d but got %d", *r.Gte, v)
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
			return fmt.Errorf("value must be one of %v but got %d", r.In, v)
		}
	}
	if r.NotIn != nil {
		for _, val := range r.NotIn {
			if v == val {
				return fmt.Errorf("value must not be one of %v but got %d", r.NotIn, v)
			}
		}
	}
	return nil
}

type BoolRules struct {
	Eq *bool `json:"Eq,omitempty"`
}

func (r *BoolRules) Validate(v bool) error {
	if r == nil {
		return nil
	}
	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("expected %v but got %v", *r.Eq, v)
	}
	return nil
}

type StringRules struct {
	Required    *bool             `json:"Required,omitempty"`
	Eq          *string           `json:"Eq,omitempty"`
	Len         *uint64           `json:"Len,omitempty"`
	MinLen      *uint64           `json:"MinLen,omitempty"`
	MaxLen      *uint64           `json:"MaxLen,omitempty"`
	Pattern     *string           `json:"Pattern,omitempty"`
	Prefix      *string           `json:"Prefix,omitempty"`
	Suffix      *string           `json:"Suffix,omitempty"`
	Contains    *string           `json:"Contains,omitempty"`
	NotContains *string           `json:"NotContains,omitempty"`
	In          []string          `json:"In,omitempty"`
	NotIn       []string          `json:"NotIn,omitempty"`
	WellKnown   *WellKnownStrings `json:"WellKnown,omitempty"`
	IgnoreEmpty bool              `json:"IgnoreEmpty,omitempty"`
}

func (r *StringRules) Validate(v string) error {
	if r == nil {
		return nil
	}

	if r.IgnoreEmpty && v == "" {
		return nil
	}

	if r.Eq != nil && *r.Eq != v {
		return fmt.Errorf("expected %v but got %v", *r.Eq, v)
	}
	if r.Len != nil && uint64(len(v)) != *r.Len {
		return fmt.Errorf("value must be exactly %d characters long but got %d", *r.Len, len(v))
	}
	if r.MinLen != nil && uint64(len(v)) < *r.MinLen {
		return fmt.Errorf("value must be at least %d characters long but got %d", *r.MinLen, len(v))
	}
	if r.MaxLen != nil && uint64(len(v)) > *r.MaxLen {
		return fmt.Errorf("value must be at most %d characters long but got %d", *r.MaxLen, len(v))
	}
	if r.Pattern != nil {
		pattern, err := regexp.CompilePOSIX(*r.Pattern)
		if err != nil {
			return fmt.Errorf("invalid pattern: %w", err)
		}
		if !pattern.MatchString(v) {
			return fmt.Errorf("value must match pattern %s but got %s", pattern.String(), v)
		}
	}
	if r.Prefix != nil && !strings.HasPrefix(v, *r.Prefix) {
		return fmt.Errorf("value must have prefix %s but got %s", *r.Prefix, v)
	}
	if r.Suffix != nil && !strings.HasSuffix(v, *r.Suffix) {
		return fmt.Errorf("value must have suffix %s but got %s", *r.Suffix, v)
	}
	if r.Contains != nil && !strings.Contains(v, *r.Contains) {
		return fmt.Errorf("value must contain %s but got %s", *r.Contains, v)
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
			return fmt.Errorf("value must be one of %v but got %s", r.In, v)
		}
	}
	if r.NotIn != nil {
		for _, val := range r.NotIn {
			if v == val {
				return fmt.Errorf("value must not be one of %v but got %s", r.NotIn, v)
			}
		}
	}
	if r.WellKnown == nil {
		return nil
	}

	switch *r.WellKnown {
	case WellKnownEmailString:
		const emailRegex = `^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$`
		re := regexp.MustCompile(emailRegex)
		if !re.MatchString(v) {
			return fmt.Errorf("value must be a valid email address but got %s", v)
		}

	case WellKnownURIString:
		_, err := url.ParseRequestURI(v)
		if err != nil {
			return fmt.Errorf("value must be a valid URL but got %s", v)
		}
	case WellKnownUUIDString:
		const uuidRegex = `^[a-fA-F0-9]{8}\-[a-fA-F0-9]{4}\-[a-fA-F0-9]{4}\-[a-fA-F0-9]{4}\-[a-fA-F0-9]{12}$`
		re := regexp.MustCompile(uuidRegex)
		if !re.MatchString(v) {
			return fmt.Errorf("value must be a valid UUID but got %s", v)
		}
	case WellKnownHostnameString:
		const hostnameRegex = `^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\-]*[A-Za-z0-9])$`
		re := regexp.MustCompile(hostnameRegex)
		if !re.MatchString(v) {
			return fmt.Errorf("value must be a valid hostname but got %s", v)
		}
	case WellKnownIPString:
		if net.ParseIP(v) == nil {
			return fmt.Errorf("value must be a valid IP address but got %s", v)
		}
	case WellKnownIpv4String:
		const ipv4Regex = `^(\d{1,3}\.){3}\d{1,3}$`
		re := regexp.MustCompile(ipv4Regex)
		if !re.MatchString(v) {
			return fmt.Errorf("value must be a valid IPv4 address but got %s", v)
		}
	case WellKnownIpv6String:
		const ipv6Regex = `^([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$`
		re := regexp.MustCompile(ipv6Regex)
		if !re.MatchString(v) {
			return fmt.Errorf("value must be a valid IPv6 address but got %s", v)
		}

	default:
		return fmt.Errorf("unknown well-known validation rule: %T", r.WellKnown)
	}

	return nil
}

type EnumRules struct {
	Required *bool   `json:"Required,omitempty"`
	Eq       *int32  `json:"Eq,omitempty"`
	In       []int32 `json:"In,omitempty"`
	NotIn    []int32 `json:"NotIn,omitempty"`
}

type RepeatedRules[T StringRules] struct {
	Required    *bool        `json:"Required,omitempty"`
	MinItems    *uint64      `json:"MinItems,omitempty"`
	MaxItems    *uint64      `json:"MaxItems,omitempty"`
	UniqueItems *bool        `json:"UniqueItems,omitempty"`
	Items       *StringRules `json:"Items,omitempty"`
	IgnoreEmpty bool         `json:"IgnoreEmpty,omitempty"`
}

type WellKnownStrings string

const (
	WellKnownEmailString = "EMAIL"
	// Hostname specifies that the field must be a valid hostname as
	// defined by RFC 1034. This constraint does not support
	// internationalized domain names (IDNs).
	WellKnownHostnameString = "HOSTNAME"
	// Ip specifies that the field must be a valid IP (v4 or v6) address.
	// Valid IPv6 addresses should not include surrounding square brackets.
	WellKnownIPString = "IP"
	// Ipv4 specifies that the field must be a valid IPv4 address.
	WellKnownIpv4String = "IPv4"
	// Ipv6 specifies that the field must be a valid IPv6 address. Valid
	// IPv6 addresses should not include surrounding square brackets.
	WellKnownIpv6String = "IPv6"
	// Uri specifies that the field must be a valid, absolute URI as defined
	// by RFC 3986
	WellKnownURIString = "URI"
	// // UriRef specifies that the field must be a valid URI as defined by RFC
	// // 3986 and may be relative or absolute.
	// WellKnownURI_refString = 7
	// Address specifies that the field must be either a valid hostname as
	// defined by RFC 1034 (which does not support internationalized domain
	// names or IDNs), or it can be a valid IP (v4 or v6).
	// WellKnownAddressString = 8
	// Uuid specifies that the field must be a valid UUID as defined by
	// RFC 4122
	WellKnownUUIDString = "UUID"
)

func (r *RepeatedRules[StringRules]) Validate(v []string) error {
	if r == nil {
		return nil
	}
	if r.IgnoreEmpty && len(v) == 0 {
		return nil
	}

	if r.MinItems != nil && uint64(len(v)) < *r.MinItems {
		return fmt.Errorf("value must have at least %d items but got %d", *r.MinItems, len(v))
	}
	if r.MaxItems != nil && uint64(len(v)) > *r.MaxItems {
		return fmt.Errorf("value must have at most %d items but got %d", *r.MaxItems, len(v))
	}
	if r.UniqueItems != nil {
		uniqueValues := make(map[string]struct{})
		for _, item := range v {
			if _, exists := uniqueValues[item]; exists {
				return fmt.Errorf("value must not contain duplicate items but got %s", item)
			}
			uniqueValues[item] = struct{}{}
		}
	}
	if r.Items == nil {
		return nil
	}
	for _, item := range v {
		if err := r.Items.Validate(item); err != nil {
			return fmt.Errorf("invalid item in repeated field: %w", err)
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

// Validate perform validation of field requirement and constraints
// relationships after the configuration is read.
// We don't check the following:
//   - if sets of fields are mutually exclusive and required
//     together at the same time
func Validate(c Configuration, v *viper.Viper) error {
	present := make(map[string]int)
	validationErrors := &ErrConfigurationMissingFields{}

	// check if required fields are present
	for _, f := range c.Fields {
		var isValid error
		switch f.Variant {
		case BoolVariant:
			isValid = f.Validate(v.GetBool(f.FieldName))
		case IntVariant:
			isValid = f.Validate(v.GetInt64(f.FieldName))
		case UintVariant:
			isValid = f.Validate(v.GetUint64(f.FieldName))
		case StringVariant:
			isValid = f.Validate(v.GetString(f.FieldName))
		case StringSliceVariant:
			isValid = f.Validate(v.GetStringSlice(f.FieldName))
		default:
			return fmt.Errorf("field %s has unsupported type %s", f.FieldName, f.FieldType)
		}
		if isValid != nil {
			validationErrors.Push(isValid)
		}
	}

	if len(validationErrors.errors) > 0 {
		return validationErrors
	}

	// check constraints
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
