// Package duty holds the builders connectors use to declare which duties an
// entitlement allows. Attach the mappings with entitlement.WithDuties.
//
// Duty IDs are plain strings defined by ConductorOne's duty catalog, named
// <object>.<action> in lowercase, such as "payment.release". A list of duty
// names will be added to this package in a later release once they are
// settled; until then pass the ID as a string.
//
// Exact and Inclusive return the wire message and take an optional
// description:
//
//	entitlement.WithDuties(
//		duty.Exact("payment.release"),
//		duty.Inclusive("payment.authorize", "Finance Administrator role"),
//	)
package duty

import (
	"errors"
	"fmt"
	"regexp"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Exact returns a mapping for an entitlement that exists to grant this one
// duty. The optional description is a plain-English note shown to people
// and is never matched on; if more than one is passed, the first is used,
// and with none the description is empty. It accepts any string type so
// typed duty-name constants can be added in a later release without an API
// change.
func Exact[T ~string](id T, description ...string) *v2.DutyMapping {
	return mapping(string(id), v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT, description)
}

// Inclusive returns a mapping for a broad entitlement that allows this duty
// among others. The optional description is a plain-English note shown to
// people and is never matched on; if more than one is passed, the first is
// used, and with none the description is empty. It accepts any string type
// so typed duty-name constants can be added in a later release without an
// API change.
func Inclusive[T ~string](id T, description ...string) *v2.DutyMapping {
	return mapping(string(id), v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, description)
}

func mapping(id string, precision v2.DutyMappingPrecision, description []string) *v2.DutyMapping {
	var d string
	if len(description) > 0 {
		d = description[0]
	}
	return v2.DutyMapping_builder{
		DutyId:      id,
		Precision:   precision,
		Description: d,
	}.Build()
}

// namingConvention is the <object>.<action> shape of duty IDs.
var namingConvention = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)

// maxIDLen matches the max_len rule on DutyMapping.duty_id.
const maxIDLen = 64

// validID reports whether id follows the duty ID naming convention: 1 to 64
// characters, <object>.<action>, lowercase letters, digits and '_', each
// part starting with a letter, and exactly one '.'.
func validID(id string) bool {
	return len(id) >= 1 && len(id) <= maxIDLen && namingConvention.MatchString(id)
}

// Validate checks the annotation's wire rules, rejects a duty ID listed
// twice, and requires every duty ID to follow the <object>.<action> naming
// convention: 1 to 64 characters, lowercase letters, digits and '_', each
// part starting with a letter, and exactly one '.'.
func Validate(a *v2.DutyMappingsAnnotation) error {
	if err := a.ValidateAll(); err != nil {
		return err
	}
	var errs []error
	seen := make(map[string]int, len(a.GetMappings()))
	for i, m := range a.GetMappings() {
		id := m.GetDutyId()
		if first, dup := seen[id]; dup {
			errs = append(errs, fmt.Errorf("mapping %d: duty %q is already mapped at index %d", i, id, first))
		} else {
			seen[id] = i
		}
		if !validID(id) {
			errs = append(errs, fmt.Errorf("mapping %d: duty %q does not follow the <object>.<action> naming convention", i, id))
		}
	}
	return errors.Join(errs...)
}
