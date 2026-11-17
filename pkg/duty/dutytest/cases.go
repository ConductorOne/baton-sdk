package dutytest

import (
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// Case is one DutyMappingsAnnotation wire-contract case. Valid reports
// whether the annotation passes duty.Validate. Some invalid cases, named
// "passes the wire rule", fail only the naming convention that
// duty.Validate checks; the generated ValidateAll accepts them.
type Case struct {
	Name       string
	Annotation *v2.DutyMappingsAnnotation
	Valid      bool
}

// Cases returns the wire-contract cases for DutyMappingsAnnotation, so the
// SDK and the code that ingests the annotation test against the same
// inputs. Each call returns fresh messages that the caller may modify.
//
// Valid cases include precision values this SDK version does not define.
// Consumers should keep mappings they do not recognize rather than drop the
// annotation.
func Cases() []Case {
	mapping := func(id string, precision v2.DutyMappingPrecision, description string) *v2.DutyMapping {
		return v2.DutyMapping_builder{DutyId: id, Precision: precision, Description: description}.Build()
	}
	annotation := func(mappings ...*v2.DutyMapping) *v2.DutyMappingsAnnotation {
		return v2.DutyMappingsAnnotation_builder{Mappings: mappings}.Build()
	}
	inclusiveN := func(n int) *v2.DutyMappingsAnnotation {
		list := make([]*v2.DutyMapping, 0, n)
		for i := range n {
			list = append(list, mapping(fmt.Sprintf("duty.action_%d", i), v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE, ""))
		}
		return annotation(list...)
	}
	const (
		exact       = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT
		inclusive   = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE
		unspecified = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_UNSPECIFIED
		release     = "payment.release"
	)
	return []Case{
		{
			Name:       "single exact",
			Annotation: annotation(mapping(release, exact, "")),
			Valid:      true,
		},
		{
			Name: "mixed exact and inclusive with a description",
			Annotation: annotation(
				mapping("payment.authorize", inclusive, "Finance Administrator role"),
				mapping("vendor.activate", inclusive, ""),
				mapping(release, exact, ""),
			),
			Valid: true,
		},
		{
			Name:       "16 mappings",
			Annotation: inclusiveN(16),
			Valid:      true,
		},
		{
			Name:       "undefined precision value",
			Annotation: annotation(mapping(release, v2.DutyMappingPrecision(99), "")),
			Valid:      true,
		},
		{
			Name:       "17 mappings",
			Annotation: inclusiveN(17),
			Valid:      false,
		},
		{
			Name:       "unspecified precision",
			Annotation: annotation(mapping(release, unspecified, "")),
			Valid:      false,
		},
		{
			Name:       "empty mappings",
			Annotation: annotation(),
			Valid:      false,
		},
		{
			Name:       "duplicate duty ID",
			Annotation: annotation(mapping(release, exact, ""), mapping(release, inclusive, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID with three segments passes the wire rule",
			Annotation: annotation(mapping("vendor.payment_instructions.modify", exact, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID with a hyphen passes the wire rule",
			Annotation: annotation(mapping("payment-run.release", exact, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID with uppercase letters passes the wire rule",
			Annotation: annotation(mapping("Payment.Release", exact, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID too long",
			Annotation: annotation(mapping("a."+strings.Repeat("b", 63), exact, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID with a disallowed character",
			Annotation: annotation(mapping("payment/release", exact, "")),
			Valid:      false,
		},
	}
}
