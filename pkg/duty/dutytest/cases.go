package dutytest

import (
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/duty"
)

// Case is one DutyMappingsAnnotation wire-contract case. Valid reports
// whether the annotation passes duty.Validate.
type Case struct {
	Name       string
	Annotation *v2.DutyMappingsAnnotation
	Valid      bool
}

// Cases returns the wire-contract cases for DutyMappingsAnnotation, so the
// SDK and the code that ingests the annotation test against the same
// inputs. Each call returns fresh messages that the caller may modify.
//
// Valid cases include duty IDs that are not constants in pkg/duty and
// precision values this SDK version does not define. Consumers should keep
// mappings they do not recognize rather than drop the annotation.
func Cases() []Case {
	mapping := func(id string, precision v2.DutyMappingPrecision, description string) *v2.DutyMapping {
		return v2.DutyMapping_builder{DutyId: id, Precision: precision, Description: description}.Build()
	}
	annotation := func(mappings ...*v2.DutyMapping) *v2.DutyMappingsAnnotation {
		return v2.DutyMappingsAnnotation_builder{Mappings: mappings}.Build()
	}
	const (
		exact       = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT
		inclusive   = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE
		conditional = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL
		unspecified = v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_UNSPECIFIED
	)
	release := string(duty.PaymentRelease)
	return []Case{
		{
			Name:       "single exact known ID",
			Annotation: annotation(mapping(release, exact, "")),
			Valid:      true,
		},
		{
			Name: "mixed precisions with a description",
			Annotation: annotation(
				mapping(string(duty.PaymentPrepare), inclusive, ""),
				mapping(string(duty.PaymentAuthorize), inclusive, ""),
				mapping(string(duty.SoftwareChangeApprove), conditional, "only when branch protection allows self-approval"),
				mapping(release, exact, ""),
			),
			Valid: true,
		},
		{
			Name:       "unknown ID",
			Annotation: annotation(mapping("acme_ledger.post", exact, "")),
			Valid:      true,
		},
		{
			Name:       "ID outside the naming convention",
			Annotation: annotation(mapping("vendor.payment_instructions.modify", inclusive, ""), mapping("Payment-Run.Release", exact, "")),
			Valid:      true,
		},
		{
			Name:       "undefined precision value",
			Annotation: annotation(mapping(release, v2.DutyMappingPrecision(99), "")),
			Valid:      true,
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
			Name:       "duty ID too long",
			Annotation: annotation(mapping(strings.Repeat("a", 65), exact, "")),
			Valid:      false,
		},
		{
			Name:       "duty ID with a disallowed character",
			Annotation: annotation(mapping("payment/release", exact, "")),
			Valid:      false,
		},
	}
}
