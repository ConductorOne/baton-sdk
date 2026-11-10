// Package duty holds the ConductorOne duty catalog IDs and the builders
// connectors use to declare which duties an entitlement allows. Attach the
// mappings with entitlement.WithDuties, which sets the annotation's catalog
// ID and version from CatalogID and CatalogVersion.
package duty

import (
	"errors"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const (
	CatalogID      = "c1.duties"
	CatalogVersion = "1"
)

const (
	VendorCreateDraft               = "vendor.create_draft"
	VendorActivate                  = "vendor.activate"
	VendorPaymentInstructionsChange = "vendor_payment_instructions.change"
	VendorPaymentInstructionsVerify = "vendor_payment_instructions.verify"
	PaymentPrepare                  = "payment.prepare"
	PaymentAuthorize                = "payment.authorize"
	PaymentRelease                  = "payment.release"
	JournalEntryPrepare             = "journal_entry.prepare"
	JournalEntryApprove             = "journal_entry.approve"
	SoftwareChangeAuthor            = "software_change.author"
	SoftwareChangeApprove           = "software_change.approve"
	ChangeControlBypass             = "change_control.bypass"
	ReleaseDeploy                   = "release.deploy"
)

// IDs returns every duty ID constant in this package.
func IDs() []string {
	return []string{
		VendorCreateDraft,
		VendorActivate,
		VendorPaymentInstructionsChange,
		VendorPaymentInstructionsVerify,
		PaymentPrepare,
		PaymentAuthorize,
		PaymentRelease,
		JournalEntryPrepare,
		JournalEntryApprove,
		SoftwareChangeAuthor,
		SoftwareChangeApprove,
		ChangeControlBypass,
		ReleaseDeploy,
	}
}

// Mapping is one duty an entitlement allows. Build it with Exact,
// Inclusive, or Conditional.
type Mapping struct {
	dutyID      string
	precision   v2.DutyMappingPrecision
	description string
}

// Exact maps an entitlement that grants exactly this duty.
func Exact(dutyID string) Mapping {
	return Mapping{dutyID: dutyID, precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT}
}

// Inclusive maps a broad entitlement that allows this duty among others.
func Inclusive(dutyID string) Mapping {
	return Mapping{dutyID: dutyID, precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE}
}

// Conditional maps a duty that applies only when app settings the
// connector does not check allow it. condition states those settings.
func Conditional(dutyID string, condition string) Mapping {
	return Mapping{dutyID: dutyID, precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL, description: condition}
}

// WithDescription replaces the mapping's description.
func (m Mapping) WithDescription(text string) Mapping {
	m.description = text
	return m
}

func (m Mapping) Proto() *v2.DutyMapping {
	return v2.DutyMapping_builder{
		DutyId:      m.dutyID,
		Precision:   m.precision,
		Description: m.description,
	}.Build()
}

// Validate checks the annotation's field rules and rejects a duty ID listed
// twice.
func Validate(a *v2.DutyMappingsAnnotation) error {
	if err := a.ValidateAll(); err != nil {
		return err
	}
	var errs []error
	seen := make(map[string]struct{}, len(a.GetMappings()))
	for _, m := range a.GetMappings() {
		if _, dup := seen[m.GetDutyId()]; dup {
			errs = append(errs, fmt.Errorf("duty %q is mapped more than once", m.GetDutyId()))
		}
		seen[m.GetDutyId()] = struct{}{}
	}
	return errors.Join(errs...)
}
