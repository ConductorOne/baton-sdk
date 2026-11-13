// Package duty holds duty IDs and the builders connectors use to declare
// which duties an entitlement allows. Attach the mappings with
// entitlement.WithDuties.
//
// The constants cover the duties in the ConductorOne duty catalog. Duty IDs
// are open strings: any value that satisfies the wire rules (letters,
// digits, '_', '.', '-'; 1 to 64 characters) is accepted, so a connector can
// try a new duty without waiting for an SDK upgrade. Use Lint, or
// dutytest.AssertEntitlement in tests, to catch typos offline.
//
// A duty ID is permanent. It is never reused or given a different meaning.
// A duty whose meaning changes gets a new ID. A retired duty keeps its
// constant, marked "// Deprecated:", and stays in IDs.
package duty

import (
	"errors"
	"fmt"
	"regexp"
	"slices"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// ID is a duty identifier. Any string satisfying the wire rules is a valid
// ID; the constants below name the duties in the catalog this SDK was built
// against. Each constant's value is permanent: it is never removed, renamed,
// or given a different meaning.
type ID string

// Finance duties.
const (
	VendorCreateDraft               ID = "vendor.create_draft"
	VendorActivate                  ID = "vendor.activate"
	VendorPaymentInstructionsChange ID = "vendor_payment_instructions.change"
	VendorPaymentInstructionsVerify ID = "vendor_payment_instructions.verify"
	PaymentPrepare                  ID = "payment.prepare"
	PaymentAuthorize                ID = "payment.authorize"
	PaymentRelease                  ID = "payment.release"
	JournalEntryPrepare             ID = "journal_entry.prepare"
	JournalEntryApprove             ID = "journal_entry.approve"
	PurchaseOrderCreate             ID = "purchase_order.create"
	PurchaseOrderApprove            ID = "purchase_order.approve"
	InvoiceRecord                   ID = "invoice.record"
	InvoiceApprove                  ID = "invoice.approve"
)

// Identity duties.
const (
	AccessRequestSubmit  ID = "access_request.submit"
	AccessRequestApprove ID = "access_request.approve"
	EntitlementAssign    ID = "entitlement.assign"
	AccessCertify        ID = "access.certify"
)

// Security duties.
const (
	AuditLogConfigure ID = "audit_log.configure"
)

// Software delivery duties.
const (
	SoftwareChangeAuthor    ID = "software_change.author"
	SoftwareChangeApprove   ID = "software_change.approve"
	SoftwareChangeMerge     ID = "software_change.merge"
	ReleaseDeploy           ID = "release.deploy"
	ChangeControlBypass     ID = "change_control.bypass"
	ChangeControlModify     ID = "change_control.modify"
	DeploymentControlModify ID = "deployment_control.modify"
)

var knownIDs = func() []ID {
	ids := []ID{
		VendorCreateDraft,
		VendorActivate,
		VendorPaymentInstructionsChange,
		VendorPaymentInstructionsVerify,
		PaymentPrepare,
		PaymentAuthorize,
		PaymentRelease,
		JournalEntryPrepare,
		JournalEntryApprove,
		PurchaseOrderCreate,
		PurchaseOrderApprove,
		InvoiceRecord,
		InvoiceApprove,
		AccessRequestSubmit,
		AccessRequestApprove,
		EntitlementAssign,
		AccessCertify,
		AuditLogConfigure,
		SoftwareChangeAuthor,
		SoftwareChangeApprove,
		SoftwareChangeMerge,
		ReleaseDeploy,
		ChangeControlBypass,
		ChangeControlModify,
		DeploymentControlModify,
	}
	slices.Sort(ids)
	return ids
}()

// IDs returns every duty ID constant in this package, sorted, including
// deprecated ones.
func IDs() []ID {
	return slices.Clone(knownIDs)
}

// IsKnown reports whether id is one of the constants in this package.
func IsKnown[T ~string](id T) bool {
	_, found := slices.BinarySearch(knownIDs, ID(id))
	return found
}

// Mapping is one duty an entitlement allows. Build it with Exact,
// Inclusive, or Conditional.
type Mapping struct {
	dutyID      string
	precision   v2.DutyMappingPrecision
	description string
}

// Exact maps an entitlement that grants exactly this duty.
func Exact[T ~string](id T) Mapping {
	return Mapping{dutyID: string(id), precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_EXACT}
}

// Inclusive maps a broad entitlement that allows this duty among others.
func Inclusive[T ~string](id T) Mapping {
	return Mapping{dutyID: string(id), precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_INCLUSIVE}
}

// Conditional maps a duty that applies only when app settings the
// connector does not check allow it. condition states those settings.
func Conditional[T ~string](id T, condition string) Mapping {
	return Mapping{dutyID: string(id), precision: v2.DutyMappingPrecision_DUTY_MAPPING_PRECISION_CONDITIONAL, description: condition}
}

// WithDescription replaces the mapping's description.
func (m Mapping) WithDescription(text string) Mapping {
	m.description = text
	return m
}

// ID returns the mapping's duty ID.
func (m Mapping) ID() ID {
	return ID(m.dutyID)
}

// Proto returns the mapping as its wire message.
func (m Mapping) Proto() *v2.DutyMapping {
	return v2.DutyMapping_builder{
		DutyId:      m.dutyID,
		Precision:   m.precision,
		Description: m.description,
	}.Build()
}

// Validate checks the annotation's wire rules and rejects a duty ID listed
// twice. It accepts duty IDs that are not constants in this package; use
// Lint to flag those.
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

// namingConvention is the <object>.<action> shape catalog duty IDs follow.
var namingConvention = regexp.MustCompile(`^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$`)

// LintOption configures Lint.
type LintOption func(*lintConfig)

type lintConfig struct {
	allowed map[string]struct{}
}

// AllowUnknown accepts the given duty IDs even though they are not
// constants in this package and may not follow the naming convention.
func AllowUnknown(ids ...string) LintOption {
	return func(c *lintConfig) {
		for _, id := range ids {
			c.allowed[id] = struct{}{}
		}
	}
}

// Lint runs Validate and then reports, in mapping order, every duty ID that
// is neither a constant in this package nor allowed by AllowUnknown, and
// every duty ID not named <object>.<action> unless allowed. It is meant for
// connector tests; nothing on the sync path runs it.
func Lint(ann *v2.DutyMappingsAnnotation, opts ...LintOption) error {
	if err := Validate(ann); err != nil {
		return err
	}
	cfg := &lintConfig{allowed: map[string]struct{}{}}
	for _, opt := range opts {
		opt(cfg)
	}
	var errs []error
	for _, m := range ann.GetMappings() {
		id := m.GetDutyId()
		if _, ok := cfg.allowed[id]; ok {
			continue
		}
		if !IsKnown(id) {
			errs = append(errs, fmt.Errorf("duty %q is not a known duty ID; check for a typo or use AllowUnknown", id))
		}
		if !namingConvention.MatchString(id) {
			errs = append(errs, fmt.Errorf("duty %q does not follow the <object>.<action> naming convention", id))
		}
	}
	return errors.Join(errs...)
}
