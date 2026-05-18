package anonymize

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"google.golang.org/protobuf/types/known/anypb"
)

// AnonymizeResource anonymizes a Resource in place.
func (a *Anonymizer) AnonymizeResource(r *v2.Resource) error {
	if r == nil {
		return nil
	}

	// Anonymize display name
	if r.GetDisplayName() != "" {
		r.SetDisplayName(a.hasher.AnonymizeDisplayName(r.GetDisplayName()))
	}

	// Anonymize description
	if r.GetDescription() != "" {
		r.SetDescription("[ANONYMIZED]")
	}

	// Anonymize resource ID
	if r.HasId() {
		a.anonymizeResourceID(r.GetId())
	}

	// Anonymize parent resource ID
	if r.HasParentResourceId() {
		a.anonymizeResourceID(r.GetParentResourceId())
	}

	// Anonymize external ID
	if r.HasExternalId() {
		a.anonymizeExternalID(r.GetExternalId())
	}

	// Anonymize trait annotations
	if err := a.anonymizeResourceAnnotations(r); err != nil {
		return err
	}

	return nil
}

// anonymizeResourceID anonymizes a ResourceId in place.
func (a *Anonymizer) anonymizeResourceID(rid *v2.ResourceId) {
	if rid == nil {
		return
	}
	if rid.GetResource() != "" {
		rid.SetResource(a.hasher.AnonymizeResourceID(rid.GetResource()))
	}
	// Anonymize resource type - uses same hashing as ResourceType.Id for consistency
	if rid.GetResourceType() != "" {
		rid.SetResourceType(a.hasher.AnonymizeResourceType(rid.GetResourceType()))
	}
}

// anonymizeExternalID anonymizes an ExternalId in place.
func (a *Anonymizer) anonymizeExternalID(eid *v2.ExternalId) {
	if eid == nil {
		return
	}
	if eid.GetId() != "" {
		eid.SetId(a.hasher.AnonymizeExternalID(eid.GetId()))
	}
	if eid.GetLink() != "" {
		eid.SetLink(a.hasher.AnonymizeURL(eid.GetLink()))
	}
	if eid.GetDescription() != "" {
		eid.SetDescription("[ANONYMIZED]")
	}
}

// anonymizeResourceAnnotations processes all annotations on a Resource,
// anonymizing known types and dropping unknown types.
func (a *Anonymizer) anonymizeResourceAnnotations(r *v2.Resource) error {
	var result []*anypb.Any
	for _, ann := range r.GetAnnotations() {
		if processed, err := a.processResourceAnnotation(ann); err != nil {
			return err
		} else if processed != nil {
			result = append(result, processed)
		}
	}

	r.SetAnnotations(result)
	return nil
}

// processResourceAnnotation processes a single annotation from a Resource.
// Returns nil if the annotation should be dropped.
func (a *Anonymizer) processResourceAnnotation(ann *anypb.Any) (*anypb.Any, error) {
	// UserTrait
	ut := &v2.UserTrait{}
	if ann.MessageIs(ut) {
		if err := ann.UnmarshalTo(ut); err != nil {
			return nil, err
		}
		a.anonymizeUserTrait(ut)
		return anypb.New(ut)
	}

	// GroupTrait
	gt := &v2.GroupTrait{}
	if ann.MessageIs(gt) {
		if err := ann.UnmarshalTo(gt); err != nil {
			return nil, err
		}
		a.anonymizeGroupTrait(gt)
		return anypb.New(gt)
	}

	// RoleTrait
	rt := &v2.RoleTrait{}
	if ann.MessageIs(rt) {
		if err := ann.UnmarshalTo(rt); err != nil {
			return nil, err
		}
		a.anonymizeRoleTrait(rt)
		return anypb.New(rt)
	}

	// AppTrait
	at := &v2.AppTrait{}
	if ann.MessageIs(at) {
		if err := ann.UnmarshalTo(at); err != nil {
			return nil, err
		}
		a.anonymizeAppTrait(at)
		return anypb.New(at)
	}

	// SecretTrait
	st := &v2.SecretTrait{}
	if ann.MessageIs(st) {
		if err := ann.UnmarshalTo(st); err != nil {
			return nil, err
		}
		a.anonymizeSecretTrait(st)
		return anypb.New(st)
	}

	// ExternalLink - anonymize URL
	externalLink := &v2.ExternalLink{}
	if ann.MessageIs(externalLink) {
		if err := ann.UnmarshalTo(externalLink); err != nil {
			return nil, err
		}
		if externalLink.GetUrl() != "" {
			externalLink.SetUrl(a.hasher.AnonymizeURL(externalLink.GetUrl()))
		}
		return anypb.New(externalLink)
	}

	// Unknown type - drop it
	return nil, nil
}

// anonymizeUserTrait anonymizes a UserTrait in place.
func (a *Anonymizer) anonymizeUserTrait(ut *v2.UserTrait) {
	if ut == nil {
		return
	}

	// Anonymize emails
	for _, email := range ut.GetEmails() {
		if email.GetAddress() != "" {
			email.SetAddress(a.hasher.AnonymizeEmail(email.GetAddress()))
		}
	}

	// Anonymize login
	if ut.GetLogin() != "" {
		ut.SetLogin(a.hasher.AnonymizeLogin(ut.GetLogin()))
	}

	// Anonymize login aliases
	aliases := ut.GetLoginAliases()
	for i, alias := range aliases {
		if alias != "" {
			aliases[i] = a.hasher.AnonymizeLogin(alias)
		}
	}
	ut.SetLoginAliases(aliases)

	// Anonymize employee IDs
	empIDs := ut.GetEmployeeIds()
	for i, empID := range empIDs {
		if empID != "" {
			empIDs[i] = a.hasher.AnonymizeEmployeeID(empID)
		}
	}
	ut.SetEmployeeIds(empIDs)

	// Anonymize structured name
	if ut.HasStructuredName() {
		sn := ut.GetStructuredName()
		if sn.GetGivenName() != "" {
			sn.SetGivenName(a.hasher.AnonymizeGivenName(sn.GetGivenName()))
		}
		if sn.GetFamilyName() != "" {
			sn.SetFamilyName(a.hasher.AnonymizeFamilyName(sn.GetFamilyName()))
		}
		middleNames := sn.GetMiddleNames()
		for i, mn := range middleNames {
			if mn != "" {
				middleNames[i] = a.hasher.AnonymizeMiddleName(mn)
			}
		}
		sn.SetMiddleNames(middleNames)
		// Clear prefix/suffix - can be identifying when combined with other data
		sn.SetPrefix("")
		sn.SetSuffix("")
	}

	// Clear profile (may contain arbitrary PII)
	ut.ClearProfile()

	// Clear icon (profile pictures are identifying)
	ut.ClearIcon()

	// Anonymize status details (may contain identifying information)
	if ut.HasStatus() && ut.GetStatus().GetDetails() != "" {
		ut.GetStatus().SetDetails("[ANONYMIZED]")
	}

	// Set timestamps to single anonymized timestamp to avoid fingerprinting
	ut.SetCreatedAt(a.AnonymizedTimestamp())
	ut.SetLastLogin(a.AnonymizedTimestamp())

	// Note: Status enum, AccountType, MfaStatus, SsoStatus are kept
	// as they don't typically contain PII and are useful for analysis
}

// anonymizeGroupTrait anonymizes a GroupTrait in place.
func (a *Anonymizer) anonymizeGroupTrait(gt *v2.GroupTrait) {
	if gt == nil {
		return
	}

	// Clear profile (may contain arbitrary PII)
	gt.ClearProfile()

	// Clear icon (profile pictures are identifying)
	gt.ClearIcon()
}

// anonymizeRoleTrait anonymizes a RoleTrait in place.
func (a *Anonymizer) anonymizeRoleTrait(rt *v2.RoleTrait) {
	if rt == nil {
		return
	}

	// Clear profile (may contain arbitrary PII)
	rt.ClearProfile()
}

// anonymizeAppTrait anonymizes an AppTrait in place.
func (a *Anonymizer) anonymizeAppTrait(at *v2.AppTrait) {
	if at == nil {
		return
	}

	// Anonymize help URL
	if at.GetHelpUrl() != "" {
		at.SetHelpUrl(a.hasher.AnonymizeURL(at.GetHelpUrl()))
	}

	// Clear profile (may contain arbitrary PII)
	at.ClearProfile()

	// Clear icon and logo (these are identifying)
	at.ClearIcon()
	at.ClearLogo()

	// Note: Flags are kept as they don't contain PII
}

// anonymizeSecretTrait anonymizes a SecretTrait in place.
func (a *Anonymizer) anonymizeSecretTrait(st *v2.SecretTrait) {
	if st == nil {
		return
	}

	// Clear profile (may contain arbitrary PII)
	st.ClearProfile()

	// Anonymize CreatedById
	if st.HasCreatedById() {
		a.anonymizeResourceID(st.GetCreatedById())
	}

	// Anonymize IdentityId
	if st.HasIdentityId() {
		a.anonymizeResourceID(st.GetIdentityId())
	}

	// Set timestamps to single anonymized timestamp to avoid fingerprinting
	st.SetCreatedAt(a.AnonymizedTimestamp())
	st.SetExpiresAt(a.AnonymizedTimestamp())
	st.SetLastUsedAt(a.AnonymizedTimestamp())
}
