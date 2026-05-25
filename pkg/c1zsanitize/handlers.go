package c1zsanitize

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

// sanitizeAssetRef rewrites the asset id while registering the
// original id so copyAssets fetches and rewrites the payload.
func (s *sanitizer) sanitizeAssetRef(in *v2.AssetRef, refs *assetRefSet) *v2.AssetRef {
	if in == nil || in.GetId() == "" {
		return nil
	}
	refs.add(in.GetId())
	return v2.AssetRef_builder{Id: s.id(in.GetId())}.Build()
}

// sanitizeStruct recursively walks a google.protobuf.Struct,
// HMAC-ing string leaves. Keys are preserved (they're connector-
// schema field names, not tenant data). Numbers and booleans pass
// through — see investigation §3.3 for the rationale.
func (s *sanitizer) sanitizeStruct(in *structpb.Struct) *structpb.Struct {
	if in == nil {
		return nil
	}
	out := &structpb.Struct{Fields: make(map[string]*structpb.Value, len(in.GetFields()))}
	for k, v := range in.GetFields() {
		out.Fields[k] = s.sanitizeValue(v)
	}
	return out
}

func (s *sanitizer) sanitizeValue(v *structpb.Value) *structpb.Value {
	if v == nil {
		return nil
	}
	switch kind := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return structpb.NewStringValue(s.id(kind.StringValue))
	case *structpb.Value_StructValue:
		return structpb.NewStructValue(s.sanitizeStruct(kind.StructValue))
	case *structpb.Value_ListValue:
		lv := kind.ListValue
		items := make([]*structpb.Value, 0, len(lv.GetValues()))
		for _, item := range lv.GetValues() {
			items = append(items, s.sanitizeValue(item))
		}
		return structpb.NewListValue(&structpb.ListValue{Values: items})
	default:
		return v
	}
}

func handleUserTrait(s *sanitizer, msg proto.Message, refs *assetRefSet) proto.Message {
	in := msg.(*v2.UserTrait)
	emails := make([]*v2.UserTrait_Email, 0, len(in.GetEmails()))
	for _, e := range in.GetEmails() {
		emails = append(emails, v2.UserTrait_Email_builder{
			Address:   sanitizeEmail(s.secret, s.domains, e.GetAddress()),
			IsPrimary: e.GetIsPrimary(),
		}.Build())
	}
	loginAliases := make([]string, 0, len(in.GetLoginAliases()))
	for _, a := range in.GetLoginAliases() {
		loginAliases = append(loginAliases, s.id(a))
	}
	employeeIDs := make([]string, 0, len(in.GetEmployeeIds()))
	for _, eid := range in.GetEmployeeIds() {
		employeeIDs = append(employeeIDs, s.id(eid))
	}

	out := v2.UserTrait_builder{
		Emails:       emails,
		Profile:      s.sanitizeStruct(in.GetProfile()),
		Icon:         s.sanitizeAssetRef(in.GetIcon(), refs),
		AccountType:  in.GetAccountType(),
		Login:        s.id(in.GetLogin()),
		LoginAliases: loginAliases,
		EmployeeIds:  employeeIDs,
	}.Build()

	if in.HasStatus() {
		st := in.GetStatus()
		out.SetStatus(v2.UserTrait_Status_builder{
			Status:  st.GetStatus(),
			Details: s.id(st.GetDetails()),
		}.Build())
	}
	if in.HasCreatedAt() {
		out.SetCreatedAt(s.shifter.shift(in.GetCreatedAt()))
	}
	if in.HasLastLogin() {
		out.SetLastLogin(s.shifter.shift(in.GetLastLogin()))
	}
	if in.HasMfaStatus() {
		out.SetMfaStatus(v2.UserTrait_MFAStatus_builder{
			MfaEnabled: in.GetMfaStatus().GetMfaEnabled(),
		}.Build())
	}
	if in.HasSsoStatus() {
		out.SetSsoStatus(v2.UserTrait_SSOStatus_builder{
			SsoEnabled: in.GetSsoStatus().GetSsoEnabled(),
		}.Build())
	}
	if in.HasStructuredName() {
		sn := in.GetStructuredName()
		middle := make([]string, 0, len(sn.GetMiddleNames()))
		for _, m := range sn.GetMiddleNames() {
			middle = append(middle, s.id(m))
		}
		out.SetStructuredName(v2.UserTrait_StructuredName_builder{
			GivenName:   s.id(sn.GetGivenName()),
			FamilyName:  s.id(sn.GetFamilyName()),
			MiddleNames: middle,
			Prefix:      s.id(sn.GetPrefix()),
			Suffix:      s.id(sn.GetSuffix()),
		}.Build())
	}
	return out
}

func handleGroupTrait(s *sanitizer, msg proto.Message, refs *assetRefSet) proto.Message {
	in := msg.(*v2.GroupTrait)
	return v2.GroupTrait_builder{
		Icon:    s.sanitizeAssetRef(in.GetIcon(), refs),
		Profile: s.sanitizeStruct(in.GetProfile()),
	}.Build()
}

func handleAppTrait(s *sanitizer, msg proto.Message, refs *assetRefSet) proto.Message {
	in := msg.(*v2.AppTrait)
	helpURL := ""
	if in.GetHelpUrl() != "" {
		helpURL = "https://sanitized.example/help"
	}
	return v2.AppTrait_builder{
		HelpUrl: helpURL,
		Icon:    s.sanitizeAssetRef(in.GetIcon(), refs),
		Logo:    s.sanitizeAssetRef(in.GetLogo(), refs),
		Profile: s.sanitizeStruct(in.GetProfile()),
		Flags:   in.GetFlags(),
	}.Build()
}

func handleRoleTrait(s *sanitizer, msg proto.Message, _ *assetRefSet) proto.Message {
	in := msg.(*v2.RoleTrait)
	out := v2.RoleTrait_builder{
		Profile: s.sanitizeStruct(in.GetProfile()),
	}.Build()
	if rsc := in.GetRoleScopeConditions(); rsc != nil {
		conds := make([]*v2.RoleScopeCondition, 0, len(rsc.GetConditions()))
		for _, c := range rsc.GetConditions() {
			conds = append(conds, v2.RoleScopeCondition_builder{
				Expression: s.id(c.GetExpression()),
			}.Build())
		}
		out.SetRoleScopeConditions(v2.RoleScopeConditions_builder{
			Type:       rsc.GetType(),
			Conditions: conds,
		}.Build())
	}
	return out
}

func handleSecretTrait(s *sanitizer, msg proto.Message, _ *assetRefSet) proto.Message {
	in := msg.(*v2.SecretTrait)
	out := v2.SecretTrait_builder{
		Profile:     s.sanitizeStruct(in.GetProfile()),
		CreatedById: s.transformResourceID(in.GetCreatedById()),
		IdentityId:  s.transformResourceID(in.GetIdentityId()),
	}.Build()
	if in.HasCreatedAt() {
		out.SetCreatedAt(s.shifter.shift(in.GetCreatedAt()))
	}
	if in.HasExpiresAt() {
		out.SetExpiresAt(s.shifter.shift(in.GetExpiresAt()))
	}
	if in.HasLastUsedAt() {
		out.SetLastUsedAt(s.shifter.shift(in.GetLastUsedAt()))
	}
	return out
}

func handleLicenseProfileTrait(s *sanitizer, msg proto.Message, _ *assetRefSet) proto.Message {
	in := msg.(*v2.LicenseProfileTrait)
	entIDs := make([]string, 0, len(in.GetEntitlementIds()))
	for _, eid := range in.GetEntitlementIds() {
		entIDs = append(entIDs, s.id(eid))
	}
	return v2.LicenseProfileTrait_builder{
		LicenseName:        in.GetLicenseName(),
		PurchasedSeats:     in.GetPurchasedSeats(),
		ConsumedSeats:      in.GetConsumedSeats(),
		CostPerUnitInCents: in.GetCostPerUnitInCents(),
		Currency:           in.GetCurrency(),
		EntitlementIds:     entIDs,
	}.Build()
}

func handleScopeBindingTrait(s *sanitizer, msg proto.Message, _ *assetRefSet) proto.Message {
	in := msg.(*v2.ScopeBindingTrait)
	return v2.ScopeBindingTrait_builder{
		RoleId:          s.transformResourceID(in.GetRoleId()),
		ScopeResourceId: s.transformResourceID(in.GetScopeResourceId()),
	}.Build()
}
