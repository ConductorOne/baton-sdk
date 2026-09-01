package connector

import (
	"fmt"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/structpb"
)

// Page state identifiers. A single List call walks users and then invitations, so the page token
// has to record which of the two it is in the middle of.
const (
	pageStateUsers       = "users"
	pageStateInvitations = "invitations"
)

// Account creation profile field names.
const (
	profileFieldEmail               = "email"
	profileFieldFirstName           = "first_name"
	profileFieldLastName            = "last_name"
	profileFieldRoles               = "roles"
	profileFieldAllAppsVisible      = "all_apps_visible"
	profileFieldProvisioningAllowed = "provisioning_allowed"
	profileFieldVisibleAppIDs       = "visible_app_ids"
)

// Resource profile keys.
const (
	profileKeyUserID              = "user_id"
	profileKeyLogin               = "login"
	profileKeyAllAppsVisible      = "all_apps_visible"
	profileKeyProvisioningAllowed = "provisioning_allowed"
	profileKeyPending             = "pending"
	profileKeyRoles               = "roles"
	profileKeyRole                = "role"
	profileKeyBundleID            = "bundle_id"
	profileKeySKU                 = "sku"
)

// newPaginationBag restores a page token, seeding a fresh two-phase walk over users and then
// invitations when there is no token yet.
func newPaginationBag(token string) (*pagination.Bag, error) {
	bag := &pagination.Bag{}
	if err := bag.Unmarshal(token); err != nil {
		return nil, fmt.Errorf("baton-appstoreconnect: page token corrupt: %w", err)
	}

	if bag.Current() == nil {
		// Pushed in reverse: invitations are handled once the user pages are exhausted.
		bag.Push(pagination.PageState{ResourceTypeID: pageStateInvitations})
		bag.Push(pagination.PageState{ResourceTypeID: pageStateUsers})
	}

	return bag, nil
}

// advance records the next page for the current phase, or moves on to the next phase when the
// current one has no more pages, and returns the serialized token. An empty return value tells the
// syncer that the walk is finished.
func advance(bag *pagination.Bag, nextURL string) (string, error) {
	if err := bag.Next(nextURL); err != nil {
		return "", err
	}

	return bag.Marshal()
}

// userResourceID builds the resource id used for a principal.
func userResourceID(userID string) *v2.ResourceId {
	return &v2.ResourceId{
		ResourceType: resourceTypeUser.Id,
		Resource:     userID,
	}
}

// contains reports whether values holds target.
func contains(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// removeValue returns values without every occurrence of target, preserving order.
func removeValue(values []string, target string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == target {
			continue
		}
		out = append(out, value)
	}
	return out
}

// toAnySlice widens a string slice so it can be embedded in a resource profile. structpb only
// accepts []interface{} for lists, and a []string silently fails the conversion.
func toAnySlice(values []string) []interface{} {
	out := make([]interface{}, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}

// roleResource builds the resource for one of App Store Connect's fixed roles.
func roleResource(role string) (*v2.Resource, error) {
	displayName := appstoreconnect.RoleDisplayName(role)

	return rs.NewRoleResource(
		displayName,
		resourceTypeRole,
		role,
		nil,
		rs.WithResourceProfile(map[string]interface{}{
			profileKeyRole: role,
		}),
	)
}

// stringFromProfileField reads a string out of an account creation profile.
func stringFromProfileField(fields map[string]*structpb.Value, key string) string {
	if value, ok := fields[key]; ok {
		return value.GetStringValue()
	}
	return ""
}

// boolFromProfileField reads a bool out of an account creation profile, falling back to
// defaultValue when the field is absent or is not a bool.
func boolFromProfileField(fields map[string]*structpb.Value, key string, defaultValue bool) bool {
	value, ok := fields[key]
	if !ok {
		return defaultValue
	}
	if _, isBool := value.GetKind().(*structpb.Value_BoolValue); !isBool {
		return defaultValue
	}
	return value.GetBoolValue()
}

// stringListFromProfileField reads a list of strings out of an account creation profile.
func stringListFromProfileField(fields map[string]*structpb.Value, key string) []string {
	value, ok := fields[key]
	if !ok {
		return nil
	}

	var out []string
	for _, item := range value.GetListValue().GetValues() {
		if str := item.GetStringValue(); str != "" {
			out = append(out, str)
		}
	}
	return out
}
