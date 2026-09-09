package appstoreconnect

import (
	"fmt"
	"strings"
	"time"
)

// Role names are a fixed enum in App Store Connect; there is no discovery endpoint for them.
// https://developer.apple.com/documentation/appstoreconnectapi/userrole
const (
	RoleAdmin                       = "ADMIN"
	RoleFinance                     = "FINANCE"
	RoleAccountHolder               = "ACCOUNT_HOLDER"
	RoleSales                       = "SALES"
	RoleMarketing                   = "MARKETING"
	RoleAppManager                  = "APP_MANAGER"
	RoleDeveloper                   = "DEVELOPER"
	RoleAccessToReports             = "ACCESS_TO_REPORTS"
	RoleCustomerSupport             = "CUSTOMER_SUPPORT"
	RoleCreateApps                  = "CREATE_APPS"
	RoleCloudManagedDeveloperID     = "CLOUD_MANAGED_DEVELOPER_ID"
	RoleCloudManagedAppDistribution = "CLOUD_MANAGED_APP_DISTRIBUTION"
	RoleGenerateIndividualKeys      = "GENERATE_INDIVIDUAL_KEYS"
)

// AllRoles is the full set of roles the connector syncs as role resources, in the order Apple
// documents them.
var AllRoles = []string{
	RoleAdmin,
	RoleFinance,
	RoleAccountHolder,
	RoleSales,
	RoleMarketing,
	RoleAppManager,
	RoleDeveloper,
	RoleAccessToReports,
	RoleCustomerSupport,
	RoleCreateApps,
	RoleCloudManagedDeveloperID,
	RoleCloudManagedAppDistribution,
	RoleGenerateIndividualKeys,
}

// roleDisplayNames overrides the generated title-cased name for roles whose generated name reads
// badly. Anything absent falls back to title-casing the enum value.
var roleDisplayNames = map[string]string{
	RoleAccountHolder:               "Account Holder",
	RoleAppManager:                  "App Manager",
	RoleAccessToReports:             "Access to Reports",
	RoleCustomerSupport:             "Customer Support",
	RoleCreateApps:                  "Create Apps",
	RoleCloudManagedDeveloperID:     "Cloud Managed Developer ID",
	RoleCloudManagedAppDistribution: "Cloud Managed App Distribution",
	RoleGenerateIndividualKeys:      "Generate Individual Keys",
}

// RoleDisplayName renders an App Store Connect role enum value for humans.
func RoleDisplayName(role string) string {
	if name, ok := roleDisplayNames[role]; ok {
		return name
	}

	parts := strings.Split(strings.ToLower(role), "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		parts[i] = strings.ToUpper(part[:1]) + part[1:]
	}

	return strings.Join(parts, " ")
}

// IsKnownRole reports whether the role is one the connector models as a role resource.
func IsKnownRole(role string) bool {
	for _, known := range AllRoles {
		if known == role {
			return true
		}
	}
	return false
}

// Links is the JSON:API links object. Only `next` matters for pagination: Apple returns a fully
// formed URL that already carries the cursor, so the connector never rebuilds it by hand.
type Links struct {
	Self string `json:"self,omitempty"`
	Next string `json:"next,omitempty"`
}

// Paging reports how many records exist behind a collection or relationship.
type Paging struct {
	Total int `json:"total"`
	Limit int `json:"limit"`
}

// Meta carries paging information for a document or a relationship.
type Meta struct {
	Paging Paging `json:"paging"`
}

// ResourceIdentifier is a JSON:API type/id pair used inside relationships.
type ResourceIdentifier struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

// Relationship is a to-many JSON:API relationship. `Data` is only populated when the caller asked
// for the relationship to be included, and Apple caps how many identifiers it inlines.
type Relationship struct {
	Data  []ResourceIdentifier `json:"data"`
	Links Links                `json:"links,omitempty"`
	Meta  *Meta                `json:"meta,omitempty"`
}

// UserAttributes are the App Store Connect user attributes the connector reads.
type UserAttributes struct {
	Username            string   `json:"username"`
	FirstName           string   `json:"firstName"`
	LastName            string   `json:"lastName"`
	Roles               []string `json:"roles"`
	AllAppsVisible      bool     `json:"allAppsVisible"`
	ProvisioningAllowed bool     `json:"provisioningAllowed"`
}

// UserRelationships holds the relationships returned alongside a user.
type UserRelationships struct {
	VisibleApps *Relationship `json:"visibleApps,omitempty"`
}

// User is a member of the App Store Connect team.
type User struct {
	Type          string            `json:"type"`
	ID            string            `json:"id"`
	Attributes    UserAttributes    `json:"attributes"`
	Relationships UserRelationships `json:"relationships"`
}

// DisplayName renders the user's name, falling back to the Apple ID when no name is set.
func (u *User) DisplayName() string {
	name := strings.TrimSpace(strings.TrimSpace(u.Attributes.FirstName) + " " + strings.TrimSpace(u.Attributes.LastName))
	if name == "" {
		return u.Attributes.Username
	}
	return name
}

// VisibleAppIDs returns the inlined visible app identifiers.
func (u *User) VisibleAppIDs() []string {
	if u.Relationships.VisibleApps == nil {
		return nil
	}

	ids := make([]string, 0, len(u.Relationships.VisibleApps.Data))
	for _, ref := range u.Relationships.VisibleApps.Data {
		ids = append(ids, ref.ID)
	}
	return ids
}

// VisibleAppsComplete reports whether the inlined visibleApps relationship holds every app the user
// can see. Apple caps the inlined identifiers (50 per user), so a user with more visible apps than
// that needs a follow-up request to GET /v1/users/{id}/visibleApps. Returning false when the
// relationship was not requested at all is deliberate: an absent relationship is not an empty one,
// and treating it as empty would silently drop grants.
func (u *User) VisibleAppsComplete() bool {
	rel := u.Relationships.VisibleApps
	if rel == nil || rel.Data == nil {
		return false
	}
	if rel.Meta != nil && rel.Meta.Paging.Total > len(rel.Data) {
		return false
	}
	return true
}

// AppAttributes are the App Store Connect app attributes the connector reads.
type AppAttributes struct {
	Name          string `json:"name"`
	BundleID      string `json:"bundleId"`
	SKU           string `json:"sku"`
	PrimaryLocale string `json:"primaryLocale"`
}

// App is an app record in the team's App Store Connect account.
type App struct {
	Type       string        `json:"type"`
	ID         string        `json:"id"`
	Attributes AppAttributes `json:"attributes"`
}

// UserInvitationAttributes are the attributes of a pending App Store Connect invitation.
type UserInvitationAttributes struct {
	Email               string     `json:"email"`
	FirstName           string     `json:"firstName"`
	LastName            string     `json:"lastName"`
	ExpirationDate      *time.Time `json:"expirationDate"`
	Roles               []string   `json:"roles"`
	AllAppsVisible      bool       `json:"allAppsVisible"`
	ProvisioningAllowed bool       `json:"provisioningAllowed"`
}

// UserInvitation is an outstanding invitation to join the team. Account creation in App Store
// Connect is invitation-only, so an invited person exists here (and nowhere else) until they accept.
type UserInvitation struct {
	Type          string                   `json:"type"`
	ID            string                   `json:"id"`
	Attributes    UserInvitationAttributes `json:"attributes"`
	Relationships UserRelationships        `json:"relationships"`
}

// DisplayName renders the invitee's name, falling back to the invited email address.
func (i *UserInvitation) DisplayName() string {
	name := strings.TrimSpace(strings.TrimSpace(i.Attributes.FirstName) + " " + strings.TrimSpace(i.Attributes.LastName))
	if name == "" {
		return i.Attributes.Email
	}
	return name
}

// VisibleAppIDs returns the inlined visible app identifiers for the invitation.
func (i *UserInvitation) VisibleAppIDs() []string {
	if i.Relationships.VisibleApps == nil {
		return nil
	}

	ids := make([]string, 0, len(i.Relationships.VisibleApps.Data))
	for _, ref := range i.Relationships.VisibleApps.Data {
		ids = append(ids, ref.ID)
	}
	return ids
}

// usersResponse is the JSON:API document returned by GET /v1/users.
type usersResponse struct {
	Data  []User `json:"data"`
	Links Links  `json:"links"`
	Meta  *Meta  `json:"meta,omitempty"`
}

// userResponse is the JSON:API document returned by GET/PATCH /v1/users/{id}.
type userResponse struct {
	Data  User  `json:"data"`
	Links Links `json:"links"`
}

// appsResponse is the JSON:API document returned by GET /v1/apps and
// GET /v1/users/{id}/visibleApps.
type appsResponse struct {
	Data  []App `json:"data"`
	Links Links `json:"links"`
	Meta  *Meta `json:"meta,omitempty"`
}

// userInvitationsResponse is the JSON:API document returned by GET /v1/userInvitations.
type userInvitationsResponse struct {
	Data  []UserInvitation `json:"data"`
	Links Links            `json:"links"`
	Meta  *Meta            `json:"meta,omitempty"`
}

// userInvitationResponse is the JSON:API document returned by POST /v1/userInvitations.
type userInvitationResponse struct {
	Data  UserInvitation `json:"data"`
	Links Links          `json:"links"`
}

// UserUpdate describes a PATCH to /v1/users/{id}. Apple replaces the whole roles array and the
// whole visibleApps relationship, so callers must send the complete desired state, not a delta.
type UserUpdate struct {
	Roles               []string
	AllAppsVisible      *bool
	ProvisioningAllowed *bool
	// VisibleAppIDs is only sent when SetVisibleApps is true, because an omitted relationship and an
	// empty one mean very different things to Apple.
	VisibleAppIDs  []string
	SetVisibleApps bool
}

// UserInvitationRequest describes a POST to /v1/userInvitations.
type UserInvitationRequest struct {
	Email               string
	FirstName           string
	LastName            string
	Roles               []string
	AllAppsVisible      bool
	ProvisioningAllowed bool
	VisibleAppIDs       []string
}

// errorDetail is one entry of a JSON:API errors array.
type errorDetail struct {
	ID     string `json:"id,omitempty"`
	Status string `json:"status,omitempty"`
	Code   string `json:"code,omitempty"`
	Title  string `json:"title,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// ErrorDocument is the error envelope App Store Connect returns for a failed request.
type ErrorDocument struct {
	Errors []errorDetail `json:"errors"`
}

// Message implements uhttp.ErrorResponse so failed requests surface Apple's own explanation
// instead of a bare status code.
func (e *ErrorDocument) Message() string {
	if len(e.Errors) == 0 {
		return "unknown App Store Connect API error"
	}

	messages := make([]string, 0, len(e.Errors))
	for _, detail := range e.Errors {
		switch {
		case detail.Code != "" && detail.Detail != "":
			messages = append(messages, fmt.Sprintf("%s: %s", detail.Code, detail.Detail))
		case detail.Detail != "":
			messages = append(messages, detail.Detail)
		case detail.Title != "":
			messages = append(messages, detail.Title)
		default:
			messages = append(messages, detail.Code)
		}
	}

	return strings.Join(messages, "; ")
}

// Codes returns the machine-readable error codes Apple reported, so callers can branch on them.
func (e *ErrorDocument) Codes() []string {
	codes := make([]string, 0, len(e.Errors))
	for _, detail := range e.Errors {
		if detail.Code != "" {
			codes = append(codes, detail.Code)
		}
	}
	return codes
}
