package connector

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/conductorone/baton-appstoreconnect/pkg/appstoreconnect"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"google.golang.org/protobuf/types/known/structpb"
)

// newTestConnector wires a connector to a stand-in App Store Connect API.
func newTestConnector(t *testing.T, handler http.Handler) *AppStoreConnect {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))

	connector, err := New(context.Background(), Config{
		KeyID:         "KEYID123",
		IssuerID:      "issuer-uuid",
		PrivateKeyPEM: keyPEM,
		BaseURL:       server.URL,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	return connector
}

// listAll drains a syncer's List, following page tokens the way the sync engine does.
func listAll(t *testing.T, syncer interface {
	List(context.Context, *v2.ResourceId, rs.SyncOpAttrs) ([]*v2.Resource, *rs.SyncOpResults, error)
},
) []*v2.Resource {
	t.Helper()

	var (
		all   []*v2.Resource
		token string
	)
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("pagination did not terminate")
		}

		resources, results, err := syncer.List(context.Background(), nil, rs.SyncOpAttrs{PageToken: pageToken(token)})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		all = append(all, resources...)

		if results == nil || results.NextPageToken == "" {
			return all
		}
		token = results.NextPageToken
	}
}

// grantAll drains a syncer's Grants across every page.
func grantAll(t *testing.T, syncer interface {
	Grants(context.Context, *v2.Resource, rs.SyncOpAttrs) ([]*v2.Grant, *rs.SyncOpResults, error)
}, resource *v2.Resource,
) []*v2.Grant {
	t.Helper()

	var (
		all   []*v2.Grant
		token string
	)
	for i := 0; ; i++ {
		if i > 20 {
			t.Fatal("pagination did not terminate")
		}

		grants, results, err := syncer.Grants(context.Background(), resource, rs.SyncOpAttrs{PageToken: pageToken(token)})
		if err != nil {
			t.Fatalf("Grants: %v", err)
		}
		all = append(all, grants...)

		if results == nil || results.NextPageToken == "" {
			return all
		}
		token = results.NextPageToken
	}
}

func pageToken(token string) pagination.Token {
	return pagination.Token{Token: token}
}

// fixtureHandler serves a small but representative team: two accepted users (one limited to a
// single app, one with access to everything) and one outstanding invitation.
func fixtureHandler(t *testing.T) *http.ServeMux {
	t.Helper()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[
			{"type":"users","id":"u-admin","attributes":{"username":"admin@example.com","firstName":"Ada","lastName":"Admin","roles":["ADMIN"],"allAppsVisible":true,"provisioningAllowed":true},
			 "relationships":{"visibleApps":{"data":[]}}},
			{"type":"users","id":"u-dev",
			 "attributes":{"username":"dev@example.com","firstName":"Dev","lastName":"Eloper",
			   "roles":["DEVELOPER","CUSTOMER_SUPPORT"],"allAppsVisible":false,"provisioningAllowed":false},
			 "relationships":{"visibleApps":{"data":[{"type":"apps","id":"app-1"}],"meta":{"paging":{"total":1,"limit":50}}}}}
		],"links":{}}`)
	})
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[
			{"type":"userInvitations","id":"inv-1","attributes":{"email":"pending@example.com","firstName":"Pat","lastName":"Pending","roles":["MARKETING"],"allAppsVisible":false},
			 "relationships":{"visibleApps":{"data":[{"type":"apps","id":"app-2"}],"meta":{"paging":{"total":1,"limit":50}}}}}
		],"links":{}}`)
	})
	mux.HandleFunc("/v1/apps", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[
			{"type":"apps","id":"app-1","attributes":{"name":"First App","bundleId":"com.example.first","sku":"FIRST"}},
			{"type":"apps","id":"app-2","attributes":{"name":"Second App","bundleId":"com.example.second"}}
		],"links":{}}`)
	})

	return mux
}

func TestUserSyncCoversMembersAndPendingInvitations(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))

	resources := listAll(t, userBuilder(connector.client))
	if len(resources) != 3 {
		t.Fatalf("expected 2 users and 1 invitation, got %d", len(resources))
	}

	byID := map[string]*v2.Resource{}
	for _, resource := range resources {
		byID[resource.GetId().GetResource()] = resource
	}

	admin, ok := byID["u-admin"]
	if !ok {
		t.Fatal("admin user missing")
	}
	adminTrait, err := rs.GetUserTrait(admin)
	if err != nil {
		t.Fatalf("GetUserTrait: %v", err)
	}
	if got := rs.GetStatus(admin).GetStatus(); got != v2.Status_RESOURCE_STATUS_ENABLED {
		t.Errorf("admin status = %v, want ENABLED", got)
	}
	if len(adminTrait.GetEmails()) == 0 || adminTrait.GetEmails()[0].GetAddress() != "admin@example.com" {
		t.Errorf("admin email = %v", adminTrait.GetEmails())
	}
	if admin.GetDisplayName() != "Ada Admin" {
		t.Errorf("admin display name = %q", admin.GetDisplayName())
	}

	invitation, ok := byID["inv-1"]
	if !ok {
		t.Fatal("pending invitation missing")
	}
	invitationTrait, err := rs.GetUserTrait(invitation)
	if err != nil {
		t.Fatalf("GetUserTrait: %v", err)
	}
	if got := rs.GetStatus(invitation).GetStatus(); got != v2.Status_RESOURCE_STATUS_PENDING {
		t.Errorf("invitation status = %v, want PENDING", got)
	}
	if invitationTrait.GetEmails()[0].GetAddress() != "pending@example.com" {
		t.Errorf("invitation email = %v", invitationTrait.GetEmails())
	}
}

func TestRoleSyncAndGrants(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))
	roles := roleBuilder(connector.client)

	resources, _, err := roles.List(context.Background(), nil, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != len(appstoreconnect.AllRoles) {
		t.Fatalf("expected %d roles, got %d", len(appstoreconnect.AllRoles), len(resources))
	}

	var developer *v2.Resource
	for _, resource := range resources {
		if resource.GetId().GetResource() == appstoreconnect.RoleDeveloper {
			developer = resource
		}
	}
	if developer == nil {
		t.Fatal("DEVELOPER role missing")
	}

	entitlements, _, err := roles.Entitlements(context.Background(), developer, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	if len(entitlements) != 1 || entitlements[0].GetSlug() != roleAssignment {
		t.Fatalf("unexpected entitlements: %+v", entitlements)
	}

	grants := grantAll(t, roles, developer)
	if len(grants) != 1 {
		t.Fatalf("expected exactly one DEVELOPER grant, got %d", len(grants))
	}
	if got := grants[0].GetPrincipal().GetId().GetResource(); got != "u-dev" {
		t.Errorf("DEVELOPER granted to %q, want u-dev", got)
	}

	// Pending invitations carry roles too, and they should show up as grants so an access review
	// sees access that has been handed out but not yet claimed.
	var marketing *v2.Resource
	for _, resource := range resources {
		if resource.GetId().GetResource() == appstoreconnect.RoleMarketing {
			marketing = resource
		}
	}
	marketingGrants := grantAll(t, roles, marketing)
	if len(marketingGrants) != 1 || marketingGrants[0].GetPrincipal().GetId().GetResource() != "inv-1" {
		t.Fatalf("expected the pending invitation to hold MARKETING, got %+v", marketingGrants)
	}
}

func TestAppGrantsIncludeAllAppsVisibleUsers(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))
	apps := appBuilder(connector.client)

	resources := listAll(t, apps)
	if len(resources) != 2 {
		t.Fatalf("expected 2 apps, got %d", len(resources))
	}

	byID := map[string]*v2.Resource{}
	for _, resource := range resources {
		byID[resource.GetId().GetResource()] = resource
	}

	firstGrants := principalIDs(grantAll(t, apps, byID["app-1"]))
	// u-admin sees everything, u-dev is limited to app-1.
	if !firstGrants["u-admin"] || !firstGrants["u-dev"] {
		t.Errorf("app-1 grants = %v, want both u-admin and u-dev", firstGrants)
	}
	if firstGrants["inv-1"] {
		t.Error("app-1 must not be granted to an invitation limited to app-2")
	}

	secondGrants := principalIDs(grantAll(t, apps, byID["app-2"]))
	if !secondGrants["u-admin"] {
		t.Error("a user with all-apps access must hold every app")
	}
	if secondGrants["u-dev"] {
		t.Error("u-dev is limited to app-1 and must not hold app-2")
	}
	if !secondGrants["inv-1"] {
		t.Error("the pending invitation is limited to app-2 and should hold it")
	}
}

// TestAppGrantsFallBackWhenRelationshipTruncated covers the case Apple's 50-app inline cap creates:
// the inlined relationship is incomplete, so trusting it would drop grants.
func TestAppGrantsFallBackWhenRelationshipTruncated(t *testing.T) {
	var visibleAppsCalls int

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[
			{"type":"users","id":"u-many","attributes":{"username":"many@example.com","roles":["DEVELOPER"],"allAppsVisible":false},
			 "relationships":{"visibleApps":{"data":[{"type":"apps","id":"app-other"}],"meta":{"paging":{"total":51,"limit":50}}}}}
		],"links":{}}`)
	})
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[],"links":{}}`)
	})
	mux.HandleFunc("/v1/users/u-many/visibleApps", func(w http.ResponseWriter, _ *http.Request) {
		visibleAppsCalls++
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":[{"type":"apps","id":"app-other"},{"type":"apps","id":"app-1"}],"links":{}}`)
	})

	connector := newTestConnector(t, mux)
	apps := appBuilder(connector.client)

	resource, err := appResource(&appstoreconnect.App{ID: "app-1", Attributes: appstoreconnect.AppAttributes{Name: "First App"}})
	if err != nil {
		t.Fatalf("appResource: %v", err)
	}

	grants := principalIDs(grantAll(t, apps, resource))
	if !grants["u-many"] {
		t.Error("expected the truncated relationship to be resolved against the relationship endpoint")
	}
	if visibleAppsCalls == 0 {
		t.Error("expected a follow-up request for the full visibleApps list")
	}
}

func TestRoleGrantIsFullReplaceOfExistingRoles(t *testing.T) {
	var patched map[string]any

	mux := fixtureHandler(t)
	mux.HandleFunc("/v1/users/u-dev", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev",
				"attributes":{"username":"dev@example.com","roles":["DEVELOPER","CUSTOMER_SUPPORT"],"allAppsVisible":false},
				"relationships":{"visibleApps":{"data":[{"type":"apps","id":"app-1"}],"meta":{"paging":{"total":1,"limit":50}}}}}}`)
		case http.MethodPatch:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &patched)
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev","attributes":{"username":"dev@example.com","roles":["DEVELOPER","CUSTOMER_SUPPORT","APP_MANAGER"]}}}`)
		}
	})

	connector := newTestConnector(t, mux)
	roles := roleBuilder(connector.client)

	principal, err := rs.NewUserResource("Dev", resourceTypeUser, "u-dev", nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}
	appManager, err := roleResource(appstoreconnect.RoleAppManager)
	if err != nil {
		t.Fatalf("roleResource: %v", err)
	}
	entitlements, _, err := roles.Entitlements(context.Background(), appManager, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}

	grants, _, err := roles.Grant(context.Background(), principal, entitlements[0])
	if err != nil {
		t.Fatalf("Grant: %v", err)
	}
	if len(grants) != 1 {
		t.Errorf("expected the new grant to be returned, got %d", len(grants))
	}

	sent := patchedRoles(t, patched)
	// Apple replaces the array wholesale, so the roles the user already had must be resent.
	for _, want := range []string{"DEVELOPER", "CUSTOMER_SUPPORT", "APP_MANAGER"} {
		if !sent[want] {
			t.Errorf("PATCH dropped role %q; sent %v", want, sent)
		}
	}
}

func TestRoleRevokeKeepsOtherRoles(t *testing.T) {
	var patched map[string]any

	mux := fixtureHandler(t)
	mux.HandleFunc("/v1/users/u-dev", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev","attributes":{"username":"dev@example.com","roles":["DEVELOPER","CUSTOMER_SUPPORT"]}}}`)
		case http.MethodPatch:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &patched)
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev","attributes":{"username":"dev@example.com","roles":["CUSTOMER_SUPPORT"]}}}`)
		}
	})

	connector := newTestConnector(t, mux)
	roles := roleBuilder(connector.client)

	developer, err := roleResource(appstoreconnect.RoleDeveloper)
	if err != nil {
		t.Fatalf("roleResource: %v", err)
	}
	entitlements, _, err := roles.Entitlements(context.Background(), developer, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	principal, err := rs.NewUserResource("Dev", resourceTypeUser, "u-dev", nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}

	if _, err := roles.Revoke(context.Background(), v2.Grant_builder{
		Entitlement: entitlements[0],
		Principal:   principal,
	}.Build()); err != nil {
		t.Fatalf("Revoke: %v", err)
	}

	sent := patchedRoles(t, patched)
	if sent["DEVELOPER"] {
		t.Error("revoked role was resent")
	}
	if !sent["CUSTOMER_SUPPORT"] {
		t.Errorf("revoke dropped an unrelated role; sent %v", sent)
	}
}

func TestAccountHolderCannotBeProvisioned(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))
	roles := roleBuilder(connector.client)

	accountHolder, err := roleResource(appstoreconnect.RoleAccountHolder)
	if err != nil {
		t.Fatalf("roleResource: %v", err)
	}
	entitlements, _, err := roles.Entitlements(context.Background(), accountHolder, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	principal, err := rs.NewUserResource("Dev", resourceTypeUser, "u-dev", nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}

	if _, _, err := roles.Grant(context.Background(), principal, entitlements[0]); err == nil {
		t.Error("granting ACCOUNT_HOLDER must fail: Apple does not allow it through the API")
	}
	if _, err := roles.Revoke(context.Background(), v2.Grant_builder{
		Entitlement: entitlements[0],
		Principal:   principal,
	}.Build()); err == nil {
		t.Error("revoking ACCOUNT_HOLDER must fail")
	}
}

// TestAppRevokeRefusesAllAppsUser guards the destructive case: dropping one app from a user who
// sees everything would mean silently narrowing them to a hand-built list.
func TestAppRevokeRefusesAllAppsUser(t *testing.T) {
	mux := fixtureHandler(t)
	mux.HandleFunc("/v1/users/u-admin", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPatch {
			t.Error("revoke must not PATCH a user who has access to all apps")
		}
		fmt.Fprint(w, `{"data":{"type":"users","id":"u-admin","attributes":{"username":"admin@example.com","roles":["ADMIN"],"allAppsVisible":true}}}`)
	})

	connector := newTestConnector(t, mux)
	apps := appBuilder(connector.client)

	resource, err := appResource(&appstoreconnect.App{ID: "app-1", Attributes: appstoreconnect.AppAttributes{Name: "First App"}})
	if err != nil {
		t.Fatalf("appResource: %v", err)
	}
	entitlements, _, err := apps.Entitlements(context.Background(), resource, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	principal, err := rs.NewUserResource("Ada", resourceTypeUser, "u-admin", nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}

	if _, err := apps.Revoke(context.Background(), v2.Grant_builder{
		Entitlement: entitlements[0],
		Principal:   principal,
	}.Build()); err == nil {
		t.Error("expected revoke to refuse rather than narrow an all-apps user")
	}
}

func TestAppGrantAddsToExistingVisibleApps(t *testing.T) {
	var patched map[string]any

	mux := fixtureHandler(t)
	mux.HandleFunc("/v1/users/u-dev", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev",
				"attributes":{"username":"dev@example.com","roles":["DEVELOPER"],"allAppsVisible":false},
				"relationships":{"visibleApps":{"data":[{"type":"apps","id":"app-1"}],"meta":{"paging":{"total":1,"limit":50}}}}}}`)
		case http.MethodPatch:
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &patched)
			fmt.Fprint(w, `{"data":{"type":"users","id":"u-dev","attributes":{"username":"dev@example.com"}}}`)
		}
	})

	connector := newTestConnector(t, mux)
	apps := appBuilder(connector.client)

	resource, err := appResource(&appstoreconnect.App{ID: "app-2", Attributes: appstoreconnect.AppAttributes{Name: "Second App"}})
	if err != nil {
		t.Fatalf("appResource: %v", err)
	}
	entitlements, _, err := apps.Entitlements(context.Background(), resource, rs.SyncOpAttrs{})
	if err != nil {
		t.Fatalf("Entitlements: %v", err)
	}
	principal, err := rs.NewUserResource("Dev", resourceTypeUser, "u-dev", nil)
	if err != nil {
		t.Fatalf("NewUserResource: %v", err)
	}

	if _, _, err := apps.Grant(context.Background(), principal, entitlements[0]); err != nil {
		t.Fatalf("Grant: %v", err)
	}

	sent := patchedVisibleApps(t, patched)
	if !sent["app-1"] {
		t.Errorf("grant dropped the app the user already had; sent %v", sent)
	}
	if !sent["app-2"] {
		t.Errorf("grant did not add the requested app; sent %v", sent)
	}
}

func TestCreateAccountIssuesAnInvitation(t *testing.T) {
	var posted map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &posted)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"data":{"type":"userInvitations","id":"inv-new","attributes":{"email":"new@example.com","firstName":"New","lastName":"Person","roles":["DEVELOPER"],"allAppsVisible":true}}}`)
	})

	connector := newTestConnector(t, mux)
	users := userBuilder(connector.client)

	profile, err := structpb.NewStruct(map[string]any{
		profileFieldEmail:     "new@example.com",
		profileFieldFirstName: "New",
		profileFieldLastName:  "Person",
		profileFieldRoles:     []any{"DEVELOPER"},
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}

	response, _, _, err := users.CreateAccount(context.Background(), v2.AccountInfo_builder{Profile: profile}.Build(), nil)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}

	success, ok := response.(*v2.CreateAccountResponse_SuccessResult)
	if !ok {
		t.Fatalf("expected a success result, got %T", response)
	}
	if success.GetResource().GetId().GetResource() != "inv-new" {
		t.Errorf("resource id = %q, want the invitation id", success.GetResource().GetId().GetResource())
	}

	if got := rs.GetStatus(success.GetResource()).GetStatus(); got != v2.Status_RESOURCE_STATUS_PENDING {
		t.Errorf("a new invitation must be PENDING, got %v", got)
	}

	if posted == nil {
		t.Fatal("no invitation was posted")
	}
}

func TestCreateAccountRequiresNameAndRejectsAccountHolder(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))
	users := userBuilder(connector.client)

	missingName, err := structpb.NewStruct(map[string]any{profileFieldEmail: "new@example.com"})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	if _, _, _, err := users.CreateAccount(context.Background(), v2.AccountInfo_builder{Profile: missingName}.Build(), nil); err == nil {
		t.Error("expected an error when first and last name are missing")
	}

	accountHolder, err := structpb.NewStruct(map[string]any{
		profileFieldEmail:     "new@example.com",
		profileFieldFirstName: "New",
		profileFieldLastName:  "Person",
		profileFieldRoles:     []any{"ACCOUNT_HOLDER"},
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}
	if _, _, _, err := users.CreateAccount(context.Background(), v2.AccountInfo_builder{Profile: accountHolder}.Build(), nil); err == nil {
		t.Error("expected an error when inviting someone as ACCOUNT_HOLDER")
	}
}

func TestCreateAccountReportsDuplicateAsAlreadyExists(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"errors":[{"status":"409","code":"ENTITY_ERROR.ATTRIBUTE.INVALID.DUPLICATE","detail":"already invited"}]}`)
	})

	connector := newTestConnector(t, mux)
	users := userBuilder(connector.client)

	profile, err := structpb.NewStruct(map[string]any{
		profileFieldEmail:     "dupe@example.com",
		profileFieldFirstName: "Dupe",
		profileFieldLastName:  "Person",
	})
	if err != nil {
		t.Fatalf("NewStruct: %v", err)
	}

	response, _, _, err := users.CreateAccount(context.Background(), v2.AccountInfo_builder{Profile: profile}.Build(), nil)
	if err != nil {
		t.Fatalf("CreateAccount: %v", err)
	}
	if _, ok := response.(*v2.CreateAccountResponse_AlreadyExistsResult); !ok {
		t.Fatalf("expected an already-exists result, got %T", response)
	}
}

// TestDeleteFallsBackToInvitationCancel covers the shared resource type: an id that is not a user
// may still be a pending invitation, which lives behind a different endpoint.
func TestDeleteFallsBackToInvitationCancel(t *testing.T) {
	var canceled bool

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/inv-1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"errors":[{"status":"404","code":"NOT_FOUND","detail":"no such user"}]}`)
	})
	mux.HandleFunc("/v1/userInvitations/inv-1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		canceled = true
		w.WriteHeader(http.StatusNoContent)
	})

	connector := newTestConnector(t, mux)
	users := userBuilder(connector.client)

	if _, err := users.Delete(context.Background(), &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "inv-1"}, nil); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !canceled {
		t.Error("expected the invitation to be canceled when the user delete came back 404")
	}
}

func TestDeleteReportsMissingResource(t *testing.T) {
	mux := http.NewServeMux()
	notFound := func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"errors":[{"status":"404","code":"NOT_FOUND","detail":"gone"}]}`)
	}
	mux.HandleFunc("/v1/users/ghost", notFound)
	mux.HandleFunc("/v1/userInvitations/ghost", notFound)

	connector := newTestConnector(t, mux)
	users := userBuilder(connector.client)

	annos, err := users.Delete(context.Background(), &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: "ghost"}, nil)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	found, err := annos.Pick(&v2.ResourceDoesNotExist{})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	if !found {
		t.Error("expected a ResourceDoesNotExist annotation")
	}
}

func TestValidateSurfacesInsufficientPermissions(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"errors":[{"status":"403","code":"FORBIDDEN_ERROR","detail":"not permitted"}]}`)
	})

	connector := newTestConnector(t, mux)

	_, err := connector.Validate(context.Background())
	if err == nil {
		t.Fatal("expected validation to fail")
	}
	if !strings.Contains(err.Error(), "Admin") {
		t.Errorf("expected the error to explain the Admin requirement, got %v", err)
	}
}

func TestValidateSucceeds(t *testing.T) {
	connector := newTestConnector(t, fixtureHandler(t))

	if _, err := connector.Validate(context.Background()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestAccountCreationSchema(t *testing.T) {
	schema := accountCreationSchema()
	fields := schema.GetFieldMap()

	for _, want := range []struct {
		key      string
		required bool
	}{
		{profileFieldEmail, true},
		{profileFieldFirstName, true},
		{profileFieldLastName, true},
		{profileFieldRoles, false},
		{profileFieldAllAppsVisible, false},
		{profileFieldVisibleAppIDs, false},
		{profileFieldProvisioningAllowed, false},
	} {
		field, ok := fields[want.key]
		if !ok {
			t.Errorf("field %q missing", want.key)
			continue
		}
		if field.GetRequired() != want.required {
			t.Errorf("field %q required = %v, want %v", want.key, field.GetRequired(), want.required)
		}
	}
}

func TestProfileFieldHelpers(t *testing.T) {
	list, err := structpb.NewList([]any{"a", "b"})
	if err != nil {
		t.Fatalf("NewList: %v", err)
	}
	fields := map[string]*structpb.Value{
		"text":   structpb.NewStringValue("hello"),
		"flag":   structpb.NewBoolValue(false),
		"number": structpb.NewNumberValue(1),
		"list":   structpb.NewListValue(list),
	}

	if got := stringFromProfileField(fields, "text"); got != "hello" {
		t.Errorf("stringFromProfileField = %q", got)
	}
	if got := stringFromProfileField(fields, "missing"); got != "" {
		t.Errorf("missing field should be empty, got %q", got)
	}
	if got := boolFromProfileField(fields, "flag", true); got {
		t.Error("explicit false must win over the default")
	}
	if got := boolFromProfileField(fields, "missing", true); !got {
		t.Error("missing field should fall back to the default")
	}
	if got := boolFromProfileField(fields, "number", true); !got {
		t.Error("a non-bool value should fall back to the default")
	}
	if got := stringListFromProfileField(fields, "list"); len(got) != 2 || got[0] != "a" {
		t.Errorf("stringListFromProfileField = %v", got)
	}
	if got := stringListFromProfileField(fields, "text"); got != nil {
		t.Errorf("a scalar should not read as a list, got %v", got)
	}
}

func TestContainsAndRemoveValue(t *testing.T) {
	values := []string{"a", "b", "a", "c"}

	if !contains(values, "b") || contains(values, "z") {
		t.Error("contains is wrong")
	}

	got := removeValue(values, "a")
	if len(got) != 2 || got[0] != "b" || got[1] != "c" {
		t.Errorf("removeValue = %v, want every occurrence removed", got)
	}
	if len(values) != 4 {
		t.Error("removeValue must not mutate its input")
	}

	empty := removeValue([]string{"a"}, "a")
	if empty == nil {
		t.Error("removeValue must return a non-nil empty slice so an empty roles array is sent explicitly")
	}
}

func principalIDs(grants []*v2.Grant) map[string]bool {
	out := map[string]bool{}
	for _, grant := range grants {
		out[grant.GetPrincipal().GetId().GetResource()] = true
	}
	return out
}

func patchedRoles(t *testing.T, patched map[string]any) map[string]bool {
	t.Helper()

	if patched == nil {
		t.Fatal("no PATCH was sent")
	}
	data, ok := patched["data"].(map[string]any)
	if !ok {
		t.Fatalf("PATCH has no data: %v", patched)
	}
	attributes, ok := data["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("PATCH has no attributes: %v", data)
	}

	out := map[string]bool{}
	for _, role := range attributes["roles"].([]any) {
		out[role.(string)] = true
	}
	return out
}

func patchedVisibleApps(t *testing.T, patched map[string]any) map[string]bool {
	t.Helper()

	if patched == nil {
		t.Fatal("no PATCH was sent")
	}
	data := patched["data"].(map[string]any)
	relationships, ok := data["relationships"].(map[string]any)
	if !ok {
		t.Fatalf("PATCH has no relationships: %v", data)
	}
	visibleApps := relationships["visibleApps"].(map[string]any)

	out := map[string]bool{}
	for _, identifier := range visibleApps["data"].([]any) {
		out[identifier.(map[string]any)["id"].(string)] = true
	}
	return out
}
