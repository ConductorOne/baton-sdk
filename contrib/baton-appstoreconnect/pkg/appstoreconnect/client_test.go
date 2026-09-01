package appstoreconnect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
)

// newTestClient wires a client to a stand-in API. Requests are still fully signed, so the auth path
// is exercised by every client test.
func newTestClient(t *testing.T, handler http.Handler) (*Client, *httptest.Server) {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	keyPEM, _ := testKeyPEM(t)
	client, err := NewClient(server.Client(), "KEYID123", "issuer-uuid", keyPEM, server.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	return client, server
}

func TestListUsersRequestShapeAndPagination(t *testing.T) {
	var requested []string

	var serverURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users", func(w http.ResponseWriter, r *http.Request) {
		requested = append(requested, r.URL.String())

		if auth := r.Header.Get("Authorization"); len(auth) < 8 || auth[:7] != "Bearer " {
			t.Errorf("missing bearer token, got %q", auth)
		}

		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "PAGE2" {
			fmt.Fprint(w, `{"data":[{"type":"users","id":"u2","attributes":{"username":"second@example.com","roles":["DEVELOPER"]}}],"links":{}}`)
			return
		}

		fmt.Fprintf(w, `{
			"data":[{"type":"users","id":"u1","attributes":{"username":"first@example.com","firstName":"First","lastName":"User","roles":["ADMIN","FINANCE"],"allAppsVisible":true}}],
			"links":{"next":"%s/v1/users?cursor=PAGE2"}
		}`, serverURL)
	})

	client, server := newTestClient(t, mux)
	serverURL = server.URL

	users, next, _, err := client.ListUsers(context.Background(), "")
	if err != nil {
		t.Fatalf("ListUsers: %v", err)
	}
	if len(users) != 1 || users[0].ID != "u1" {
		t.Fatalf("unexpected first page: %+v", users)
	}
	if users[0].DisplayName() != "First User" {
		t.Errorf("DisplayName = %q", users[0].DisplayName())
	}
	if next == "" {
		t.Fatal("expected a next link")
	}

	users, next, _, err = client.ListUsers(context.Background(), next)
	if err != nil {
		t.Fatalf("ListUsers page 2: %v", err)
	}
	if len(users) != 1 || users[0].ID != "u2" {
		t.Fatalf("unexpected second page: %+v", users)
	}
	if next != "" {
		t.Errorf("expected pagination to end, got %q", next)
	}

	// The first request has to ask for the relationship, otherwise per-app access is invisible.
	first := requested[0]
	for _, want := range []string{"include=visibleApps", "limit=200", "limit%5BvisibleApps%5D=50"} {
		if !strings.Contains(first, want) {
			t.Errorf("first request %q is missing %q", first, want)
		}
	}

	// The next link is followed verbatim; rebuilding it would drop Apple's cursor.
	if requested[1] != "/v1/users?cursor=PAGE2" {
		t.Errorf("second request = %q, want the next link to be followed as-is", requested[1])
	}
}

func TestListUserVisibleAppsFollowsEveryPage(t *testing.T) {
	var serverURL string
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/u1/visibleApps", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "PAGE2" {
			fmt.Fprint(w, `{"data":[{"type":"apps","id":"a2","attributes":{"name":"Second"}}],"links":{}}`)
			return
		}
		fmt.Fprintf(w, `{"data":[{"type":"apps","id":"a1","attributes":{"name":"First"}}],"links":{"next":"%s/v1/users/u1/visibleApps?cursor=PAGE2"}}`, serverURL)
	})

	client, server := newTestClient(t, mux)
	serverURL = server.URL

	apps, _, err := client.ListUserVisibleApps(context.Background(), "u1")
	if err != nil {
		t.Fatalf("ListUserVisibleApps: %v", err)
	}
	if len(apps) != 2 || apps[0].ID != "a1" || apps[1].ID != "a2" {
		t.Fatalf("expected both pages, got %+v", apps)
	}
}

func TestUpdateUserSendsFullReplacePayload(t *testing.T) {
	var body map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/u1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Fatalf("unmarshaling request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"type":"users","id":"u1","attributes":{"username":"first@example.com","roles":["DEVELOPER"]}}}`)
	})

	client, _ := newTestClient(t, mux)

	allApps := false
	if _, _, err := client.UpdateUser(context.Background(), "u1", UserUpdate{
		Roles:          []string{"DEVELOPER"},
		AllAppsVisible: &allApps,
		VisibleAppIDs:  []string{"a1", "a2"},
		SetVisibleApps: true,
	}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	data, ok := body["data"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no data member: %v", body)
	}
	if data["type"] != "users" || data["id"] != "u1" {
		t.Errorf("data identity = %v", data)
	}

	attributes, ok := data["attributes"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no attributes: %v", data)
	}
	roles, ok := attributes["roles"].([]any)
	if !ok || len(roles) != 1 || roles[0] != "DEVELOPER" {
		t.Errorf("roles = %v, want the complete replacement array", attributes["roles"])
	}
	if attributes["allAppsVisible"] != false {
		t.Errorf("allAppsVisible = %v, want false", attributes["allAppsVisible"])
	}

	relationships, ok := data["relationships"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no relationships: %v", data)
	}
	visibleApps, ok := relationships["visibleApps"].(map[string]any)
	if !ok {
		t.Fatalf("payload has no visibleApps relationship: %v", relationships)
	}
	identifiers, ok := visibleApps["data"].([]any)
	if !ok || len(identifiers) != 2 {
		t.Fatalf("visibleApps data = %v, want two identifiers", visibleApps["data"])
	}
	first, ok := identifiers[0].(map[string]any)
	if !ok || first["type"] != "apps" || first["id"] != "a1" {
		t.Errorf("first identifier = %v", identifiers[0])
	}
}

// TestUpdateUserOmitsVisibleAppsUnlessAsked guards the difference between "leave visibleApps alone"
// and "the user should see nothing": sending an empty relationship by accident revokes every app.
func TestUpdateUserOmitsVisibleAppsUnlessAsked(t *testing.T) {
	var body map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/u1", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"type":"users","id":"u1","attributes":{"username":"first@example.com"}}}`)
	})

	client, _ := newTestClient(t, mux)

	if _, _, err := client.UpdateUser(context.Background(), "u1", UserUpdate{Roles: []string{"ADMIN"}}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	data := body["data"].(map[string]any)
	if _, present := data["relationships"]; present {
		t.Error("visibleApps relationship must be omitted when SetVisibleApps is false")
	}
}

// TestUpdateUserSendsEmptyVisibleApps covers the other side: revoking the last app has to send an
// explicit empty array, not an omitted relationship.
func TestUpdateUserSendsEmptyVisibleApps(t *testing.T) {
	var body map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/u1", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"data":{"type":"users","id":"u1","attributes":{"username":"first@example.com"}}}`)
	})

	client, _ := newTestClient(t, mux)

	if _, _, err := client.UpdateUser(context.Background(), "u1", UserUpdate{SetVisibleApps: true}); err != nil {
		t.Fatalf("UpdateUser: %v", err)
	}

	data := body["data"].(map[string]any)
	relationships := data["relationships"].(map[string]any)
	visibleApps := relationships["visibleApps"].(map[string]any)
	identifiers, ok := visibleApps["data"].([]any)
	if !ok {
		t.Fatalf("visibleApps data = %v, want an empty array", visibleApps["data"])
	}
	if len(identifiers) != 0 {
		t.Errorf("visibleApps data = %v, want it empty", identifiers)
	}
}

func TestCreateUserInvitationPayload(t *testing.T) {
	var body map[string]any

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, `{"data":{"type":"userInvitations","id":"inv1","attributes":{"email":"new@example.com","firstName":"New","lastName":"Person","roles":["DEVELOPER"]}}}`)
	})

	client, _ := newTestClient(t, mux)

	invitation, _, err := client.CreateUserInvitation(context.Background(), UserInvitationRequest{
		Email:          "new@example.com",
		FirstName:      "New",
		LastName:       "Person",
		Roles:          []string{"DEVELOPER"},
		AllAppsVisible: false,
		VisibleAppIDs:  []string{"a1"},
	})
	if err != nil {
		t.Fatalf("CreateUserInvitation: %v", err)
	}
	if invitation.ID != "inv1" {
		t.Errorf("invitation id = %q", invitation.ID)
	}

	data := body["data"].(map[string]any)
	if data["type"] != "userInvitations" {
		t.Errorf("type = %v", data["type"])
	}
	attributes := data["attributes"].(map[string]any)
	if attributes["email"] != "new@example.com" {
		t.Errorf("email = %v", attributes["email"])
	}
	relationships, ok := data["relationships"].(map[string]any)
	if !ok {
		t.Fatalf("expected visibleApps on an app-limited invitation, got %v", data)
	}
	if relationships["visibleApps"] == nil {
		t.Error("visibleApps relationship missing")
	}
}

func TestErrorsCarryStatusAndAppleDetail(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/missing", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, `{"errors":[{"status":"404","code":"NOT_FOUND","title":"The specified resource does not exist","detail":"There is no resource of type 'users' with id 'missing'"}]}`)
	})
	mux.HandleFunc("/v1/userInvitations", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		fmt.Fprint(w, `{"errors":[{"status":"409","code":"ENTITY_ERROR.ATTRIBUTE.INVALID.DUPLICATE","detail":"A user or invitation with this email already exists"}]}`)
	})
	mux.HandleFunc("/v1/apps", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		fmt.Fprint(w, `{"errors":[{"status":"403","code":"FORBIDDEN_ERROR","detail":"The API key is not permitted to perform this operation"}]}`)
	})

	client, _ := newTestClient(t, mux)
	ctx := context.Background()

	_, _, err := client.GetUser(ctx, "missing")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !IsNotFound(err) {
		t.Errorf("IsNotFound = false for a 404: %v", err)
	}
	if IsConflict(err) {
		t.Error("a 404 must not read as a conflict")
	}
	if !HasErrorCode(err, "NOT_FOUND") {
		t.Errorf("expected Apple's error code to survive, got %v", err)
	}
	if !strings.Contains(err.Error(), "There is no resource of type") {
		t.Errorf("expected Apple's detail in the message, got %v", err)
	}

	_, _, err = client.CreateUserInvitation(ctx, UserInvitationRequest{Email: "dupe@example.com"})
	if err == nil {
		t.Fatal("expected an error")
	}
	if !IsConflict(err) {
		t.Errorf("IsConflict = false for a 409: %v", err)
	}
	if IsNotFound(err) {
		t.Error("a 409 must not read as not-found")
	}

	_, _, _, err = client.ListApps(ctx, "")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !IsForbidden(err) {
		t.Errorf("IsForbidden = false for a 403: %v", err)
	}
}

func TestRateLimitAnnotations(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/apps", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Rate-Limit", "user-hour-lim:3600;user-hour-rem:3421;")
		fmt.Fprint(w, `{"data":[],"links":{}}`)
	})

	client, _ := newTestClient(t, mux)

	_, _, annos, err := client.ListApps(context.Background(), "")
	if err != nil {
		t.Fatalf("ListApps: %v", err)
	}

	description := rateLimitFromAnnotations(t, annos)
	if description.GetLimit() != 3600 {
		t.Errorf("limit = %d, want 3600", description.GetLimit())
	}
	if description.GetRemaining() != 3421 {
		t.Errorf("remaining = %d, want 3421", description.GetRemaining())
	}
	if description.GetStatus() != v2.RateLimitDescription_STATUS_OK {
		t.Errorf("status = %v, want OK", description.GetStatus())
	}
}

func TestRateLimitAnnotationOnThrottledResponse(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/apps", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Rate-Limit", "user-hour-lim:3600;user-hour-rem:0;")
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprint(w, `{"errors":[{"status":"429","code":"RATE_LIMIT_EXCEEDED","detail":"Too many requests"}]}`)
	})

	client, _ := newTestClient(t, mux)

	_, _, annos, err := client.ListApps(context.Background(), "")
	if err == nil {
		t.Fatal("expected an error for a 429")
	}

	description := rateLimitFromAnnotations(t, annos)
	if description.GetStatus() != v2.RateLimitDescription_STATUS_OVERLIMIT {
		t.Errorf("status = %v, want OVERLIMIT", description.GetStatus())
	}
	if description.GetRemaining() != 0 {
		t.Errorf("remaining = %d, want 0", description.GetRemaining())
	}
	if description.GetResetAt() == nil {
		t.Error("expected Retry-After to produce a reset hint")
	}
}

func TestExtractRateLimitDataIgnoresJunk(t *testing.T) {
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"X-Rate-Limit": []string{"user-hour-lim:not-a-number;user-hour-rem:12;garbage"},
		},
	}

	description := extractRateLimitData(response)
	if description == nil {
		t.Fatal("expected a description")
	}
	if description.GetLimit() != 0 {
		t.Errorf("limit = %d, want 0 for an unparseable value", description.GetLimit())
	}
	if description.GetRemaining() != 12 {
		t.Errorf("remaining = %d, want 12", description.GetRemaining())
	}

	if extractRateLimitData(&http.Response{StatusCode: http.StatusOK, Header: http.Header{}}) != nil {
		t.Error("expected no description when there is no header and no throttling")
	}
}

func TestDeleteUser(t *testing.T) {
	var method, path string

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/users/u1", func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})

	client, _ := newTestClient(t, mux)

	if _, err := client.DeleteUser(context.Background(), "u1"); err != nil {
		t.Fatalf("DeleteUser: %v", err)
	}
	if method != http.MethodDelete || path != "/v1/users/u1" {
		t.Errorf("got %s %s, want DELETE /v1/users/u1", method, path)
	}
}

func TestVisibleAppsCompleteness(t *testing.T) {
	absent := User{ID: "u1"}
	if absent.VisibleAppsComplete() {
		t.Error("a user without the relationship must not read as complete")
	}

	empty := User{ID: "u2"}
	empty.Relationships.VisibleApps = &Relationship{Data: []ResourceIdentifier{}}
	if !empty.VisibleAppsComplete() {
		t.Error("an explicitly empty relationship is complete")
	}

	truncated := User{ID: "u3"}
	truncated.Relationships.VisibleApps = &Relationship{
		Data: []ResourceIdentifier{{Type: "apps", ID: "a1"}},
		Meta: &Meta{Paging: Paging{Total: 51, Limit: 50}},
	}
	if truncated.VisibleAppsComplete() {
		t.Error("a truncated relationship must not read as complete")
	}
	if got := truncated.VisibleAppIDs(); len(got) != 1 || got[0] != "a1" {
		t.Errorf("VisibleAppIDs = %v", got)
	}
}

func TestRoleDisplayNames(t *testing.T) {
	for role, want := range map[string]string{
		"ADMIN":                      "Admin",
		"APP_MANAGER":                "App Manager",
		"ACCOUNT_HOLDER":             "Account Holder",
		"ACCESS_TO_REPORTS":          "Access to Reports",
		"CLOUD_MANAGED_DEVELOPER_ID": "Cloud Managed Developer ID",
		"CUSTOMER_SUPPORT":           "Customer Support",
		"SOME_FUTURE_ROLE":           "Some Future Role",
	} {
		if got := RoleDisplayName(role); got != want {
			t.Errorf("RoleDisplayName(%q) = %q, want %q", role, got, want)
		}
	}

	if !IsKnownRole("ADMIN") || IsKnownRole("NOT_A_ROLE") {
		t.Error("IsKnownRole is wrong")
	}
}

func TestErrorDocumentMessage(t *testing.T) {
	empty := &ErrorDocument{}
	if empty.Message() == "" {
		t.Error("an empty document still needs a message")
	}

	document := &ErrorDocument{Errors: []errorDetail{
		{Code: "A", Detail: "first"},
		{Title: "second"},
	}}
	if got := document.Message(); got != "A: first; second" {
		t.Errorf("Message = %q", got)
	}
}

// rateLimitFromAnnotations pulls the rate limit description out of an annotation set.
func rateLimitFromAnnotations(t *testing.T, annos annotations.Annotations) *v2.RateLimitDescription {
	t.Helper()

	description := &v2.RateLimitDescription{}
	ok, err := annos.Pick(description)
	if err != nil {
		t.Fatalf("picking rate limit annotation: %v", err)
	}
	if !ok {
		t.Fatal("no rate limit annotation was attached")
	}

	return description
}
