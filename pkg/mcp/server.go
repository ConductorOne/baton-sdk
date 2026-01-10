package mcp

import (
	"context"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// MCPServer wraps a ConnectorServer and exposes its functionality via MCP.
type MCPServer struct {
	connector types.ConnectorServer
	server    *server.MCPServer
	caps      *v2.ConnectorCapabilities
}

// NewMCPServer creates a new MCP server that wraps the given ConnectorServer.
func NewMCPServer(ctx context.Context, name string, connector types.ConnectorServer) (*MCPServer, error) {
	// Get connector metadata to determine capabilities.
	metaResp, err := connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get connector metadata: %w", err)
	}

	s := server.NewMCPServer(
		name,
		"1.0.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)

	m := &MCPServer{
		connector: connector,
		server:    s,
		caps:      metaResp.GetMetadata().GetCapabilities(),
	}

	m.registerTools()
	return m, nil
}

// Serve starts the MCP server on stdio.
func (m *MCPServer) Serve() error {
	return server.ServeStdio(m.server)
}

// registerTools registers all MCP tools based on connector capabilities.
func (m *MCPServer) registerTools() {
	// Always register read-only tools.
	m.registerReadTools()

	// Register provisioning tools if the connector supports provisioning.
	if m.hasCapability(v2.Capability_CAPABILITY_PROVISION) {
		m.registerProvisioningTools()
	}

	// Register ticketing tools if the connector supports ticketing.
	if m.hasCapability(v2.Capability_CAPABILITY_TICKETING) {
		m.registerTicketingTools()
	}
}

// hasCapability checks if the connector has the given capability.
func (m *MCPServer) hasCapability(cap v2.Capability) bool {
	if m.caps == nil {
		return false
	}
	for _, c := range m.caps.GetConnectorCapabilities() {
		if c == cap {
			return true
		}
	}
	return false
}

// registerReadTools registers read-only tools that are always available.
func (m *MCPServer) registerReadTools() {
	// get_metadata - Get connector metadata and capabilities.
	m.server.AddTool(
		mcp.NewTool("get_metadata",
			mcp.WithDescription("Get connector metadata including display name, description, and capabilities"),
		),
		m.handleGetMetadata,
	)

	// validate - Validate connector configuration.
	m.server.AddTool(
		mcp.NewTool("validate",
			mcp.WithDescription("Validate the connector configuration and connectivity"),
		),
		m.handleValidate,
	)

	// list_resource_types - List available resource types.
	m.server.AddTool(
		mcp.NewTool("list_resource_types",
			mcp.WithDescription("List all resource types supported by this connector"),
			mcp.WithNumber("page_size",
				mcp.Description("Number of items per page (default 50)"),
			),
			mcp.WithString("page_token",
				mcp.Description("Pagination token from previous response"),
			),
		),
		m.handleListResourceTypes,
	)

	// list_resources - List resources of a specific type.
	m.server.AddTool(
		mcp.NewTool("list_resources",
			mcp.WithDescription("List resources of a specific type"),
			mcp.WithString("resource_type_id",
				mcp.Required(),
				mcp.Description("The resource type ID to list (e.g., 'user', 'group')"),
			),
			mcp.WithString("parent_resource_type",
				mcp.Description("Parent resource type (optional, for hierarchical resources)"),
			),
			mcp.WithString("parent_resource_id",
				mcp.Description("Parent resource ID (optional, for hierarchical resources)"),
			),
			mcp.WithNumber("page_size",
				mcp.Description("Number of items per page (default 50)"),
			),
			mcp.WithString("page_token",
				mcp.Description("Pagination token from previous response"),
			),
		),
		m.handleListResources,
	)

	// get_resource - Get a specific resource by ID.
	m.server.AddTool(
		mcp.NewTool("get_resource",
			mcp.WithDescription("Get a specific resource by its type and ID"),
			mcp.WithString("resource_type",
				mcp.Required(),
				mcp.Description("The resource type (e.g., 'user', 'group')"),
			),
			mcp.WithString("resource_id",
				mcp.Required(),
				mcp.Description("The resource ID"),
			),
		),
		m.handleGetResource,
	)

	// list_entitlements - List entitlements for a resource.
	m.server.AddTool(
		mcp.NewTool("list_entitlements",
			mcp.WithDescription("List entitlements (permissions, roles, memberships) for a resource"),
			mcp.WithString("resource_type",
				mcp.Required(),
				mcp.Description("The resource type"),
			),
			mcp.WithString("resource_id",
				mcp.Required(),
				mcp.Description("The resource ID"),
			),
			mcp.WithNumber("page_size",
				mcp.Description("Number of items per page (default 50)"),
			),
			mcp.WithString("page_token",
				mcp.Description("Pagination token from previous response"),
			),
		),
		m.handleListEntitlements,
	)

	// list_grants - List grants for a resource.
	m.server.AddTool(
		mcp.NewTool("list_grants",
			mcp.WithDescription("List grants (who has what access) for a resource"),
			mcp.WithString("resource_type",
				mcp.Required(),
				mcp.Description("The resource type"),
			),
			mcp.WithString("resource_id",
				mcp.Required(),
				mcp.Description("The resource ID"),
			),
			mcp.WithNumber("page_size",
				mcp.Description("Number of items per page (default 50)"),
			),
			mcp.WithString("page_token",
				mcp.Description("Pagination token from previous response"),
			),
		),
		m.handleListGrants,
	)
}

// registerProvisioningTools registers tools for provisioning operations.
func (m *MCPServer) registerProvisioningTools() {
	// grant - Grant an entitlement to a principal.
	m.server.AddTool(
		mcp.NewTool("grant",
			mcp.WithDescription("Grant an entitlement to a principal (user or group)"),
			mcp.WithString("entitlement_resource_type",
				mcp.Required(),
				mcp.Description("Resource type of the entitlement"),
			),
			mcp.WithString("entitlement_resource_id",
				mcp.Required(),
				mcp.Description("Resource ID of the entitlement"),
			),
			mcp.WithString("entitlement_id",
				mcp.Required(),
				mcp.Description("The entitlement ID"),
			),
			mcp.WithString("principal_resource_type",
				mcp.Required(),
				mcp.Description("Resource type of the principal (e.g., 'user', 'group')"),
			),
			mcp.WithString("principal_resource_id",
				mcp.Required(),
				mcp.Description("Resource ID of the principal"),
			),
		),
		m.handleGrant,
	)

	// revoke - Revoke a grant.
	m.server.AddTool(
		mcp.NewTool("revoke",
			mcp.WithDescription("Revoke a grant from a principal"),
			mcp.WithString("grant_id",
				mcp.Required(),
				mcp.Description("The grant ID to revoke"),
			),
			mcp.WithString("entitlement_resource_type",
				mcp.Required(),
				mcp.Description("Resource type of the entitlement"),
			),
			mcp.WithString("entitlement_resource_id",
				mcp.Required(),
				mcp.Description("Resource ID of the entitlement"),
			),
			mcp.WithString("entitlement_id",
				mcp.Required(),
				mcp.Description("The entitlement ID"),
			),
			mcp.WithString("principal_resource_type",
				mcp.Required(),
				mcp.Description("Resource type of the principal"),
			),
			mcp.WithString("principal_resource_id",
				mcp.Required(),
				mcp.Description("Resource ID of the principal"),
			),
		),
		m.handleRevoke,
	)

	// create_resource - Create a new resource.
	m.server.AddTool(
		mcp.NewTool("create_resource",
			mcp.WithDescription("Create a new resource"),
			mcp.WithString("resource_type",
				mcp.Required(),
				mcp.Description("The resource type to create"),
			),
			mcp.WithString("display_name",
				mcp.Required(),
				mcp.Description("Display name for the new resource"),
			),
			mcp.WithString("parent_resource_type",
				mcp.Description("Parent resource type (optional)"),
			),
			mcp.WithString("parent_resource_id",
				mcp.Description("Parent resource ID (optional)"),
			),
		),
		m.handleCreateResource,
	)

	// delete_resource - Delete a resource.
	m.server.AddTool(
		mcp.NewTool("delete_resource",
			mcp.WithDescription("Delete a resource"),
			mcp.WithString("resource_type",
				mcp.Required(),
				mcp.Description("The resource type"),
			),
			mcp.WithString("resource_id",
				mcp.Required(),
				mcp.Description("The resource ID to delete"),
			),
		),
		m.handleDeleteResource,
	)
}

// registerTicketingTools registers tools for ticketing operations.
func (m *MCPServer) registerTicketingTools() {
	// list_ticket_schemas - List available ticket schemas.
	m.server.AddTool(
		mcp.NewTool("list_ticket_schemas",
			mcp.WithDescription("List available ticket schemas"),
		),
		m.handleListTicketSchemas,
	)

	// create_ticket - Create a new ticket.
	m.server.AddTool(
		mcp.NewTool("create_ticket",
			mcp.WithDescription("Create a new ticket"),
			mcp.WithString("schema_id",
				mcp.Required(),
				mcp.Description("The ticket schema ID"),
			),
			mcp.WithString("display_name",
				mcp.Required(),
				mcp.Description("Display name for the ticket"),
			),
			mcp.WithString("description",
				mcp.Description("Description of the ticket"),
			),
		),
		m.handleCreateTicket,
	)

	// get_ticket - Get a ticket by ID.
	m.server.AddTool(
		mcp.NewTool("get_ticket",
			mcp.WithDescription("Get a ticket by ID"),
			mcp.WithString("ticket_id",
				mcp.Required(),
				mcp.Description("The ticket ID"),
			),
		),
		m.handleGetTicket,
	)
}
