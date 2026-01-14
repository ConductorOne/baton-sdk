package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// MCPServer wraps a ConnectorServer and exposes its functionality via MCP.
type MCPServer struct {
	connector types.ConnectorServer
	server    *mcp.Server
	caps      *v2.ConnectorCapabilities
}

// NewMCPServer creates a new MCP server that wraps the given ConnectorServer.
func NewMCPServer(ctx context.Context, name string, connector types.ConnectorServer) (*MCPServer, error) {
	// Get connector metadata to determine capabilities.
	metaResp, err := connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get connector metadata: %w", err)
	}

	s := mcp.NewServer(
		&mcp.Implementation{
			Name:    name,
			Version: "1.0.0",
		},
		nil,
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
func (m *MCPServer) Serve(ctx context.Context) error {
	return m.server.Run(ctx, &mcp.StdioTransport{})
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
	// get_metadata
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "get_metadata",
		Description: "Get connector metadata including display name, description, and capabilities",
	}, m.handleGetMetadata)

	// validate
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "validate",
		Description: "Validate the connector configuration and connectivity",
	}, m.handleValidate)

	// list_resource_types
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "list_resource_types",
		Description: "List all resource types supported by this connector",
	}, m.handleListResourceTypes)

	// list_resources
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "list_resources",
		Description: "List resources of a specific type",
	}, m.handleListResources)

	// get_resource
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "get_resource",
		Description: "Get a specific resource by its type and ID",
	}, m.handleGetResource)

	// list_entitlements
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "list_entitlements",
		Description: "List entitlements (permissions, roles, memberships) for a resource",
	}, m.handleListEntitlements)

	// list_grants
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "list_grants",
		Description: "List grants (who has what access) for a resource",
	}, m.handleListGrants)
}

// registerProvisioningTools registers tools for provisioning operations.
func (m *MCPServer) registerProvisioningTools() {
	// grant
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "grant",
		Description: "Grant an entitlement to a principal (user or group)",
	}, m.handleGrant)

	// revoke
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "revoke",
		Description: "Revoke a grant from a principal",
	}, m.handleRevoke)

	// create_resource
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "create_resource",
		Description: "Create a new resource",
	}, m.handleCreateResource)

	// delete_resource
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "delete_resource",
		Description: "Delete a resource",
	}, m.handleDeleteResource)
}

// registerTicketingTools registers tools for ticketing operations.
func (m *MCPServer) registerTicketingTools() {
	// list_ticket_schemas
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "list_ticket_schemas",
		Description: "List available ticket schemas",
	}, m.handleListTicketSchemas)

	// create_ticket
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "create_ticket",
		Description: "Create a new ticket",
	}, m.handleCreateTicket)

	// get_ticket
	mcp.AddTool(m.server, &mcp.Tool{
		Name:        "get_ticket",
		Description: "Get a ticket by ID",
	}, m.handleGetTicket)
}
