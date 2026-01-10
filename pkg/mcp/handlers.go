package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const defaultPageSize = 50

// handleGetMetadata handles the get_metadata tool.
func (m *MCPServer) handleGetMetadata(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resp, err := m.connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get metadata: %v", err)), nil
	}

	result, err := protoToMap(resp.GetMetadata())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize metadata: %v", err)), nil
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleValidate handles the validate tool.
func (m *MCPServer) handleValidate(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resp, err := m.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("validation failed: %v", err)), nil
	}

	result := map[string]any{
		"valid":       true,
		"annotations": resp.GetAnnotations(),
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleListResourceTypes handles the list_resource_types tool.
func (m *MCPServer) handleListResourceTypes(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	pageSize := getPageSize(req)
	pageToken := getStringArg(req, "page_token")

	resp, err := m.connector.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{
		PageSize:  pageSize,
		PageToken: pageToken,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list resource types: %v", err)), nil
	}

	resourceTypes, err := protoListToMaps(resp.GetList())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize resource types: %v", err)), nil
	}

	result := map[string]any{
		"resource_types":  resourceTypes,
		"next_page_token": resp.GetNextPageToken(),
		"has_more":        resp.GetNextPageToken() != "",
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleListResources handles the list_resources tool.
func (m *MCPServer) handleListResources(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceTypeID, err := req.RequireString("resource_type_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type_id is required: %v", err)), nil
	}

	pageSize := getPageSize(req)
	pageToken := getStringArg(req, "page_token")

	// Build parent resource ID if specified.
	var parentResourceID *v2.ResourceId
	parentType := getStringArg(req, "parent_resource_type")
	parentID := getStringArg(req, "parent_resource_id")
	if parentType != "" && parentID != "" {
		parentResourceID = &v2.ResourceId{
			ResourceType: parentType,
			Resource:     parentID,
		}
	}

	resp, err := m.connector.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId:   resourceTypeID,
		ParentResourceId: parentResourceID,
		PageSize:         pageSize,
		PageToken:        pageToken,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list resources: %v", err)), nil
	}

	resources, err := protoListToMaps(resp.GetList())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize resources: %v", err)), nil
	}

	result := map[string]any{
		"resources":       resources,
		"next_page_token": resp.GetNextPageToken(),
		"has_more":        resp.GetNextPageToken() != "",
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleGetResource handles the get_resource tool.
func (m *MCPServer) handleGetResource(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := req.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type is required: %v", err)), nil
	}

	resourceID, err := req.RequireString("resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_id is required: %v", err)), nil
	}

	resp, err := m.connector.GetResource(ctx, &v2.ResourceGetterServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: resourceType,
			Resource:     resourceID,
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to get resource: %v", err)), nil
	}

	resource, err := protoToMap(resp.GetResource())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize resource: %v", err)), nil
	}

	jsonBytes, err := json.Marshal(resource)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleListEntitlements handles the list_entitlements tool.
func (m *MCPServer) handleListEntitlements(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := req.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type is required: %v", err)), nil
	}

	resourceID, err := req.RequireString("resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_id is required: %v", err)), nil
	}

	pageSize := getPageSize(req)
	pageToken := getStringArg(req, "page_token")

	resp, err := m.connector.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceType,
				Resource:     resourceID,
			},
		},
		PageSize:  pageSize,
		PageToken: pageToken,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list entitlements: %v", err)), nil
	}

	entitlements, err := protoListToMaps(resp.GetList())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize entitlements: %v", err)), nil
	}

	result := map[string]any{
		"entitlements":    entitlements,
		"next_page_token": resp.GetNextPageToken(),
		"has_more":        resp.GetNextPageToken() != "",
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleListGrants handles the list_grants tool.
func (m *MCPServer) handleListGrants(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := req.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type is required: %v", err)), nil
	}

	resourceID, err := req.RequireString("resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_id is required: %v", err)), nil
	}

	pageSize := getPageSize(req)
	pageToken := getStringArg(req, "page_token")

	resp, err := m.connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceType,
				Resource:     resourceID,
			},
		},
		PageSize:  pageSize,
		PageToken: pageToken,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list grants: %v", err)), nil
	}

	grants, err := protoListToMaps(resp.GetList())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize grants: %v", err)), nil
	}

	result := map[string]any{
		"grants":          grants,
		"next_page_token": resp.GetNextPageToken(),
		"has_more":        resp.GetNextPageToken() != "",
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleGrant handles the grant tool.
func (m *MCPServer) handleGrant(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	entResourceType, err := req.RequireString("entitlement_resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_resource_type is required: %v", err)), nil
	}

	entResourceID, err := req.RequireString("entitlement_resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_resource_id is required: %v", err)), nil
	}

	entID, err := req.RequireString("entitlement_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_id is required: %v", err)), nil
	}

	principalType, err := req.RequireString("principal_resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("principal_resource_type is required: %v", err)), nil
	}

	principalID, err := req.RequireString("principal_resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("principal_resource_id is required: %v", err)), nil
	}

	resp, err := m.connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: entResourceType,
					Resource:     entResourceID,
				},
			},
			Id: entID,
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: principalType,
				Resource:     principalID,
			},
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("grant failed: %v", err)), nil
	}

	grants, err := protoListToMaps(resp.GetGrants())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize grants: %v", err)), nil
	}

	result := map[string]any{
		"grants": grants,
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleRevoke handles the revoke tool.
func (m *MCPServer) handleRevoke(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	grantID, err := req.RequireString("grant_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("grant_id is required: %v", err)), nil
	}

	entResourceType, err := req.RequireString("entitlement_resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_resource_type is required: %v", err)), nil
	}

	entResourceID, err := req.RequireString("entitlement_resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_resource_id is required: %v", err)), nil
	}

	entID, err := req.RequireString("entitlement_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("entitlement_id is required: %v", err)), nil
	}

	principalType, err := req.RequireString("principal_resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("principal_resource_type is required: %v", err)), nil
	}

	principalID, err := req.RequireString("principal_resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("principal_resource_id is required: %v", err)), nil
	}

	_, err = m.connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Id: grantID,
			Entitlement: &v2.Entitlement{
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: entResourceType,
						Resource:     entResourceID,
					},
				},
				Id: entID,
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: principalType,
					Resource:     principalID,
				},
			},
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("revoke failed: %v", err)), nil
	}

	result := map[string]any{
		"success": true,
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleCreateResource handles the create_resource tool.
func (m *MCPServer) handleCreateResource(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := req.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type is required: %v", err)), nil
	}

	displayName, err := req.RequireString("display_name")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("display_name is required: %v", err)), nil
	}

	// Build parent resource if specified.
	var parentResource *v2.Resource
	parentType := getStringArg(req, "parent_resource_type")
	parentID := getStringArg(req, "parent_resource_id")
	if parentType != "" && parentID != "" {
		parentResource = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: parentType,
				Resource:     parentID,
			},
		}
	}

	resp, err := m.connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: resourceType,
			},
			DisplayName:      displayName,
			ParentResourceId: parentResource.GetId(),
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("create resource failed: %v", err)), nil
	}

	resource, err := protoToMap(resp.GetCreated())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize resource: %v", err)), nil
	}

	jsonBytes, err := json.Marshal(resource)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleDeleteResource handles the delete_resource tool.
func (m *MCPServer) handleDeleteResource(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resourceType, err := req.RequireString("resource_type")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_type is required: %v", err)), nil
	}

	resourceID, err := req.RequireString("resource_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("resource_id is required: %v", err)), nil
	}

	_, err = m.connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: resourceType,
			Resource:     resourceID,
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("delete resource failed: %v", err)), nil
	}

	result := map[string]any{
		"success": true,
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleListTicketSchemas handles the list_ticket_schemas tool.
func (m *MCPServer) handleListTicketSchemas(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	resp, err := m.connector.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to list ticket schemas: %v", err)), nil
	}

	schemas, err := protoListToMaps(resp.GetList())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize ticket schemas: %v", err)), nil
	}

	result := map[string]any{
		"schemas":         schemas,
		"next_page_token": resp.GetNextPageToken(),
		"has_more":        resp.GetNextPageToken() != "",
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleCreateTicket handles the create_ticket tool.
func (m *MCPServer) handleCreateTicket(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	schemaID, err := req.RequireString("schema_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("schema_id is required: %v", err)), nil
	}

	displayName, err := req.RequireString("display_name")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("display_name is required: %v", err)), nil
	}

	description := getStringArg(req, "description")

	resp, err := m.connector.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{
		Schema: &v2.TicketSchema{
			Id: schemaID,
		},
		Request: &v2.TicketRequest{
			DisplayName: displayName,
			Description: description,
		},
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("create ticket failed: %v", err)), nil
	}

	ticket, err := protoToMap(resp.GetTicket())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize ticket: %v", err)), nil
	}

	jsonBytes, err := json.Marshal(ticket)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// handleGetTicket handles the get_ticket tool.
func (m *MCPServer) handleGetTicket(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	ticketID, err := req.RequireString("ticket_id")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("ticket_id is required: %v", err)), nil
	}

	resp, err := m.connector.GetTicket(ctx, &v2.TicketsServiceGetTicketRequest{
		Id: ticketID,
	})
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("get ticket failed: %v", err)), nil
	}

	ticket, err := protoToMap(resp.GetTicket())
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to serialize ticket: %v", err)), nil
	}

	jsonBytes, err := json.Marshal(ticket)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("failed to marshal result: %v", err)), nil
	}

	return mcp.NewToolResultText(string(jsonBytes)), nil
}

// Helper functions.

func getPageSize(req mcp.CallToolRequest) uint32 {
	args := req.GetArguments()
	if args == nil {
		return defaultPageSize
	}
	if ps, ok := args["page_size"]; ok {
		if psFloat, ok := ps.(float64); ok {
			return uint32(psFloat)
		}
	}
	return defaultPageSize
}

func getStringArg(req mcp.CallToolRequest, name string) string {
	args := req.GetArguments()
	if args == nil {
		return ""
	}
	if v, ok := args[name]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
