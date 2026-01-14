package mcp

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

const defaultPageSize = 50

// Input/Output types for handlers.

type EmptyInput struct{}

type PaginationInput struct {
	PageSize  int    `json:"page_size,omitempty" jsonschema:"description=Number of items per page (default 50)"`
	PageToken string `json:"page_token,omitempty" jsonschema:"description=Pagination token from previous response"`
}

type ResourceInput struct {
	ResourceType string `json:"resource_type" jsonschema:"required,description=The resource type (e.g. user or group)"`
	ResourceID   string `json:"resource_id" jsonschema:"required,description=The resource ID"`
}

type ResourcePaginationInput struct {
	ResourceType string `json:"resource_type" jsonschema:"required,description=The resource type"`
	ResourceID   string `json:"resource_id" jsonschema:"required,description=The resource ID"`
	PageSize     int    `json:"page_size,omitempty" jsonschema:"description=Number of items per page (default 50)"`
	PageToken    string `json:"page_token,omitempty" jsonschema:"description=Pagination token from previous response"`
}

type ListResourcesInput struct {
	ResourceTypeID     string `json:"resource_type_id" jsonschema:"required,description=The resource type ID to list (e.g. user or group)"`
	ParentResourceType string `json:"parent_resource_type,omitempty" jsonschema:"description=Parent resource type (optional)"`
	ParentResourceID   string `json:"parent_resource_id,omitempty" jsonschema:"description=Parent resource ID (optional)"`
	PageSize           int    `json:"page_size,omitempty" jsonschema:"description=Number of items per page (default 50)"`
	PageToken          string `json:"page_token,omitempty" jsonschema:"description=Pagination token from previous response"`
}

type GrantInput struct {
	EntitlementResourceType string `json:"entitlement_resource_type" jsonschema:"required,description=Resource type of the entitlement"`
	EntitlementResourceID   string `json:"entitlement_resource_id" jsonschema:"required,description=Resource ID of the entitlement"`
	EntitlementID           string `json:"entitlement_id" jsonschema:"required,description=The entitlement ID"`
	PrincipalResourceType   string `json:"principal_resource_type" jsonschema:"required,description=Resource type of the principal (e.g. user or group)"`
	PrincipalResourceID     string `json:"principal_resource_id" jsonschema:"required,description=Resource ID of the principal"`
}

type RevokeInput struct {
	GrantID                 string `json:"grant_id" jsonschema:"required,description=The grant ID to revoke"`
	EntitlementResourceType string `json:"entitlement_resource_type" jsonschema:"required,description=Resource type of the entitlement"`
	EntitlementResourceID   string `json:"entitlement_resource_id" jsonschema:"required,description=Resource ID of the entitlement"`
	EntitlementID           string `json:"entitlement_id" jsonschema:"required,description=The entitlement ID"`
	PrincipalResourceType   string `json:"principal_resource_type" jsonschema:"required,description=Resource type of the principal"`
	PrincipalResourceID     string `json:"principal_resource_id" jsonschema:"required,description=Resource ID of the principal"`
}

type CreateResourceInput struct {
	ResourceType       string `json:"resource_type" jsonschema:"required,description=The resource type to create"`
	DisplayName        string `json:"display_name" jsonschema:"required,description=Display name for the new resource"`
	ParentResourceType string `json:"parent_resource_type,omitempty" jsonschema:"description=Parent resource type (optional)"`
	ParentResourceID   string `json:"parent_resource_id,omitempty" jsonschema:"description=Parent resource ID (optional)"`
}

type DeleteResourceInput struct {
	ResourceType string `json:"resource_type" jsonschema:"required,description=The resource type"`
	ResourceID   string `json:"resource_id" jsonschema:"required,description=The resource ID to delete"`
}

type CreateTicketInput struct {
	SchemaID    string `json:"schema_id" jsonschema:"required,description=The ticket schema ID"`
	DisplayName string `json:"display_name" jsonschema:"required,description=Display name for the ticket"`
	Description string `json:"description,omitempty" jsonschema:"description=Description of the ticket"`
}

type GetTicketInput struct {
	TicketID string `json:"ticket_id" jsonschema:"required,description=The ticket ID"`
}

// Output types.

type MetadataOutput struct {
	Metadata map[string]any `json:"metadata"`
}

type ValidateOutput struct {
	Valid       bool `json:"valid"`
	Annotations any  `json:"annotations,omitempty"`
}

type ListResourceTypesOutput struct {
	ResourceTypes []map[string]any `json:"resource_types"`
	NextPageToken string           `json:"next_page_token,omitempty"`
	HasMore       bool             `json:"has_more"`
}

type ListResourcesOutput struct {
	Resources     []map[string]any `json:"resources"`
	NextPageToken string           `json:"next_page_token,omitempty"`
	HasMore       bool             `json:"has_more"`
}

type ResourceOutput struct {
	Resource map[string]any `json:"resource"`
}

type ListEntitlementsOutput struct {
	Entitlements  []map[string]any `json:"entitlements"`
	NextPageToken string           `json:"next_page_token,omitempty"`
	HasMore       bool             `json:"has_more"`
}

type ListGrantsOutput struct {
	Grants        []map[string]any `json:"grants"`
	NextPageToken string           `json:"next_page_token,omitempty"`
	HasMore       bool             `json:"has_more"`
}

type GrantOutput struct {
	Grants []map[string]any `json:"grants"`
}

type SuccessOutput struct {
	Success bool `json:"success"`
}

type ListTicketSchemasOutput struct {
	Schemas       []map[string]any `json:"schemas"`
	NextPageToken string           `json:"next_page_token,omitempty"`
	HasMore       bool             `json:"has_more"`
}

type TicketOutput struct {
	Ticket map[string]any `json:"ticket"`
}

// Handler implementations.

func (m *MCPServer) handleGetMetadata(ctx context.Context, req *mcp.CallToolRequest, input EmptyInput) (*mcp.CallToolResult, MetadataOutput, error) {
	resp, err := m.connector.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return nil, MetadataOutput{}, fmt.Errorf("failed to get metadata: %w", err)
	}

	result, err := protoToMap(resp.GetMetadata())
	if err != nil {
		return nil, MetadataOutput{}, fmt.Errorf("failed to serialize metadata: %w", err)
	}

	return nil, MetadataOutput{Metadata: result}, nil
}

func (m *MCPServer) handleValidate(ctx context.Context, req *mcp.CallToolRequest, input EmptyInput) (*mcp.CallToolResult, ValidateOutput, error) {
	resp, err := m.connector.Validate(ctx, &v2.ConnectorServiceValidateRequest{})
	if err != nil {
		return nil, ValidateOutput{}, fmt.Errorf("validation failed: %w", err)
	}

	return nil, ValidateOutput{
		Valid:       true,
		Annotations: resp.GetAnnotations(),
	}, nil
}

func (m *MCPServer) handleListResourceTypes(ctx context.Context, req *mcp.CallToolRequest, input PaginationInput) (*mcp.CallToolResult, ListResourceTypesOutput, error) {
	pageSize := uint32(defaultPageSize)
	if input.PageSize > 0 {
		pageSize = uint32(input.PageSize)
	}

	resp, err := m.connector.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{
		PageSize:  pageSize,
		PageToken: input.PageToken,
	})
	if err != nil {
		return nil, ListResourceTypesOutput{}, fmt.Errorf("failed to list resource types: %w", err)
	}

	resourceTypes, err := protoListToMaps(resp.GetList())
	if err != nil {
		return nil, ListResourceTypesOutput{}, fmt.Errorf("failed to serialize resource types: %w", err)
	}

	return nil, ListResourceTypesOutput{
		ResourceTypes: resourceTypes,
		NextPageToken: resp.GetNextPageToken(),
		HasMore:       resp.GetNextPageToken() != "",
	}, nil
}

func (m *MCPServer) handleListResources(ctx context.Context, req *mcp.CallToolRequest, input ListResourcesInput) (*mcp.CallToolResult, ListResourcesOutput, error) {
	pageSize := uint32(defaultPageSize)
	if input.PageSize > 0 {
		pageSize = uint32(input.PageSize)
	}

	var parentResourceID *v2.ResourceId
	if input.ParentResourceType != "" && input.ParentResourceID != "" {
		parentResourceID = &v2.ResourceId{
			ResourceType: input.ParentResourceType,
			Resource:     input.ParentResourceID,
		}
	}

	resp, err := m.connector.ListResources(ctx, &v2.ResourcesServiceListResourcesRequest{
		ResourceTypeId:   input.ResourceTypeID,
		ParentResourceId: parentResourceID,
		PageSize:         pageSize,
		PageToken:        input.PageToken,
	})
	if err != nil {
		return nil, ListResourcesOutput{}, fmt.Errorf("failed to list resources: %w", err)
	}

	resources, err := protoListToMaps(resp.GetList())
	if err != nil {
		return nil, ListResourcesOutput{}, fmt.Errorf("failed to serialize resources: %w", err)
	}

	return nil, ListResourcesOutput{
		Resources:     resources,
		NextPageToken: resp.GetNextPageToken(),
		HasMore:       resp.GetNextPageToken() != "",
	}, nil
}

func (m *MCPServer) handleGetResource(ctx context.Context, req *mcp.CallToolRequest, input ResourceInput) (*mcp.CallToolResult, ResourceOutput, error) {
	resp, err := m.connector.GetResource(ctx, &v2.ResourceGetterServiceGetResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: input.ResourceType,
			Resource:     input.ResourceID,
		},
	})
	if err != nil {
		return nil, ResourceOutput{}, fmt.Errorf("failed to get resource: %w", err)
	}

	resource, err := protoToMap(resp.GetResource())
	if err != nil {
		return nil, ResourceOutput{}, fmt.Errorf("failed to serialize resource: %w", err)
	}

	return nil, ResourceOutput{Resource: resource}, nil
}

func (m *MCPServer) handleListEntitlements(ctx context.Context, req *mcp.CallToolRequest, input ResourcePaginationInput) (*mcp.CallToolResult, ListEntitlementsOutput, error) {
	pageSize := uint32(defaultPageSize)
	if input.PageSize > 0 {
		pageSize = uint32(input.PageSize)
	}

	resp, err := m.connector.ListEntitlements(ctx, &v2.EntitlementsServiceListEntitlementsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: input.ResourceType,
				Resource:     input.ResourceID,
			},
		},
		PageSize:  pageSize,
		PageToken: input.PageToken,
	})
	if err != nil {
		return nil, ListEntitlementsOutput{}, fmt.Errorf("failed to list entitlements: %w", err)
	}

	entitlements, err := protoListToMaps(resp.GetList())
	if err != nil {
		return nil, ListEntitlementsOutput{}, fmt.Errorf("failed to serialize entitlements: %w", err)
	}

	return nil, ListEntitlementsOutput{
		Entitlements:  entitlements,
		NextPageToken: resp.GetNextPageToken(),
		HasMore:       resp.GetNextPageToken() != "",
	}, nil
}

func (m *MCPServer) handleListGrants(ctx context.Context, req *mcp.CallToolRequest, input ResourcePaginationInput) (*mcp.CallToolResult, ListGrantsOutput, error) {
	pageSize := uint32(defaultPageSize)
	if input.PageSize > 0 {
		pageSize = uint32(input.PageSize)
	}

	resp, err := m.connector.ListGrants(ctx, &v2.GrantsServiceListGrantsRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: input.ResourceType,
				Resource:     input.ResourceID,
			},
		},
		PageSize:  pageSize,
		PageToken: input.PageToken,
	})
	if err != nil {
		return nil, ListGrantsOutput{}, fmt.Errorf("failed to list grants: %w", err)
	}

	grants, err := protoListToMaps(resp.GetList())
	if err != nil {
		return nil, ListGrantsOutput{}, fmt.Errorf("failed to serialize grants: %w", err)
	}

	return nil, ListGrantsOutput{
		Grants:        grants,
		NextPageToken: resp.GetNextPageToken(),
		HasMore:       resp.GetNextPageToken() != "",
	}, nil
}

func (m *MCPServer) handleGrant(ctx context.Context, req *mcp.CallToolRequest, input GrantInput) (*mcp.CallToolResult, GrantOutput, error) {
	resp, err := m.connector.Grant(ctx, &v2.GrantManagerServiceGrantRequest{
		Entitlement: &v2.Entitlement{
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: input.EntitlementResourceType,
					Resource:     input.EntitlementResourceID,
				},
			},
			Id: input.EntitlementID,
		},
		Principal: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: input.PrincipalResourceType,
				Resource:     input.PrincipalResourceID,
			},
		},
	})
	if err != nil {
		return nil, GrantOutput{}, fmt.Errorf("grant failed: %w", err)
	}

	grants, err := protoListToMaps(resp.GetGrants())
	if err != nil {
		return nil, GrantOutput{}, fmt.Errorf("failed to serialize grants: %w", err)
	}

	return nil, GrantOutput{Grants: grants}, nil
}

func (m *MCPServer) handleRevoke(ctx context.Context, req *mcp.CallToolRequest, input RevokeInput) (*mcp.CallToolResult, SuccessOutput, error) {
	_, err := m.connector.Revoke(ctx, &v2.GrantManagerServiceRevokeRequest{
		Grant: &v2.Grant{
			Id: input.GrantID,
			Entitlement: &v2.Entitlement{
				Resource: &v2.Resource{
					Id: &v2.ResourceId{
						ResourceType: input.EntitlementResourceType,
						Resource:     input.EntitlementResourceID,
					},
				},
				Id: input.EntitlementID,
			},
			Principal: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: input.PrincipalResourceType,
					Resource:     input.PrincipalResourceID,
				},
			},
		},
	})
	if err != nil {
		return nil, SuccessOutput{}, fmt.Errorf("revoke failed: %w", err)
	}

	return nil, SuccessOutput{Success: true}, nil
}

func (m *MCPServer) handleCreateResource(ctx context.Context, req *mcp.CallToolRequest, input CreateResourceInput) (*mcp.CallToolResult, ResourceOutput, error) {
	var parentResource *v2.Resource
	if input.ParentResourceType != "" && input.ParentResourceID != "" {
		parentResource = &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: input.ParentResourceType,
				Resource:     input.ParentResourceID,
			},
		}
	}

	resp, err := m.connector.CreateResource(ctx, &v2.CreateResourceRequest{
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: input.ResourceType,
			},
			DisplayName:      input.DisplayName,
			ParentResourceId: parentResource.GetId(),
		},
	})
	if err != nil {
		return nil, ResourceOutput{}, fmt.Errorf("create resource failed: %w", err)
	}

	resource, err := protoToMap(resp.GetCreated())
	if err != nil {
		return nil, ResourceOutput{}, fmt.Errorf("failed to serialize resource: %w", err)
	}

	return nil, ResourceOutput{Resource: resource}, nil
}

func (m *MCPServer) handleDeleteResource(ctx context.Context, req *mcp.CallToolRequest, input DeleteResourceInput) (*mcp.CallToolResult, SuccessOutput, error) {
	_, err := m.connector.DeleteResource(ctx, &v2.DeleteResourceRequest{
		ResourceId: &v2.ResourceId{
			ResourceType: input.ResourceType,
			Resource:     input.ResourceID,
		},
	})
	if err != nil {
		return nil, SuccessOutput{}, fmt.Errorf("delete resource failed: %w", err)
	}

	return nil, SuccessOutput{Success: true}, nil
}

func (m *MCPServer) handleListTicketSchemas(ctx context.Context, req *mcp.CallToolRequest, input EmptyInput) (*mcp.CallToolResult, ListTicketSchemasOutput, error) {
	resp, err := m.connector.ListTicketSchemas(ctx, &v2.TicketsServiceListTicketSchemasRequest{})
	if err != nil {
		return nil, ListTicketSchemasOutput{}, fmt.Errorf("failed to list ticket schemas: %w", err)
	}

	schemas, err := protoListToMaps(resp.GetList())
	if err != nil {
		return nil, ListTicketSchemasOutput{}, fmt.Errorf("failed to serialize ticket schemas: %w", err)
	}

	return nil, ListTicketSchemasOutput{
		Schemas:       schemas,
		NextPageToken: resp.GetNextPageToken(),
		HasMore:       resp.GetNextPageToken() != "",
	}, nil
}

func (m *MCPServer) handleCreateTicket(ctx context.Context, req *mcp.CallToolRequest, input CreateTicketInput) (*mcp.CallToolResult, TicketOutput, error) {
	resp, err := m.connector.CreateTicket(ctx, &v2.TicketsServiceCreateTicketRequest{
		Schema: &v2.TicketSchema{
			Id: input.SchemaID,
		},
		Request: &v2.TicketRequest{
			DisplayName: input.DisplayName,
			Description: input.Description,
		},
	})
	if err != nil {
		return nil, TicketOutput{}, fmt.Errorf("create ticket failed: %w", err)
	}

	ticket, err := protoToMap(resp.GetTicket())
	if err != nil {
		return nil, TicketOutput{}, fmt.Errorf("failed to serialize ticket: %w", err)
	}

	return nil, TicketOutput{Ticket: ticket}, nil
}

func (m *MCPServer) handleGetTicket(ctx context.Context, req *mcp.CallToolRequest, input GetTicketInput) (*mcp.CallToolResult, TicketOutput, error) {
	resp, err := m.connector.GetTicket(ctx, &v2.TicketsServiceGetTicketRequest{
		Id: input.TicketID,
	})
	if err != nil {
		return nil, TicketOutput{}, fmt.Errorf("get ticket failed: %w", err)
	}

	ticket, err := protoToMap(resp.GetTicket())
	if err != nil {
		return nil, TicketOutput{}, fmt.Errorf("failed to serialize ticket: %w", err)
	}

	return nil, TicketOutput{Ticket: ticket}, nil
}
