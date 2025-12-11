package dotc1z

import (
	"context"
	"fmt"

	"github.com/doug-martin/goqu/v9"

	"github.com/conductorone/baton-sdk/pkg/annotations"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
)

const resourcesTableVersion = "1"
const resourcesTableName = "resources"
const resourcesTableSchema = `
create table if not exists %s (
    id integer primary key,
    resource_type_id text not null,
    external_id text not null,
    resource_id text,
	parent_resource_type_id text,
	parent_resource_id text,
    data blob not null,
    sync_id text not null,
    discovered_at datetime not null
);
create index if not exists %s on %s (resource_type_id);
create index if not exists %s on %s (parent_resource_type_id, parent_resource_id);
create unique index if not exists %s on %s (external_id, sync_id);
create index if not exists %s on %s (resource_type_id, resource_id, sync_id);`

var resources = (*resourcesTable)(nil)

type resourcesTable struct{}

func (r *resourcesTable) Name() string {
	return fmt.Sprintf("v%s_%s", r.Version(), resourcesTableName)
}

func (r *resourcesTable) Version() string {
	return resourcesTableVersion
}

func (r *resourcesTable) Schema() (string, []interface{}) {
	return resourcesTableSchema, []interface{}{
		r.Name(),
		fmt.Sprintf("idx_resources_resource_type_id_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_resources_parent_resource_id_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_resources_external_sync_v%s", r.Version()),
		r.Name(),
		fmt.Sprintf("idx_resources_type_res_sync_v%s", r.Version()),
		r.Name(),
	}
}

func (r *resourcesTable) Migrations(ctx context.Context, db *goqu.Database) error {
	// Check if resource_id column exists
	var resourceIdExists int
	err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM pragma_table_info('%s') WHERE name='resource_id'", r.Name())).Scan(&resourceIdExists)
	if err != nil {
		return err
	}
	if resourceIdExists == 0 {
		// For existing tables, we can't add a GENERATED column.
		// Add a regular column and populate it. New tables will use the GENERATED version from the schema.
		_, err = db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN resource_id TEXT", r.Name()))
		if err != nil {
			return err
		}
		// Populate from external_id (format: "type:id" -> extract "id" part)
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			"UPDATE %s SET resource_id = substr(external_id, instr(external_id, ':') + 1)",
			r.Name(),
		))
		if err != nil {
			return err
		}
		// Create index for efficient joins
		_, err = db.ExecContext(ctx, fmt.Sprintf(
			"CREATE INDEX IF NOT EXISTS idx_resources_type_res_sync_v%s ON %s (resource_type_id, resource_id, sync_id)",
			r.Version(), r.Name(),
		))
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *C1File) ListResources(ctx context.Context, request *v2.ResourcesServiceListResourcesRequest) (*v2.ResourcesServiceListResourcesResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.ListResources")
	defer span.End()

	resourceRows := make([]*v2.Resource, 0, 10000)
	ret, nextPageToken, err := listConnectorObjects(ctx, c, resources.Name(), request, func() *v2.Resource { return &v2.Resource{} }, resourceRows)
	if err != nil {
		return nil, fmt.Errorf("error listing resources: %w", err)
	}

	return v2.ResourcesServiceListResourcesResponse_builder{
		List:          ret,
		NextPageToken: nextPageToken,
	}.Build(), nil
}

func (c *C1File) GetResource(ctx context.Context, request *reader_v2.ResourcesReaderServiceGetResourceRequest) (*reader_v2.ResourcesReaderServiceGetResourceResponse, error) {
	ctx, span := tracer.Start(ctx, "C1File.GetResource")
	defer span.End()

	ret := &v2.Resource{}
	syncId, err := annotations.GetSyncIdFromAnnotations(request.GetAnnotations())
	if err != nil {
		return nil, fmt.Errorf("error getting sync id from annotations for resource '%s': %w", request.GetResourceId(), err)
	}
	err = c.getResourceObject(ctx, request.GetResourceId(), ret, syncId)
	if err != nil {
		return nil, fmt.Errorf("error fetching resource '%s': %w", request.GetResourceId(), err)
	}

	return reader_v2.ResourcesReaderServiceGetResourceResponse_builder{
		Resource: ret,
	}.Build(), nil
}

func (c *C1File) PutResources(ctx context.Context, resourceObjs ...*v2.Resource) error {
	ctx, span := tracer.Start(ctx, "C1File.PutResources")
	defer span.End()

	return c.putResourcesInternal(ctx, bulkPutConnectorObject, resourceObjs...)
}

func (c *C1File) PutResourcesIfNewer(ctx context.Context, resourceObjs ...*v2.Resource) error {
	ctx, span := tracer.Start(ctx, "C1File.PutResourcesIfNewer")
	defer span.End()

	return c.putResourcesInternal(ctx, bulkPutConnectorObjectIfNewer, resourceObjs...)
}

type resourcePutFunc func(context.Context, *C1File, string, func(m *v2.Resource) (goqu.Record, error), ...*v2.Resource) error

func (c *C1File) putResourcesInternal(ctx context.Context, f resourcePutFunc, resourceObjs ...*v2.Resource) error {
	err := f(ctx, c, resources.Name(),
		func(resource *v2.Resource) (goqu.Record, error) {
			fields := goqu.Record{
				"resource_type_id": resource.GetId().GetResourceType(),
				"resource_id":      resource.GetId().GetResource(),
				"external_id":      fmt.Sprintf("%s:%s", resource.GetId().GetResourceType(), resource.GetId().GetResource()),
			}

			// If we bulk insert some resources with parent ids and some without, goqu errors because of the different number of fields.
			if !resource.HasParentResourceId() {
				fields["parent_resource_type_id"] = nil
				fields["parent_resource_id"] = nil
			} else {
				fields["parent_resource_type_id"] = resource.GetParentResourceId().GetResourceType()
				fields["parent_resource_id"] = resource.GetParentResourceId().GetResource()
			}
			return fields, nil
		},
		resourceObjs...,
	)
	if err != nil {
		return err
	}
	c.dbUpdated = true
	return nil
}
