package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"google.golang.org/protobuf/proto"
)

const staticMaterializationPageSize = 10000

type staticMaterializationCursor struct {
	Version        int    `json:"version"`
	Origin         []byte `json:"origin"`
	Ordinal        int    `json:"ordinal"`
	Template       []byte `json:"template"`
	ResourceCursor string `json:"resource_cursor,omitempty"`
}

func (c staticMaterializationCursor) encode() (string, error) {
	data, err := json.Marshal(c)
	return string(data), err
}

func (s *syncer) materializeLedgerStaticEntitlements(ctx context.Context, action *Action) error {
	invocation, ok := ctx.Value(ledgerInvocationKey{}).(*ledgerInvocation)
	if !ok {
		return errors.New("static materialization requires an open ledger page")
	}
	started := time.Now()
	defer func() { invocation.page.row.PageDuration = time.Since(started) }()
	var cursor staticMaterializationCursor
	if err := json.Unmarshal([]byte(action.PageToken), &cursor); err != nil {
		return err
	}
	if cursor.Version != 1 || len(cursor.Origin) != sha256.Size || cursor.Ordinal < 0 || action.ResourceTypeID == "" {
		return errors.New("invalid static materialization cursor")
	}
	template := &v2.Entitlement{}
	if err := proto.Unmarshal(cursor.Template, template); err != nil {
		return err
	}
	response, err := s.store.ListResources(ctx, v2.ResourcesServiceListResourcesRequest_builder{
		ResourceTypeId: action.ResourceTypeID, PageToken: cursor.ResourceCursor, PageSize: staticMaterializationPageSize, ActiveSyncId: s.getActiveSyncID(),
	}.Build())
	if err != nil {
		return err
	}
	if len(response.GetList()) > staticMaterializationPageSize {
		return errors.New("static resource page exceeds requested size")
	}
	next := response.GetNextPageToken()
	if next != "" && next == cursor.ResourceCursor {
		return errors.New("static resource cursor did not advance")
	}
	annos := annotations.Annotations(template.GetAnnotations())
	group := &v2.EntitlementExclusionGroup{}
	scoped, err := annos.Pick(group)
	if err != nil {
		return err
	}
	groupID := group.GetExclusionGroupId()
	values := make([]*v2.Entitlement, 0, len(response.GetList()))
	for _, resource := range response.GetList() {
		display := template.GetDisplayName()
		if display == "" {
			display = resource.GetDisplayName()
		}
		description := template.GetDescription()
		if description == "" {
			description = resource.GetDescription()
		}
		if scoped && group.GetScopeToResource() {
			group.SetExclusionGroupId(groupID + "-" + resource.GetId().GetResource())
			annos.Update(group)
		}
		values = append(values, &v2.Entitlement{Resource: resource, Id: entitlement.NewEntitlementID(resource, template.GetSlug()), DisplayName: display,
			Description: description, GrantableTo: template.GetGrantableTo(), Annotations: annos, Slug: template.GetSlug(), Purpose: template.GetPurpose()})
	}
	if err := invocation.page.writer.PutEntitlements(ctx, values...); err != nil {
		return err
	}
	token := ""
	if next != "" {
		cursor.ResourceCursor = next
		token, err = cursor.encode()
		if err != nil {
			return err
		}
	}
	return s.nextPageOrFinishAction(ctx, action, token)
}
