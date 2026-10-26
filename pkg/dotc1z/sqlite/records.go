package sqlite

import (
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func localizeSQLiteTimestamp(t time.Time, loc *time.Location) time.Time {
	if t.IsZero() || t.Location() != time.UTC {
		return t
	}
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc)
}

func localizeSQLiteTimestampPtr(t *time.Time, loc *time.Location) *time.Time {
	if t == nil {
		return nil
	}
	localized := localizeSQLiteTimestamp(*t, loc)
	return &localized
}

func discoveredAtTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(localizeSQLiteTimestamp(t, time.Local))
}

func reattachExpansion(grant *v2.Grant, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	expandable := &v2.GrantExpandable{}
	if err := proto.Unmarshal(data, expandable); err != nil {
		return err
	}
	grantAnnotations := annotations.Annotations(grant.GetAnnotations())
	grantAnnotations.Update(expandable)
	grant.SetAnnotations(grantAnnotations)
	return nil
}

type grantIdentity struct {
	entitlementID             string
	entitlementResourceTypeID string
	entitlementResourceID     string
	principalResourceTypeID   string
	principalResourceID       string
}

func hydrateGrant(grant *v2.Grant, identity grantIdentity) {
	if grant.GetEntitlement() == nil {
		grant.SetEntitlement(v2.Entitlement_builder{
			Id: identity.entitlementID,
			Resource: v2.Resource_builder{
				Id: v2.ResourceId_builder{
					ResourceType: identity.entitlementResourceTypeID,
					Resource:     identity.entitlementResourceID,
				}.Build(),
			}.Build(),
		}.Build())
	}
	if grant.GetPrincipal() == nil {
		grant.SetPrincipal(v2.Resource_builder{
			Id: v2.ResourceId_builder{
				ResourceType: identity.principalResourceTypeID,
				Resource:     identity.principalResourceID,
			}.Build(),
		}.Build())
	}
}
