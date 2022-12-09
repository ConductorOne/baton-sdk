package entitlement

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

type EntitlementOption func(*v2.Entitlement)

func WithAnnotation(msgs ...proto.Message) EntitlementOption {
	return func(e *v2.Entitlement) {
		annos := annotations.Annotations(e.Annotations)
		for _, msg := range msgs {
			annos.Append(msg)
		}
		e.Annotations = annos
	}
}

func WithGrantableTo(grantableTo ...*v2.ResourceType) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.GrantableTo = grantableTo
	}
}

func WithDisplayName(displayName string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.DisplayName = displayName
	}
}

func WithDescriptionName(description string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.Description = description
	}
}
