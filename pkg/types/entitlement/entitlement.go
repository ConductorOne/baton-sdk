package entitlement

import (
	"errors"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/proto"
)

const (
	EntitlementKindSDK    = "sdk"
	EntitlementKindCustom = "custom"
)

var (
	ErrInvalidEntitlementID = errors.New("invalid entitlement id")
	ErrInvalidEscapedID     = errors.New("invalid escaped id")
)

type EntitlementIDParts struct {
	ResourceTypeID string
	ResourceID     string
	Kind           string
	Name           string
}

type EntitlementOption func(*v2.Entitlement)

func WithAnnotation(msgs ...proto.Message) EntitlementOption {
	return func(e *v2.Entitlement) {
		annos := annotations.Annotations(e.GetAnnotations())
		for _, msg := range msgs {
			annos.Append(msg)
		}
		e.SetAnnotations(annos)
	}
}

func WithGrantableTo(grantableTo ...*v2.ResourceType) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetGrantableTo(grantableTo)
	}
}

func WithDisplayName(displayName string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetDisplayName(displayName)
	}
}

func WithSlug(slug string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetSlug(slug)
	}
}

func WithDescription(description string) EntitlementOption {
	return func(g *v2.Entitlement) {
		g.SetDescription(description)
	}
}

// WithExclusionGroup marks this entitlement as belonging to a mutually exclusive
// group. Two entitlements on the same resource with the same exclusionGroupID
// cannot be simultaneously granted to the same principal.
func WithExclusionGroup(exclusionGroupID string) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
	})
}

// WithExclusionGroupOrder is like WithExclusionGroup but also sets an ordering
// hint. Higher order values indicate higher privilege levels.
func WithExclusionGroupOrder(exclusionGroupID string, order uint32) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
		Order:            order,
	})
}

// WithExclusionGroupDefault marks this entitlement as the default for a mandatory
// exclusion group. When another entitlement in the group is revoked without an
// explicit replacement, this entitlement is granted as the fallback.
//
// Setting is_default on any entitlement in a group signals to ConductorOne that
// the group is mandatory (a principal must always hold exactly one).
func WithExclusionGroupDefault(exclusionGroupID string, order uint32) EntitlementOption {
	return WithAnnotation(&v2.EntitlementExclusionGroup{
		ExclusionGroupId: exclusionGroupID,
		Order:            order,
		IsDefault:        true,
	})
}

func NewEntitlementID(resource *v2.Resource, permission string) string {
	return EncodeEntitlementID(resource.GetId().GetResourceType(), resource.GetId().GetResource(), EntitlementKindSDK, permission)
}

func EncodeEntitlementID(resourceTypeID, resourceID, kind, name string) string {
	if kind == EntitlementKindCustom {
		return JoinEscapedID(resourceTypeID, resourceID, EntitlementKindCustom, name)
	}
	return JoinEscapedID(resourceTypeID, resourceID, name)
}

func DecodeEntitlementID(id string) (EntitlementIDParts, error) {
	parts, err := SplitEscapedID(id)
	if err != nil {
		return EntitlementIDParts{}, err
	}
	switch {
	case len(parts) == 3:
		return EntitlementIDParts{
			ResourceTypeID: parts[0],
			ResourceID:     parts[1],
			Kind:           EntitlementKindSDK,
			Name:           parts[2],
		}, nil
	case len(parts) == 4 && parts[2] == EntitlementKindCustom:
		return EntitlementIDParts{
			ResourceTypeID: parts[0],
			ResourceID:     parts[1],
			Kind:           EntitlementKindCustom,
			Name:           parts[3],
		}, nil
	default:
		return EntitlementIDParts{}, ErrInvalidEntitlementID
	}
}

func DeriveEntitlementIDParts(resourceTypeID, resourceID, entitlementID string) EntitlementIDParts {
	// Hot path for expansion: entitlement ids are commonly canonical strings for
	// resource ids without escaped delimiters. Avoid the generic split/decode path
	// for every synthesized grant.
	prefix := resourceTypeID + ":" + resourceID + ":"
	if strings.HasPrefix(entitlementID, prefix) {
		tail := entitlementID[len(prefix):]
		if strings.HasPrefix(tail, EntitlementKindCustom+":") {
			name, ok := unescapeIDPart(tail[len(EntitlementKindCustom)+1:])
			if ok {
				return EntitlementIDParts{
					ResourceTypeID: resourceTypeID,
					ResourceID:     resourceID,
					Kind:           EntitlementKindCustom,
					Name:           name,
				}
			}
		} else {
			name, ok := unescapeIDPart(tail)
			if ok {
				return EntitlementIDParts{
					ResourceTypeID: resourceTypeID,
					ResourceID:     resourceID,
					Kind:           EntitlementKindSDK,
					Name:           name,
				}
			}
		}
	}

	if parts, err := DecodeEntitlementID(entitlementID); err == nil &&
		parts.ResourceTypeID == resourceTypeID &&
		parts.ResourceID == resourceID {
		return parts
	}

	return DeriveLegacyEntitlementIDParts(resourceTypeID, resourceID, entitlementID)
}

func DeriveLegacyEntitlementIDParts(resourceTypeID, resourceID, entitlementID string) EntitlementIDParts {
	prefix := resourceTypeID + ":" + resourceID + ":"
	if strings.HasPrefix(entitlementID, prefix) {
		return EntitlementIDParts{
			ResourceTypeID: resourceTypeID,
			ResourceID:     resourceID,
			Kind:           EntitlementKindSDK,
			Name:           strings.TrimPrefix(entitlementID, prefix),
		}
	}

	return EntitlementIDParts{
		ResourceTypeID: resourceTypeID,
		ResourceID:     resourceID,
		Kind:           EntitlementKindCustom,
		Name:           entitlementID,
	}
}

func (p EntitlementIDParts) Encode() string {
	return EncodeEntitlementID(p.ResourceTypeID, p.ResourceID, p.Kind, p.Name)
}

func JoinEscapedID(parts ...string) string {
	needsEscape := false
	totalLen := max(0, len(parts)-1)
	for _, part := range parts {
		totalLen += len(part)
		if strings.ContainsAny(part, `:\`) {
			needsEscape = true
		}
	}
	if !needsEscape {
		return strings.Join(parts, ":")
	}
	var b strings.Builder
	b.Grow(totalLen)
	for i, part := range parts {
		if i > 0 {
			writeBuilderByte(&b, ':')
		}
		writeEscapedIDPart(&b, part)
	}
	return b.String()
}

func SplitEscapedID(id string) ([]string, error) {
	if !strings.Contains(id, `\`) {
		return splitUnescapedID(id), nil
	}
	parts := make([]string, 0, strings.Count(id, ":")+1)
	var b strings.Builder
	b.Grow(len(id))
	escaped := false
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case escaped:
			if c != '\\' && c != ':' {
				return nil, ErrInvalidEscapedID
			}
			writeBuilderByte(&b, c)
			escaped = false
		case c == '\\':
			escaped = true
		case c == ':':
			parts = append(parts, b.String())
			b.Reset()
		default:
			writeBuilderByte(&b, c)
		}
	}
	if escaped {
		return nil, ErrInvalidEscapedID
	}
	parts = append(parts, b.String())
	return parts, nil
}

func writeEscapedIDPart(b *strings.Builder, part string) {
	start := 0
	for i := 0; i < len(part); i++ {
		if part[i] != '\\' && part[i] != ':' {
			continue
		}
		writeBuilderString(b, part[start:i])
		writeBuilderByte(b, '\\')
		writeBuilderByte(b, part[i])
		start = i + 1
	}
	writeBuilderString(b, part[start:])
}

func splitUnescapedID(id string) []string {
	parts := make([]string, 0, strings.Count(id, ":")+1)
	start := 0
	for {
		i := strings.IndexByte(id[start:], ':')
		if i < 0 {
			parts = append(parts, id[start:])
			return parts
		}
		i += start
		parts = append(parts, id[start:i])
		start = i + 1
	}
}

func unescapeIDPart(part string) (string, bool) {
	if !strings.Contains(part, `\`) {
		return part, true
	}
	var b strings.Builder
	b.Grow(len(part))
	escaped := false
	for i := 0; i < len(part); i++ {
		c := part[i]
		switch {
		case escaped:
			if c != '\\' && c != ':' {
				return "", false
			}
			writeBuilderByte(&b, c)
			escaped = false
		case c == '\\':
			escaped = true
		default:
			writeBuilderByte(&b, c)
		}
	}
	if escaped {
		return "", false
	}
	return b.String(), true
}

func writeBuilderByte(b *strings.Builder, c byte) {
	_ = b.WriteByte(c)
}

func writeBuilderString(b *strings.Builder, s string) {
	_, _ = b.WriteString(s)
}

func NewPermissionEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_PERMISSION,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewAssignmentEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewOwnershipEntitlement(resource *v2.Resource, name string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     v2.Entitlement_PURPOSE_VALUE_OWNERSHIP,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}

func NewEntitlement(resource *v2.Resource, name, purposeStr string, entitlementOptions ...EntitlementOption) *v2.Entitlement {
	var purpose v2.Entitlement_PurposeValue
	switch purposeStr {
	case "permission":
		purpose = v2.Entitlement_PURPOSE_VALUE_PERMISSION
	case "assignment":
		purpose = v2.Entitlement_PURPOSE_VALUE_ASSIGNMENT
	case "ownership":
		purpose = v2.Entitlement_PURPOSE_VALUE_OWNERSHIP
	default:
		purpose = v2.Entitlement_PURPOSE_VALUE_UNSPECIFIED
	}

	entitlement := v2.Entitlement_builder{
		Id:          NewEntitlementID(resource, name),
		DisplayName: name,
		Slug:        name,
		Purpose:     purpose,
		Resource:    resource,
	}.Build()

	for _, entitlementOption := range entitlementOptions {
		entitlementOption(entitlement)
	}
	return entitlement
}
