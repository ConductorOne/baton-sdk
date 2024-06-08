package action

import "encoding/json"

const (
	UnknownOp ActionOp = iota
	InitOp
	SyncResourceTypesOp
	SyncResourcesOp
	SyncEntitlementsOp
	ListResourcesForEntitlementsOp
	SyncGrantsOp
	SyncAssetsOp
	SyncGrantExpansionOp
)

// Action stores the current operation, page token, and optional fields for which resource is being worked with.
type Action struct {
	Op                   ActionOp `json:"operation,omitempty"`
	PageToken            string   `json:"page_token,omitempty"`
	ResourceTypeID       string   `json:"resource_type_id,omitempty"`
	ResourceID           string   `json:"resource_id,omitempty"`
	ParentResourceTypeID string   `json:"parent_resource_type_id,omitempty"`
	ParentResourceID     string   `json:"parent_resource_id,omitempty"`
}

// ActionOp represents a sync operation.
type ActionOp uint8

// String() returns the string representation for an ActionOp. This is used for marshalling the op.
func (s ActionOp) String() string {
	switch s {
	case InitOp:
		return "init"
	case SyncResourceTypesOp:
		return "list-resource-types"
	case SyncResourcesOp:
		return "list-resources"
	case SyncEntitlementsOp:
		return "list-entitlements"
	case SyncGrantsOp:
		return "list-grants"
	case SyncAssetsOp:
		return "fetch-assets"
	case SyncGrantExpansionOp:
		return "grant-expansion"
	default:
		return "unknown"
	}
}

// MarshalJSON marshals the ActionOp into a json string.
func (s *ActionOp) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

// UnmarshalJSON unmarshals the input byte slice and updates this action op.
func (s *ActionOp) UnmarshalJSON(data []byte) error {
	var v string
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	*s = newActionOp(v)
	return nil
}

// newActionOp returns a new ActionOp given a string name. This is useful for unmarshalling.
func newActionOp(str string) ActionOp {
	switch str {
	case InitOp.String():
		return InitOp
	case SyncResourceTypesOp.String():
		return SyncResourceTypesOp
	case SyncResourcesOp.String():
		return SyncResourcesOp
	case SyncEntitlementsOp.String():
		return SyncEntitlementsOp
	case SyncGrantsOp.String():
		return SyncGrantsOp
	case SyncAssetsOp.String():
		return SyncAssetsOp
	case SyncGrantExpansionOp.String():
		return SyncGrantExpansionOp
	default:
		return UnknownOp
	}
}
