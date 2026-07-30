package chaosconnector

// SurfaceStatus is the chaos connector's coverage state for one client RPC.
type SurfaceStatus string

const (
	SurfaceSupported SurfaceStatus = "supported"
	SurfaceExcluded  SurfaceStatus = "excluded"
)

// SurfaceCoverage is an explicit supported-or-excluded entry.
type SurfaceCoverage struct {
	Status SurfaceStatus
	Reason string
}

// ConnectorSurfaceCoverage is intentionally explicit. A new method on
// types.ConnectorClient fails the registry meta-test until it gets an entry.
func ConnectorSurfaceCoverage() map[string]SurfaceCoverage {
	supported := []string{
		"BulkCreateTickets",
		"BulkGetTickets",
		"Cleanup",
		"CreateAccount",
		"CreateResource",
		"CreateTicket",
		"DeleteResource",
		"DeleteResourceV2",
		"GetActionSchema",
		"GetActionStatus",
		"GetMetadata",
		"GetResource",
		"GetTicket",
		"GetTicketSchema",
		"Grant",
		"InvokeAction",
		"IssueCredential",
		"ListActionSchemas",
		"ListEntitlements",
		"ListEventFeeds",
		"ListEvents",
		"ListGrants",
		"ListResources",
		"ListResourceTypes",
		"ListStaticEntitlements",
		"ListTicketSchemas",
		"Revoke",
		"RotateCredential",
		"Validate",
	}
	out := make(map[string]SurfaceCoverage, len(supported)+1)
	for _, method := range supported {
		out[method] = SurfaceCoverage{Status: SurfaceSupported}
	}
	out["GetAsset"] = SurfaceCoverage{
		Status: SurfaceExcluded,
		Reason: "connectorbuilder asset streaming is disabled in pkg/connectorbuilder/assets.go",
	}
	return out
}
