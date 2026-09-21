package c1zstore

const (
	LedgerFactReportOptions       = "c1z.report.latest_options"
	LedgerFactReportOptionsPrefix = "c1z.report.options."
)

type LedgerReportTarget struct {
	ResourceTypeID       string `json:"resource_type_id"`
	ResourceID           string `json:"resource_id"`
	ParentResourceTypeID string `json:"parent_resource_type_id"`
	ParentResourceID     string `json:"parent_resource_id"`
}

type LedgerReportOptions struct {
	Attempt                            string                 `json:"attempt"`
	Requested                          LedgerRequestedOptions `json:"requested"`
	EffectiveSkipGrants                bool                   `json:"effective_skip_grants"`
	EffectiveSkipEntitlementsAndGrants bool                   `json:"effective_skip_entitlements_and_grants"`
}

type LedgerRequestedOptions struct {
	SyncType                    string               `json:"sync_type"`
	ResourceTypes               []string             `json:"resource_types"`
	Targets                     []LedgerReportTarget `json:"targets"`
	WorkerCount                 int                  `json:"worker_count"`
	RunDurationMs               int64                `json:"run_duration_ms"`
	SkipFullSync                bool                 `json:"skip_full_sync"`
	SkipGrants                  bool                 `json:"skip_grants"`
	SkipEntitlementsAndGrants   bool                 `json:"skip_entitlements_and_grants"`
	OnlyExpandGrants            bool                 `json:"only_expand_grants"`
	DontExpandGrants            bool                 `json:"dont_expand_grants"`
	PreserveEntitlementGraph    bool                 `json:"preserve_entitlement_graph"`
	FailFastInvariants          bool                 `json:"fail_fast_invariants"`
	ExternalSourceConfigured    bool                 `json:"external_source_configured"`
	ExternalResourceTraits      []string             `json:"external_resource_traits"`
	ExternalEntitlementIDFilter string               `json:"external_entitlement_id_filter"`
	PreviousSourceConfigured    bool                 `json:"previous_source_configured"`
	PreviousSourceOptional      bool                 `json:"previous_source_optional"`
}
