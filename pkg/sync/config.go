package sync //nolint:revive,nolintlint // we can't change the package name for backwards compatibility

import (
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/metrics"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	"github.com/conductorone/baton-sdk/pkg/uotel"
)

// syncConfig is everything the caller decides before a sync starts: the
// complete set of values written by the exported With* options (SyncOpt)
// and by nothing else. NewSyncer applies the options and then never writes
// to it again, so a syncConfig read during Sync — including after a resume,
// on any worker goroutine — is the caller's original request.
//
// Fields that an option seeds but the sync then mutates are NOT config and
// stay on syncer: store (WithConnectorStore, replaced by loadStore) and
// syncID (WithSyncID, replaced by startOrResumeSync). Keeping them out
// makes the immutable set trustworthy, which matters because a sync
// resumes across process boundaries: mutable run state has to survive in
// the store, config only has to be passed again.
type syncConfig struct {
	c1zPath                             string
	externalResourceC1ZPath             string
	externalResourceEntitlementIdFilter string
	// externalResourceTraits are the resource type traits that this
	// connector wants synced from the external resource source and made
	// available to the External Identity Matcher (see externalMatchTraits,
	// set via WithExternalResourceTraits). When left empty the matcher
	// falls back to TRAIT_USER/TRAIT_GROUP, preserving pre-CE-975 behavior
	// for callers that never opt in.
	externalResourceTraits      []v2.ResourceType_Trait
	previousSyncC1ZPath         string
	previousSyncC1ZPathOptional bool
	// failFastInvariants promotes every ingestion-invariant verdict
	// (see ingest_invariants.go) to a hard, plainly-attributed sync
	// failure — tolerated warns fail — and enables I4 (skipped
	// entirely in default mode). Tests and equivalence harnesses set
	// it; production default follows the per-invariant policy in the
	// verdict table (ingestInvariants).
	failFastInvariants bool
	runDuration        time.Duration
	transitionHandler  func(s Action)
	progressHandler    func(p *Progress)
	tmpDir             string
	storageEngine      c1zstore.Engine
	skipFullSync       bool
	// compactionMergedStore marks the store as a pre-sealed artifact
	// this process did not collect (WithCompactionMergedStore — the
	// compactor's keep-newer merge and rollback-expansion's replay):
	// invariant verdicts attribute merge-manufactured shapes to the
	// merge and soften hard arms to aggregated warnings. Distinct from
	// onlyExpandGrants, which changes WHAT syncs and carries no
	// invariant policy on its own.
	compactionMergedStore      bool
	targetedSyncResources      []*v2.Resource
	onlyExpandGrants           bool
	preserveEntitlementGraph   bool
	dontExpandGrants           bool
	checkpointEntitlementGraph bool
	skipEntitlementsAndGrants  bool
	skipGrants                 bool
	syncType                   connectorstore.SyncType
	setSessionStore            sessions.SetSessionStore
	syncResourceTypes          []string
	workerCount                int // If 1, sync is sequential (default). If > 1, sync operations are done in parallel.
	metricsHandler             metrics.Handler
	syncIdentity               uotel.SyncIdentity
}
