package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/conductorone/baton-sdk/pkg/sourcecache"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
)

// source-cache: the attribution surface for source-cache replay. Rows in
// a c1z are stamped with the scope that produced them and the manifest
// records each scope's validator + write time — this command joins the
// two so "why is this row missing / stale?" becomes a per-scope answer:
// when was the scope last actually fetched, what validator did it record,
// how many rows does it hold, and is its index/manifest state consistent.
//
// Exits non-zero when any orphan-index scope is found (ingestion
// invariant I6) on a file that is ELIGIBLE as a replay source: such a
// file would poison a future sync's replay. Files the metadata gate
// already refuses (compaction folds — which legitimately clear the
// manifest while keeping scope stamps — and partial/derived syncs) can
// never be replayed from, so their index/manifest divergence is reported
// as "stale-index" without failing the audit.
func sourceCacheCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "source-cache",
		Short: "Audit the C1Z's source-cache replay state (scopes, validators, row counts, consistency)",
		RunE:  runSourceCacheAudit,
	}
	return cmd
}

type sourceCacheScopeReport struct {
	RowKind      string    `json:"row_kind"`
	ScopeKey     string    `json:"scope_key"`
	Rows         int       `json:"rows"`
	Validator    string    `json:"cache_validator,omitempty"`
	DiscoveredAt time.Time `json:"discovered_at,omitempty"`
	// Status: "ok" (manifest + rows), "zero-row" (manifest entry with no
	// stamped rows — legal: the scope legitimately returned nothing),
	// "invalidated" (the dangling-reference drops removed rows from this
	// scope, so its entry is marked to miss and re-fetch cold next sync —
	// legal, informational), "orphan-index" (stamped rows with NO
	// manifest entry on a replay-eligible file — an invariant violation
	// that would poison the next sync's replay; see ingestion invariant
	// I6), or "stale-index" (the same shape on a file the replay metadata
	// gate already refuses, e.g. a compaction fold whose manifest was
	// deliberately cleared — informational, never a failure).
	Status string `json:"status"`
}

type sourceCacheAuditReport struct {
	SyncID string `json:"sync_id,omitempty"`
	// SyncType / Compacted / UsableAsReplaySource describe whether a
	// FUTURE sync may use this file as its replay source (the metadata
	// gate; see c1zstore.SyncRun.UsableAsReplaySource, plus the
	// compaction-provenance token check the syncer applies for artifacts
	// produced by pre-compacted-flag compactors).
	SyncType             string                   `json:"sync_type,omitempty"`
	Compacted            bool                     `json:"compacted"`
	UsableAsReplaySource bool                     `json:"usable_as_replay_source"`
	ReplayUnusableReason string                   `json:"replay_unusable_reason,omitempty"`
	Scopes               []sourceCacheScopeReport `json:"scopes"`
	OrphanIndexScopes    int                      `json:"orphan_index_scopes"`
	StaleIndexScopes     int                      `json:"stale_index_scopes,omitempty"`
}

func runSourceCacheAudit(cmd *cobra.Command, args []string) error {
	ctx, err := logging.Init(context.Background(), logging.WithLogFormat("console"), logging.WithLogLevel("error"))
	if err != nil {
		return err
	}
	c1zPath, err := cmd.Flags().GetString("file")
	if err != nil {
		return err
	}
	outputFormat, err := cmd.Flags().GetString("output-format")
	if err != nil {
		return err
	}

	store, err := openReadOnlyC1ZStore(ctx, c1zPath)
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	inspector, ok := store.(dotc1z.SourceCacheInspector)
	if !ok {
		return fmt.Errorf("source-cache state exists only in pebble-format c1z files; %q is not one", c1zPath)
	}

	report := sourceCacheAuditReport{}
	// The newest FINISHED sync of any type: it is the sync the store
	// opened (see openReadOnlyC1ZStore's SetCurrentSync) and the one the
	// replay metadata gate would evaluate — LatestFullSync could describe
	// an older sync than the state being inspected, or nothing at all on
	// partial/derived artifacts.
	//
	// A metadata READ FAILURE is fatal: replay eligibility gates whether
	// orphan indexes are a hard failure or informational, so silently
	// defaulting eligibility to false would let the audit exit zero on a
	// file it could not actually judge.
	latest, err := store.SyncMeta().LatestFinishedSyncOfAnyType(ctx)
	if err != nil {
		return fmt.Errorf("reading sync run metadata (needed to judge replay eligibility): %w", err)
	}
	if latest != nil {
		report.SyncID = latest.ID
		report.SyncType = string(latest.Type)
		report.Compacted = latest.Compacted
		// The syncer's own metadata gate (type, compacted flag,
		// compaction-provenance token) — shared, so audit and syncer can
		// never drift.
		report.ReplayUnusableReason = sdkSync.ReplaySourceRunUnusableReason(latest)
		report.UsableAsReplaySource = report.ReplayUnusableReason == ""
	}
	if report.UsableAsReplaySource {
		// The replay-compatibility gate's offline-decidable half: key
		// PRESENCE. An exact-match verdict depends on the NEXT run's
		// connector declarations (cache generation, config fingerprint,
		// selection), which no offline audit can know — but an ABSENT
		// key can never match any future key, so the syncer will always
		// run cold from this file (pre-compat-key SDKs, compaction
		// folds). Reporting such a file usable=true would be wrong.
		if _, compatFound, compatErr := inspector.GetSourceCacheCompat(ctx); compatErr != nil {
			return fmt.Errorf("reading replay-compatibility key: %w", compatErr)
		} else if !compatFound {
			report.UsableAsReplaySource = false
			report.ReplayUnusableReason = "no replay-compatibility key recorded (pre-key SDK or compaction fold); the syncer treats an absent key as a mismatch and always runs cold"
		}
	}

	manifest, err := inspector.SourceCacheManifestSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("reading source-cache manifest: %w", err)
	}
	indexCounts, err := inspector.SourceScopeIndexSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("reading scope indexes: %w", err)
	}
	orphans, err := inspector.SourceCacheOrphanScopes(ctx)
	if err != nil {
		return fmt.Errorf("checking scope consistency: %w", err)
	}

	for key, snapEntry := range manifest {
		kind, scope, found := strings.Cut(key, "\x00")
		if !found {
			return fmt.Errorf("malformed manifest snapshot key %q", key)
		}
		rep := sourceCacheScopeReport{RowKind: kind, ScopeKey: scope, Rows: indexCounts[kind][scope]}
		entry, entryFound, err := inspector.LookupSourceCacheEntry(ctx, sourcecache.RowKind(kind), scope)
		if err != nil {
			return fmt.Errorf("reading manifest entry for %s/%q: %w", kind, scope, err)
		}
		if entryFound {
			rep.Validator = entry.CacheValidator
			rep.DiscoveredAt = entry.DiscoveredAt
		}
		switch {
		case snapEntry.Invalidated:
			// The lookup surface reports invalidated entries as misses;
			// surface the retained (stale) validator for forensics.
			rep.Validator = snapEntry.CacheValidator
			rep.Status = "invalidated"
		case rep.Rows == 0:
			rep.Status = "zero-row"
		default:
			rep.Status = "ok"
		}
		report.Scopes = append(report.Scopes, rep)
	}
	for kind, scopes := range orphans {
		for _, scope := range scopes {
			status := "orphan-index"
			if !report.UsableAsReplaySource {
				// Replay from this file is already impossible (compaction
				// fold, partial/derived sync): the divergence cannot poison
				// anything. Compaction folds in particular ALWAYS look like
				// this — the fold clears the manifest and keeps scope
				// stamps by design (see pebble.ClearSourceCacheEntries).
				status = "stale-index"
			}
			report.Scopes = append(report.Scopes, sourceCacheScopeReport{
				RowKind: kind, ScopeKey: scope, Rows: indexCounts[kind][scope], Status: status,
			})
			if status == "orphan-index" {
				report.OrphanIndexScopes++
			} else {
				report.StaleIndexScopes++
			}
		}
	}
	sort.Slice(report.Scopes, func(i, j int) bool {
		if report.Scopes[i].RowKind != report.Scopes[j].RowKind {
			return report.Scopes[i].RowKind < report.Scopes[j].RowKind
		}
		return report.Scopes[i].ScopeKey < report.Scopes[j].ScopeKey
	})

	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return err
		}
	} else if err := printSourceCacheAudit(report); err != nil {
		return err
	}
	if report.OrphanIndexScopes > 0 {
		return fmt.Errorf("%d orphan-index scope(s) found (ingestion invariant I6); a future sync replaying from this file could silently drop or resurrect rows", report.OrphanIndexScopes)
	}
	return nil
}

func printSourceCacheAudit(report sourceCacheAuditReport) error {
	if report.SyncID != "" {
		fmt.Fprintf(os.Stdout, "sync %s type=%s compacted=%v usable-as-replay-source=%v\n",
			report.SyncID, report.SyncType, report.Compacted, report.UsableAsReplaySource)
		if report.ReplayUnusableReason != "" {
			fmt.Fprintf(os.Stdout, "  (not usable: %s)\n", report.ReplayUnusableReason)
		}
		fmt.Fprintln(os.Stdout)
	}
	if len(report.Scopes) == 0 {
		fmt.Fprintln(os.Stdout, "no source-cache state (cold artifact: every scope will fetch fresh next sync)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "KIND\tSCOPE\tROWS\tSTATUS\tDISCOVERED\tVALIDATOR")
	for _, r := range report.Scopes {
		validator := r.Validator
		if len(validator) > 48 {
			validator = validator[:45] + "..."
		}
		discovered := ""
		if !r.DiscoveredAt.IsZero() {
			discovered = r.DiscoveredAt.UTC().Format(time.RFC3339)
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n", r.RowKind, r.ScopeKey, r.Rows, r.Status, discovered, validator)
	}
	return w.Flush()
}
