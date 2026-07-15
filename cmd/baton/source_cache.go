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
)

// source-cache: the attribution surface for source-cache replay. Rows in
// a c1z are stamped with the scope that produced them and the manifest
// records each scope's validator + write time — this command joins the
// two so "why is this row missing / stale?" becomes a per-scope answer:
// when was the scope last actually fetched, what validator did it record,
// how many rows does it hold, and is its index/manifest state consistent.
//
// Exits non-zero when any orphan-index scope is found (ingestion
// invariant I6): such a file would poison a future sync's replay.
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
	// stamped rows — legal: the scope legitimately returned nothing), or
	// "orphan-index" (stamped rows with NO manifest entry — an invariant
	// violation that would poison the next sync's replay; see ingestion
	// invariant I6).
	Status string `json:"status"`
}

type sourceCacheAuditReport struct {
	SyncID string `json:"sync_id,omitempty"`
	// SyncType / Compacted / UsableAsReplaySource describe whether a
	// FUTURE sync may use this file as its replay source (the metadata
	// gate; see c1zstore.SyncRun.UsableAsReplaySource).
	SyncType             string                   `json:"sync_type,omitempty"`
	Compacted            bool                     `json:"compacted"`
	UsableAsReplaySource bool                     `json:"usable_as_replay_source"`
	Scopes               []sourceCacheScopeReport `json:"scopes"`
	OrphanIndexScopes    int                      `json:"orphan_index_scopes"`
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
	if latest, err := store.SyncMeta().LatestFullSync(ctx); err == nil && latest != nil {
		report.SyncID = latest.ID
		report.SyncType = string(latest.Type)
		report.Compacted = latest.Compacted
		report.UsableAsReplaySource = latest.UsableAsReplaySource()
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

	for key := range manifest {
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
		if rep.Rows == 0 {
			rep.Status = "zero-row"
		} else {
			rep.Status = "ok"
		}
		report.Scopes = append(report.Scopes, rep)
	}
	for kind, scopes := range orphans {
		for _, scope := range scopes {
			report.Scopes = append(report.Scopes, sourceCacheScopeReport{
				RowKind: kind, ScopeKey: scope, Rows: indexCounts[kind][scope], Status: "orphan-index",
			})
			report.OrphanIndexScopes++
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
		fmt.Fprintf(os.Stdout, "sync %s type=%s compacted=%v usable-as-replay-source=%v\n\n",
			report.SyncID, report.SyncType, report.Compacted, report.UsableAsReplaySource)
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
