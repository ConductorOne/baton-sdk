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

	pebbleengine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/conductorone/baton-sdk/pkg/logging"
)

// source-cache: the attribution surface for source-cache replay. Rows in
// a c1z are stamped with the scope that produced them and the manifest
// records each scope's validator + write time — this command joins the
// two so "why is this row missing / stale?" becomes a per-scope answer:
// when was the scope last actually fetched, what validator did it record,
// how many rows does it hold, and is its index/manifest state consistent.
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

	holder, ok := store.(interface{ PebbleEngine() *pebbleengine.Engine })
	var engine *pebbleengine.Engine
	if ok {
		engine = holder.PebbleEngine()
	}
	if engine == nil {
		return fmt.Errorf("source-cache state exists only in pebble-format c1z files; %q is not one", c1zPath)
	}

	manifest, err := engine.SourceCacheManifestSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("reading source-cache manifest: %w", err)
	}
	indexCounts, err := engine.SourceScopeIndexSnapshot(ctx)
	if err != nil {
		return fmt.Errorf("reading scope indexes: %w", err)
	}

	reports := make([]sourceCacheScopeReport, 0, len(manifest))
	for key := range manifest {
		kind, scope, found := strings.Cut(key, "\x00")
		if !found {
			return fmt.Errorf("malformed manifest snapshot key %q", key)
		}
		rep := sourceCacheScopeReport{RowKind: kind, ScopeKey: scope, Rows: indexCounts[kind][scope]}
		rec, err := engine.GetSourceCacheEntry(ctx, kind, scope)
		if err != nil {
			return fmt.Errorf("reading manifest entry for %s/%q: %w", kind, scope, err)
		}
		rep.Validator = rec.GetCacheValidator()
		rep.DiscoveredAt = rec.GetDiscoveredAt().AsTime()
		if rep.Rows == 0 {
			rep.Status = "zero-row"
		} else {
			rep.Status = "ok"
		}
		reports = append(reports, rep)
	}
	// Index scopes with no manifest entry: invariant I6 violations.
	for kind, scopes := range indexCounts {
		for scope, n := range scopes {
			if _, ok := manifest[kind+"\x00"+scope]; !ok {
				reports = append(reports, sourceCacheScopeReport{
					RowKind: kind, ScopeKey: scope, Rows: n, Status: "orphan-index",
				})
			}
		}
	}
	sort.Slice(reports, func(i, j int) bool {
		if reports[i].RowKind != reports[j].RowKind {
			return reports[i].RowKind < reports[j].RowKind
		}
		return reports[i].ScopeKey < reports[j].ScopeKey
	})

	if outputFormat == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(reports)
	}

	if len(reports) == 0 {
		fmt.Fprintln(os.Stdout, "no source-cache state (cold artifact: every scope will fetch fresh next sync)")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "KIND\tSCOPE\tROWS\tSTATUS\tDISCOVERED\tVALIDATOR")
	orphans := 0
	for _, r := range reports {
		validator := r.Validator
		if len(validator) > 48 {
			validator = validator[:45] + "..."
		}
		discovered := ""
		if !r.DiscoveredAt.IsZero() {
			discovered = r.DiscoveredAt.UTC().Format(time.RFC3339)
		}
		if r.Status == "orphan-index" {
			orphans++
		}
		fmt.Fprintf(w, "%s\t%s\t%d\t%s\t%s\t%s\n", r.RowKind, r.ScopeKey, r.Rows, r.Status, discovered, validator)
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if orphans > 0 {
		fmt.Fprintf(os.Stdout, "\nWARNING: %d orphan-index scope(s): stamped rows with no manifest entry (ingestion invariant I6). "+
			"A future sync replaying from this file could silently drop or resurrect these scopes' rows.\n", orphans)
	}
	return nil
}
