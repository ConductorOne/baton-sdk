package main

import (
	"context"
	"sort"
	"strconv"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/baton/output"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
)

func statsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stats",
		Short: "Simple stats about the c1z",
		RunE:  runStats,
	}

	return cmd
}

func runStats(cmd *cobra.Command, args []string) error {
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
	outputManager := output.NewManager(ctx, outputFormat)

	store, err := openReadOnlyC1ZStore(ctx, c1zPath)
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	syncResp, err := store.GetLatestFinishedSync(ctx, &v2.SyncsReaderServiceGetLatestFinishedSyncRequest{})
	if err != nil {
		return err
	}

	sync := syncResp.GetSync()

	if sync == nil {
		pterm.Info.Println("No finished sync found in the c1z")
		return nil
	}

	if outputFormat == "json" {
		return outputManager.Output(ctx, sync)
	}

	if err := renderSyncRunInfo(sync); err != nil {
		return err
	}

	return nil
}

func renderSyncRunInfo(sync *v2.SyncRun) error {
	startedAt := sync.GetStartedAt().AsTime()
	endedAt := sync.GetEndedAt().AsTime()

	duration := "-"
	if sync.HasStartedAt() && sync.HasEndedAt() {
		duration = endedAt.Sub(startedAt).Round(time.Second).String()
	}

	startedStr := "-"
	if sync.HasStartedAt() {
		startedStr = startedAt.Format(time.RFC3339)
	}
	endedStr := "-"
	if sync.HasEndedAt() {
		endedStr = endedAt.Format(time.RFC3339)
	}

	infoTable := pterm.TableData{
		{"Field", "Value"},
		{"Sync ID", sync.GetId()},
		{"Sync Type", sync.GetSyncType()},
		{"Parent Sync ID", sync.GetParentSyncId()},
		{"Started At", startedStr},
		{"Ended At", endedStr},
		{"Duration", duration},
		{"Sync Token", sync.GetSyncToken()},
	}

	if err := pterm.DefaultTable.WithHasHeader().WithData(infoTable).Render(); err != nil {
		return err
	}

	stats := sync.GetStats()
	if stats == nil {
		pterm.Info.Println("No stats available for this sync")
		return nil
	}

	pterm.Println()
	totalsTable := pterm.TableData{
		{"Type", "Count"},
		{"Resource Types", strconv.FormatInt(stats.GetResourceTypes(), 10)},
		{"Resources", strconv.FormatInt(stats.GetResources(), 10)},
		{"Entitlements", strconv.FormatInt(stats.GetEntitlements(), 10)},
		{"Grants", strconv.FormatInt(stats.GetGrants(), 10)},
	}
	if err := pterm.DefaultTable.WithHasHeader().WithData(totalsTable).Render(); err != nil {
		return err
	}

	resourcesByRT := stats.GetResourcesByResourceType()
	entitlementsByRT := stats.GetEntitlementsByResourceType()
	grantsByRT := stats.GetGrantsByResourceType()

	resourceTypes := make(map[string]struct{})
	for rt := range resourcesByRT {
		resourceTypes[rt] = struct{}{}
	}
	for rt := range entitlementsByRT {
		resourceTypes[rt] = struct{}{}
	}
	for rt := range grantsByRT {
		resourceTypes[rt] = struct{}{}
	}

	if len(resourceTypes) == 0 {
		return nil
	}

	sortedRTs := make([]string, 0, len(resourceTypes))
	for rt := range resourceTypes {
		sortedRTs = append(sortedRTs, rt)
	}
	sort.Strings(sortedRTs)

	breakdownTable := pterm.TableData{
		{"Resource Type", "Resources", "Entitlements", "Grants"},
	}
	for _, rt := range sortedRTs {
		breakdownTable = append(breakdownTable, []string{
			rt,
			strconv.FormatInt(resourcesByRT[rt], 10),
			strconv.FormatInt(entitlementsByRT[rt], 10),
			strconv.FormatInt(grantsByRT[rt], 10),
		})
	}

	pterm.Println()
	if err := pterm.DefaultTable.WithHasHeader().WithData(breakdownTable).Render(); err != nil {
		return err
	}

	return nil
}
