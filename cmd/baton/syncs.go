package main

import (
	"context"

	v1 "github.com/conductorone/baton-sdk/pb/baton/v1"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/baton/output"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func syncsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "syncs",
		Short: "List the information for the various sync data stored in the C1Z",
		RunE:  runSyncList,
	}

	return cmd
}

func runSyncList(cmd *cobra.Command, args []string) error {
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

	var syncRuns []*v1.SyncOutput
	pageToken := ""
	for {
		resp, err := store.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
			PageSize:  100,
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return err
		}

		for _, sr := range resp.GetSyncs() {
			syncRuns = append(syncRuns, &v1.SyncOutput{
				Id:           sr.GetId(),
				StartedAt:    sr.GetStartedAt(),
				EndedAt:      sr.GetEndedAt(),
				SyncToken:    sr.GetSyncToken(),
				SyncType:     sr.GetSyncType(),
				ParentSyncId: sr.GetParentSyncId(),
			})
		}

		if resp.GetNextPageToken() == "" {
			break
		}

		pageToken = resp.GetNextPageToken()
	}

	err = outputManager.Output(ctx, &v1.SyncListOutput{
		Syncs: syncRuns,
	})
	if err != nil {
		return err
	}

	return nil
}
