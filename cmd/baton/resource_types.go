package main

import (
	"context"

	v1 "github.com/conductorone/baton-sdk/pb/baton/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/baton/output"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/logging"
	"github.com/spf13/cobra"
)

func resourceTypesCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resource-types",
		Short: "List resource types for the latest (or current) sync",
		RunE:  runResourceTypes,
	}

	addSyncIDFlag(cmd)

	return cmd
}

func runResourceTypes(cmd *cobra.Command, args []string) error {
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

	syncID, err := cmd.Flags().GetString("sync-id")
	if err != nil {
		return err
	}

	store, err := dotc1z.NewC1ZFile(ctx, c1zPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return err
	}
	defer store.Close(ctx)

	if syncID != "" {
		err = store.ViewSync(ctx, syncID)
		if err != nil {
			return err
		}
	}

	var resourceTypes []*v1.ResourceTypeOutput
	pageToken := ""
	for {
		resp, err := store.ListResourceTypes(ctx, &v2.ResourceTypesServiceListResourceTypesRequest{PageToken: pageToken})
		if err != nil {
			return err
		}

		for _, rt := range resp.List {
			resourceTypes = append(resourceTypes, &v1.ResourceTypeOutput{ResourceType: rt})
		}

		if resp.NextPageToken == "" {
			break
		}

		pageToken = resp.NextPageToken
	}

	err = outputManager.Output(ctx, &v1.ResourceTypeListOutput{
		ResourceTypes: resourceTypes,
	})
	if err != nil {
		return err
	}

	return nil
}
