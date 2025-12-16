package anonymize

import (
	"context"
	"fmt"
	"os"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

// ProcessorStats contains statistics about the anonymization process.
type ProcessorStats struct {
	ResourceTypesProcessed int
	ResourcesProcessed     int
	EntitlementsProcessed  int
	GrantsProcessed        int
	AssetsDeleted          bool
	SessionsCleared        bool
	SyncRunsCleared        bool
}

// AnonymizeC1ZFile anonymizes a c1z file and writes the result to the output path.
// If outputPath is empty, it creates a file with ".anonymized" suffix.
func (a *Anonymizer) AnonymizeC1ZFile(ctx context.Context, inputPath string, outputPath string) (*ProcessorStats, error) {
	if outputPath == "" {
		outputPath = inputPath + ".anonymized"
	}

	// Open the input file read-only
	inputFile, err := dotc1z.NewC1ZFile(ctx, inputPath, dotc1z.WithReadOnly(true))
	if err != nil {
		return nil, fmt.Errorf("failed to open input c1z file: %w", err)
	}
	defer inputFile.Close()

	// Create the output file
	outputFile, err := dotc1z.NewC1ZFile(ctx, outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output c1z file: %w", err)
	}

	stats := &ProcessorStats{}

	// Process each sync run from the input file
	if err := a.processC1File(ctx, inputFile, outputFile, stats); err != nil {
		_ = outputFile.Close()
		_ = os.Remove(outputPath)
		return nil, err
	}

	// Anonymize the timestamps on all sync runs in the output file
	if err := outputFile.ClearSyncRunTimestamps(ctx); err != nil {
		_ = outputFile.Close()
		_ = os.Remove(outputPath)
		return nil, fmt.Errorf("failed to anonymize sync run timestamps: %w", err)
	}
	stats.SyncRunsCleared = true

	// Close the output file (this saves the changes)
	if err := outputFile.Close(); err != nil {
		_ = os.Remove(outputPath)
		return nil, fmt.Errorf("failed to save anonymized file: %w", err)
	}

	return stats, nil
}

// processC1File processes all data from the input file and writes anonymized data to the output file.
// It iterates through each sync run in the input and creates a corresponding sync in the output.
func (a *Anonymizer) processC1File(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	// List all sync runs from the input file
	// pageToken := ""
	// for {
	latestSync, err := input.GetLatestFinishedSync(ctx, reader_v2.SyncsReaderServiceGetLatestFinishedSyncRequest_builder{
		SyncType: string(connectorstore.SyncTypeFull),
	}.Build())
	if err != nil {
		return fmt.Errorf("failed to get latest finished sync: %w", err)
	}

	// Set the input file to view this specific sync
	if err := input.ViewSync(ctx, latestSync.GetSync().GetId()); err != nil {
		return fmt.Errorf("failed to set view sync: %w", err)
	}

	// Start a new sync in the output file with the same type
	syncType := connectorstore.SyncType(latestSync.GetSync().GetSyncType())
	if _, err := output.StartNewSync(ctx, syncType, ""); err != nil {
		return fmt.Errorf("failed to start sync in output file: %w", err)
	}

	err = a.processSyncData(ctx, input, output, stats)
	if err != nil {
		return fmt.Errorf("failed to process sync data: %w", err)
	}

	// End the sync in the output file
	if err := output.EndSync(ctx); err != nil {
		return fmt.Errorf("failed to end sync in output file: %w", err)
	}

	// Note: Assets and sessions are NOT copied to the output file.
	// The output file starts fresh without these potentially identifying data.
	stats.AssetsDeleted = true
	stats.SessionsCleared = true

	return nil
}

// processSyncData processes all data types for the current sync.
func (a *Anonymizer) processSyncData(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	// Process resource types
	if err := a.processResourceTypes(ctx, input, output, stats); err != nil {
		return fmt.Errorf("failed to process resource types: %w", err)
	}

	// Process resources
	if err := a.processResources(ctx, input, output, stats); err != nil {
		return fmt.Errorf("failed to process resources: %w", err)
	}

	// Process entitlements
	if err := a.processEntitlements(ctx, input, output, stats); err != nil {
		return fmt.Errorf("failed to process entitlements: %w", err)
	}

	// Process grants
	if err := a.processGrants(ctx, input, output, stats); err != nil {
		return fmt.Errorf("failed to process grants: %w", err)
	}

	return nil
}

// processResourceTypes reads resource types from input, anonymizes them, and writes to output.
func (a *Anonymizer) processResourceTypes(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	pageToken := ""
	for {
		req := v2.ResourceTypesServiceListResourceTypesRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build()

		resp, err := input.ListResourceTypes(ctx, req)
		if err != nil {
			return err
		}

		slice := resp.GetList()
		for _, rt := range slice {
			if err := a.AnonymizeResourceType(rt); err != nil {
				return err
			}
			stats.ResourceTypesProcessed++
		}

		// Write the anonymized resource types to output
		if len(slice) > 0 {
			if err := output.PutResourceTypes(ctx, slice...); err != nil {
				return err
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil
}

// processResources reads resources from input, anonymizes them, and writes to output.
func (a *Anonymizer) processResources(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	pageToken := ""
	for {
		req := v2.ResourcesServiceListResourcesRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build()

		resp, err := input.ListResources(ctx, req)
		if err != nil {
			return err
		}

		slice := resp.GetList()
		for _, r := range slice {
			if err := a.AnonymizeResource(r); err != nil {
				return err
			}
			stats.ResourcesProcessed++
		}

		// Write the anonymized resources to output
		if len(slice) > 0 {
			if err := output.PutResources(ctx, slice...); err != nil {
				return err
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil
}

// processEntitlements reads entitlements from input, anonymizes them, and writes to output.
func (a *Anonymizer) processEntitlements(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	pageToken := ""
	for {
		req := v2.EntitlementsServiceListEntitlementsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build()

		resp, err := input.ListEntitlements(ctx, req)
		if err != nil {
			return err
		}

		slice := resp.GetList()
		for _, e := range slice {
			if err := a.AnonymizeEntitlement(e); err != nil {
				return err
			}
			stats.EntitlementsProcessed++
		}

		// Write the anonymized entitlements to output
		if len(slice) > 0 {
			if err := output.PutEntitlements(ctx, slice...); err != nil {
				return err
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil
}

// processGrants reads grants from input, anonymizes them, and writes to output.
func (a *Anonymizer) processGrants(ctx context.Context, input *dotc1z.C1File, output *dotc1z.C1File, stats *ProcessorStats) error {
	pageToken := ""
	for {
		req := v2.GrantsServiceListGrantsRequest_builder{
			PageSize:  1000,
			PageToken: pageToken,
		}.Build()

		resp, err := input.ListGrants(ctx, req)
		if err != nil {
			return err
		}

		slice := resp.GetList()
		for _, g := range slice {
			if err := a.AnonymizeGrant(g); err != nil {
				return err
			}
			stats.GrantsProcessed++
		}

		// Write the anonymized grants to output
		if len(slice) > 0 {
			if err := output.PutGrants(ctx, slice...); err != nil {
				return err
			}
		}

		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	return nil
}
