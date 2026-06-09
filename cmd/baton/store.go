package main

import (
	"context"
	"sort"

	reader_v2 "github.com/conductorone/baton-sdk/pb/c1/reader/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
)

func openReadOnlyC1ZStore(ctx context.Context, path string) (dotc1z.C1ZStore, error) {
	c1zStore, err := dotc1z.NewStore(ctx, path, dotc1z.WithReadOnly(true))
	if err != nil {
		return nil, err
	}
	if latest, err := c1zStore.SyncMeta().LatestFinishedSyncOfAnyType(ctx); err != nil {
		_ = c1zStore.Close(ctx)
		return nil, err
	} else if latest != nil {
		if err := c1zStore.SetCurrentSync(ctx, latest.ID); err != nil {
			_ = c1zStore.Close(ctx)
			return nil, err
		}
	}
	return c1zStore, nil
}

func setCurrentSyncIfRequested(ctx context.Context, store dotc1z.C1ZStore, syncID string) error {
	if syncID == "" {
		return nil
	}
	return store.SetCurrentSync(ctx, syncID)
}

func latestSyncID(ctx context.Context, store dotc1z.C1ZStore, syncType connectorstore.SyncType) (string, error) {
	runs, err := finishedSyncs(ctx, store, syncType)
	if err != nil {
		return "", err
	}
	if len(runs) == 0 {
		return "", nil
	}
	return runs[0].GetId(), nil
}

func latestAndPreviousSyncIDs(ctx context.Context, store dotc1z.C1ZStore, syncType connectorstore.SyncType) (string, string, error) {
	runs, err := finishedSyncs(ctx, store, syncType)
	if err != nil {
		return "", "", err
	}
	if len(runs) == 0 {
		return "", "", nil
	}
	latest := runs[0].GetId()
	if len(runs) == 1 {
		return latest, "", nil
	}
	return latest, runs[1].GetId(), nil
}

func finishedSyncs(ctx context.Context, store dotc1z.C1ZStore, syncType connectorstore.SyncType) ([]*reader_v2.SyncRun, error) {
	var out []*reader_v2.SyncRun
	pageToken := ""
	for {
		resp, err := store.ListSyncs(ctx, reader_v2.SyncsReaderServiceListSyncsRequest_builder{
			PageSize:  100,
			PageToken: pageToken,
		}.Build())
		if err != nil {
			return nil, err
		}
		for _, run := range resp.GetSyncs() {
			if run.GetEndedAt() == nil {
				continue
			}
			if syncType != connectorstore.SyncTypeAny && run.GetSyncType() != string(syncType) {
				continue
			}
			out = append(out, run)
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetEndedAt().AsTime().After(out[j].GetEndedAt().AsTime())
	})
	return out, nil
}
