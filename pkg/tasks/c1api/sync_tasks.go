package c1api

import (
	"context"

	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
)

func (c *c1ApiTaskManager) handleLocalFileSync(ctx context.Context, cc types.ConnectorClient, t *tasks.LocalFileSync) error {
	syncer, err := sdkSync.NewSyncer(ctx, cc, t.DbPath)
	if err != nil {
		return err
	}

	err = syncer.Sync(ctx)
	if err != nil {
		return err
	}

	err = syncer.Close(ctx)
	if err != nil {
		return err
	}

	return nil
}
