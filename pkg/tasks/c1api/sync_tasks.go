package c1api

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"math"
	"os"

	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/service_mode/v1"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const (
	fileChunkSize = 4 * 1024 * 1024 // 1MB
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

func (c *c1ApiTaskManager) handleSyncUpload(
	ctx context.Context,
	sc *c1ServiceClient,
	cc types.ConnectorClient,
	t *tasks.SyncUpload,
) error {
	l := ctxzap.Extract(ctx).With(zap.String("task_id", t.GetTaskId()), zap.String("task_type", t.GetTaskType()))

	l.Info("Handling sync task.")

	var syncPath string
	assetFile, err := os.CreateTemp("", "baton-sdk-sync-upload")
	if err != nil {
		l.Error("failed to create temp file", zap.Error(err))
		return err
	}
	syncPath = assetFile.Name()
	err = assetFile.Close()
	if err != nil {
		return err
	}

	if syncPath == "" {
		l.Error("sync path is empty")
		return errors.New("unable to create temp file local file for sync")
	}

	syncer, err := sdkSync.NewSyncer(ctx, cc, syncPath)
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

	l.Debug("sync file created", zap.String("path", syncPath))

	syncFile, err := os.Open(syncPath)
	if err != nil {
		l.Error("failed to open sync file prior to upload", zap.Error(err))
		return err
	}
	hasher := sha256.New()
	_, err = io.Copy(hasher, syncFile)
	if err != nil {
		l.Error("failed to calculate sha256 of sync file", zap.Error(err))
		return err
	}
	shaChecksum := hasher.Sum(nil)

	l.Debug("uploading sync file metadata", zap.String("path", syncPath))

	uploadClient, hostID, done, err := sc.Upload(ctx)
	if err != nil {
		l.Error("failed to create upload client", zap.Error(err))
		return err
	}
	defer done()

	err = uploadClient.Send(&v1.UploadAssetRequest{
		Msg: &v1.UploadAssetRequest_Metadata{
			Metadata: &v1.UploadAssetRequest_UploadMetadata{
				HostId: hostID,
				TaskId: t.GetTaskId(),
			},
		},
	})
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	l.Debug("uploading sync file", zap.String("path", syncPath))

	// Seek sync file to beginning
	_, err = syncFile.Seek(0, 0)
	if err != nil {
		l.Error("failed to seek sync file to beginning", zap.Error(err))
		return err
	}
	fileInfo, err := syncFile.Stat()
	if err != nil {
		l.Error("failed to stat sync file", zap.Error(err))
		return err
	}

	totalParts := uint64(math.Ceil(float64(fileInfo.Size()) / float64(fileChunkSize)))
	l.Debug("uploading sync file", zap.Uint64("total_parts", totalParts))
	for i := uint64(0); i < totalParts; i++ {
		partSize := fileChunkSize
		if i == totalParts-1 {
			partSize = int(fileInfo.Size()) - int(i)*fileChunkSize
		}

		part := make([]byte, partSize)
		_, err = syncFile.Read(part)
		if err != nil {
			l.Error("failed to read sync file", zap.Error(err))
			return err
		}

		err = uploadClient.Send(&v1.UploadAssetRequest{
			Msg: &v1.UploadAssetRequest_Data{
				Data: &v1.UploadAssetRequest_UploadData{
					Data: part,
				},
			},
		})
		if err != nil {
			l.Error("failed to send upload part", zap.Error(err))
			return err
		}
	}

	err = uploadClient.Send(&v1.UploadAssetRequest{
		Msg: &v1.UploadAssetRequest_Eof{
			Eof: &v1.UploadAssetRequest_UploadEOF{
				Sha256Checksum: shaChecksum,
			},
		},
	})
	if err != nil {
		l.Error("failed to send upload metadata", zap.Error(err))
		return err
	}

	err = os.Remove(syncPath)
	if err != nil {
		l.Error("failed to remove temp file", zap.Error(err))
		return err
	}

	_, err = uploadClient.CloseAndRecv()
	if err != nil {
		l.Error("failed to close upload client", zap.Error(err))
		return err
	}

	return nil
}
