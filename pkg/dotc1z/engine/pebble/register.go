package pebble

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Register installs the Pebble engine into dotc1z's process-global engine
// registry. Callers opt into Pebble dependencies by importing this package and
// calling Register before using dotc1z.NewStore with EnginePebble, or before
// opening an existing v3/Pebble .c1z through NewStore.
func Register() error {
	if existing, ok := dotc1z.EngineDriverFor(dotc1z.EnginePebble); ok {
		if _, ok := existing.(driver); ok {
			return nil
		}
	}
	return dotc1z.RegisterEngine(driver{})
}

type driver struct{}

func (driver) Engine() dotc1z.Engine    { return dotc1z.EnginePebble }
func (driver) Format() dotc1z.C1ZFormat { return dotc1z.C1ZFormatV3 }

func (driver) OpenStore(ctx context.Context, outputFilePath string, opts dotc1z.StoreOptions) (connectorstore.Writer, error) {
	tmpDir, err := os.MkdirTemp(opts.TmpDir, "c1z-pebble")
	if err != nil {
		return nil, err
	}
	cleanupOnError := func(e error) error {
		if removeErr := os.RemoveAll(tmpDir); removeErr != nil {
			e = errors.Join(e, removeErr)
		}
		return e
	}

	dbDir := filepath.Join(tmpDir, "db")
	if err := unpackExisting(outputFilePath, dbDir); err != nil {
		return nil, cleanupOnError(err)
	}

	e, err := Open(ctx, dbDir, WithReadOnly(opts.ReadOnly))
	if err != nil {
		return nil, cleanupOnError(err)
	}
	return &registeredStore{
		Adapter:         NewAdapter(e),
		engine:          e,
		outputFilePath:  outputFilePath,
		tmpDir:          tmpDir,
		readOnly:        opts.ReadOnly,
		payloadEncoding: opts.PayloadEncoding,
	}, nil
}

func unpackExisting(outputFilePath string, dbDir string) error {
	stat, err := os.Stat(outputFilePath)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case stat.Size() == 0:
		return nil
	}

	f, err := os.Open(outputFilePath)
	if err != nil {
		return err
	}
	defer f.Close()

	env, err := formatv3.ReadEnvelope(f)
	if err != nil {
		return err
	}
	defer env.Close()
	if dotc1z.Engine(env.Manifest.GetEngine()) != dotc1z.EnginePebble {
		return fmt.Errorf("%w: %s", ErrUnknownEngine, env.Manifest.GetEngine())
	}
	if err := os.MkdirAll(dbDir, 0o755); err != nil {
		return err
	}
	if err := formatv3.ExtractZstdTar(env.PayloadReader, dbDir); err != nil {
		return err
	}
	return nil
}

type registeredStore struct {
	*Adapter
	engine          *Engine
	outputFilePath  string
	tmpDir          string
	readOnly        bool
	payloadEncoding dotc1z.PayloadEncoding

	closeMu sync.Mutex
	closed  bool
	dirty   bool
}

// Compile-time guard: a registered Pebble store satisfies the full
// dotc1z.C1ZStore contract — connectorstore.Writer (via Adapter)
// plus the three sub-store methods (Grants, SyncMeta, FileOps) and
// the C1ZStore Close(ctx) signature. Lets callers route Pebble
// stores through pkg/sync.NewSyncer's WithConnectorStore option
// the same way they route SQLite *C1File handles today.
var _ dotc1z.C1ZStore = (*registeredStore)(nil)

// FileOps overrides the Adapter-level FileOps so CloneSync threads
// the registeredStore's configured payload encoding into the
// destination c1z. Without this, clone output would always use
// the default TAR_ZSTD even when the source store was opened with
// WithPayloadEncoding(PayloadEncodingTar).
func (s *registeredStore) FileOps() dotc1z.FileOps {
	return s.FileOpsWithEncoding(s.payloadEncoding)
}

func (s *registeredStore) markDirty(err error) error {
	if err == nil {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return err
}

func (s *registeredStore) StartNewSync(ctx context.Context, syncType connectorstore.SyncType, parentSyncID string) (string, error) {
	syncID, err := s.Adapter.StartNewSync(ctx, syncType, parentSyncID)
	if err == nil {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return syncID, err
}

func (s *registeredStore) StartOrResumeSync(ctx context.Context, syncType connectorstore.SyncType, syncID string) (string, bool, error) {
	id, started, err := s.Adapter.StartOrResumeSync(ctx, syncType, syncID)
	if err == nil && started {
		s.closeMu.Lock()
		s.dirty = true
		s.closeMu.Unlock()
	}
	return id, started, err
}

func (s *registeredStore) CheckpointSync(ctx context.Context, syncToken string) error {
	return s.markDirty(s.Adapter.CheckpointSync(ctx, syncToken))
}

func (s *registeredStore) EndSync(ctx context.Context) error {
	return s.markDirty(s.Adapter.EndSync(ctx))
}

func (s *registeredStore) PutAsset(ctx context.Context, assetRef *v2.AssetRef, contentType string, data []byte) error {
	return s.markDirty(s.Adapter.PutAsset(ctx, assetRef, contentType, data))
}

func (s *registeredStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	return s.markDirty(s.Adapter.PutGrants(ctx, grants...))
}

func (s *registeredStore) PutResourceTypes(ctx context.Context, resourceTypes ...*v2.ResourceType) error {
	return s.markDirty(s.Adapter.PutResourceTypes(ctx, resourceTypes...))
}

func (s *registeredStore) PutResources(ctx context.Context, resources ...*v2.Resource) error {
	return s.markDirty(s.Adapter.PutResources(ctx, resources...))
}

func (s *registeredStore) PutEntitlements(ctx context.Context, entitlements ...*v2.Entitlement) error {
	return s.markDirty(s.Adapter.PutEntitlements(ctx, entitlements...))
}

func (s *registeredStore) DeleteGrant(ctx context.Context, grantID string) error {
	return s.markDirty(s.Adapter.DeleteGrant(ctx, grantID))
}

func (s *registeredStore) Close(ctx context.Context) (retErr error) {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true

	defer func() {
		if removeErr := os.RemoveAll(s.tmpDir); removeErr != nil {
			retErr = errors.Join(retErr, removeErr)
		}
	}()

	if !s.readOnly && s.dirty {
		if err := s.save(ctx); err != nil {
			retErr = errors.Join(retErr, err)
		}
	}
	if err := s.engine.Close(); err != nil {
		retErr = errors.Join(retErr, err)
	}
	return retErr
}

func (s *registeredStore) save(ctx context.Context) error {
	if s.outputFilePath == "" {
		return fmt.Errorf("pebble engine: output file path is empty")
	}
	checkpointDir := filepath.Join(s.tmpDir, "checkpoint")
	if err := s.engine.CheckpointTo(ctx, checkpointDir); err != nil {
		return err
	}

	tmpPath := s.outputFilePath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if out != nil {
			_ = out.Close()
		}
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	manifest, err := s.manifest()
	if err != nil {
		return err
	}
	if err := formatv3.WriteEnvelope(out, manifest, checkpointDir); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	out = nil
	if err := os.Rename(tmpPath, s.outputFilePath); err != nil {
		return err
	}
	success = true
	return nil
}

func (s *registeredStore) manifest() (*c1zv3.C1ZManifestV3, error) {
	descriptors, err := formatv3.BuildDescriptorClosure()
	if err != nil {
		return nil, err
	}
	return c1zv3.C1ZManifestV3_builder{
		Engine:              string(dotc1z.EnginePebble),
		EngineSchemaVersion: uint32(SDKPebbleFormat),
		PayloadEncoding:     payloadEncodingToProto(s.payloadEncoding),
		Descriptors:         descriptors,
		CreatedAt:           timestamppb.Now(),
		CreatedByTool:       "baton-sdk",
	}.Build(), nil
}

// payloadEncodingToProto maps the public dotc1z.PayloadEncoding to
// the c1zv3 enum. PayloadEncodingUnspecified means "engine default"
// for our purposes: TAR_ZSTD.
func payloadEncodingToProto(enc dotc1z.PayloadEncoding) c1zv3.PayloadEncoding {
	switch enc {
	case dotc1z.PayloadEncodingTar:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR
	case dotc1z.PayloadEncodingTarZstd, dotc1z.PayloadEncodingUnspecified:
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	default:
		// Any non-enumerated value (including the reserved 1 and 2)
		// falls back to the default. WriteEnvelope will reject any
		// non-TAR/non-TAR_ZSTD value before writing bytes, so a
		// caller setting a bogus encoding gets an error at save time.
		return c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD
	}
}
