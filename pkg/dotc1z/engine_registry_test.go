package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

type registryTestDriver struct {
	engine c1zstore.Engine
	format C1ZFormat
	err    error
	called bool
	path   string
	opts   StoreOptions
}

func (d *registryTestDriver) Engine() c1zstore.Engine { return d.engine }
func (d *registryTestDriver) Format() C1ZFormat       { return d.format }

func (d *registryTestDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (c1zstore.Store, error) {
	d.called = true
	d.path = outputFilePath
	d.opts = opts
	return nil, d.err
}

func uniqueEngineName(t *testing.T, suffix string) c1zstore.Engine {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "-", "_", "-").Replace(name)
	return c1zstore.Engine(name + "-" + suffix)
}

func writeV3EnvelopeForEngine(t *testing.T, path string, engine c1zstore.Engine) {
	t.Helper()
	payloadDir := filepath.Join(t.TempDir(), "payload")
	require.NoError(t, os.MkdirAll(payloadDir, 0o755))
	f, err := os.Create(path)
	require.NoError(t, err)
	defer f.Close()
	manifest := c1zv3.C1ZManifestV3_builder{
		Engine:          string(engine),
		PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}.Build()
	require.NoError(t, formatv3.WriteEnvelope(f, manifest, payloadDir))
}

func TestRegisterEngineRejectsDuplicateEngine(t *testing.T) {
	engine := uniqueEngineName(t, "dup")
	first := &registryTestDriver{engine: engine, format: C1ZFormatV3}
	require.NoError(t, RegisterEngine(first))
	err := RegisterEngine(&registryTestDriver{engine: engine, format: C1ZFormatV3})
	require.Error(t, err, "RegisterEngine duplicate returned nil error")
}

func TestNewStoreDefaultsToSQLiteDriver(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "default.c1z")
	store, err := NewStore(ctx, path)
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()
	_, ok := store.(*C1File)
	require.True(t, ok, "NewStore default type = %T, want *C1File", store)
}

func TestNewStoreRequiresRegisteredEngineForNewFile(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "missing-new")
	_, err := NewStore(ctx, filepath.Join(t.TempDir(), "missing.c1z"), WithEngine(engine))
	require.ErrorIs(t, err, ErrEngineNotAvailable, "NewStore error = %v, want ErrEngineNotAvailable", err)
}

func TestNewStoreDispatchesNewFileToRegisteredEngine(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "registered-new")
	wantErr := errors.New("registered driver called")
	driver := &registryTestDriver{engine: engine, format: C1ZFormatV3, err: wantErr}
	require.NoError(t, RegisterEngine(driver))
	path := filepath.Join(t.TempDir(), "registered.c1z")
	_, err := NewStore(ctx, path, WithEngine(engine), WithReadOnly(true))
	require.ErrorIs(t, err, wantErr, "NewStore error = %v, want %v", err, wantErr)
	require.True(t, driver.called, "registered driver was not called")
	require.Equal(t, path, driver.path, "driver path = %q, want %q", driver.path, path)
	require.True(t, driver.opts.ReadOnly, "driver options lost WithReadOnly")
}

func TestNewStoreDispatchesExistingV3ByManifestEngine(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "registered-v3")
	wantErr := errors.New("v3 driver called")
	driver := &registryTestDriver{engine: engine, format: C1ZFormatV3, err: wantErr}
	require.NoError(t, RegisterEngine(driver))
	path := filepath.Join(t.TempDir(), "existing.c1z")
	writeV3EnvelopeForEngine(t, path, engine)

	_, err := NewStore(ctx, path)
	require.ErrorIs(t, err, wantErr, "NewStore error = %v, want %v", err, wantErr)
	require.True(t, driver.called, "registered v3 driver was not called")
}

func TestNewStoreExistingV3MissingDriver(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing-v3.c1z")
	writeV3EnvelopeForEngine(t, path, uniqueEngineName(t, "missing-v3"))

	_, err := NewStore(ctx, path)
	require.ErrorIs(t, err, ErrEngineNotAvailable, "NewStore error = %v, want ErrEngineNotAvailable", err)
}

func TestNewStoreExistingV1IgnoresRequestedEngine(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "existing-v1.c1z")
	f, err := NewC1ZFile(ctx, path)
	require.NoError(t, err)
	_, err = f.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)
	require.NoError(t, f.EndSync(ctx))
	require.NoError(t, f.Close(ctx))

	store, err := NewStore(ctx, path, WithEngine(uniqueEngineName(t, "ignored")))
	require.NoError(t, err)
	defer func() { _ = store.Close(ctx) }()
	_, ok := store.(*C1File)
	require.True(t, ok, "NewStore existing v1 type = %T, want *C1File", store)
}
