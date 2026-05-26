package dotc1z

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	c1zv3 "github.com/conductorone/baton-sdk/pb/c1/c1z/v3"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	formatv3 "github.com/conductorone/baton-sdk/pkg/dotc1z/format/v3"
)

type registryTestDriver struct {
	engine Engine
	format C1ZFormat
	err    error
	called bool
	path   string
	opts   StoreOptions
}

func (d *registryTestDriver) Engine() Engine    { return d.engine }
func (d *registryTestDriver) Format() C1ZFormat { return d.format }

func (d *registryTestDriver) OpenStore(ctx context.Context, outputFilePath string, opts StoreOptions) (connectorstore.Writer, error) {
	d.called = true
	d.path = outputFilePath
	d.opts = opts
	return nil, d.err
}

func uniqueEngineName(t *testing.T, suffix string) Engine {
	t.Helper()
	name := strings.ToLower(t.Name())
	name = strings.NewReplacer("/", "-", "_", "-").Replace(name)
	return Engine(name + "-" + suffix)
}

func writeV3EnvelopeForEngine(t *testing.T, path string, engine Engine) {
	t.Helper()
	payloadDir := filepath.Join(t.TempDir(), "payload")
	if err := os.MkdirAll(payloadDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	manifest := c1zv3.C1ZManifestV3_builder{
		Engine:          string(engine),
		PayloadEncoding: c1zv3.PayloadEncoding_PAYLOAD_ENCODING_TAR_ZSTD,
	}.Build()
	if err := formatv3.WriteEnvelope(f, manifest, payloadDir); err != nil {
		t.Fatalf("WriteEnvelope: %v", err)
	}
}

func TestRegisterEngineRejectsDuplicateEngine(t *testing.T) {
	engine := uniqueEngineName(t, "dup")
	first := &registryTestDriver{engine: engine, format: C1ZFormatV3}
	if err := RegisterEngine(first); err != nil {
		t.Fatalf("RegisterEngine first: %v", err)
	}
	err := RegisterEngine(&registryTestDriver{engine: engine, format: C1ZFormatV3})
	if err == nil {
		t.Fatal("RegisterEngine duplicate returned nil error")
	}
}

func TestNewStoreDefaultsToSQLiteDriver(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "default.c1z")
	store, err := NewStore(ctx, path)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	defer func() { _ = store.Close(ctx) }()
	if _, ok := store.(*C1File); !ok {
		t.Fatalf("NewStore default type = %T, want *C1File", store)
	}
}

func TestNewStoreRequiresRegisteredEngineForNewFile(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "missing-new")
	_, err := NewStore(ctx, filepath.Join(t.TempDir(), "missing.c1z"), WithEngine(engine))
	if !errors.Is(err, ErrEngineNotAvailable) {
		t.Fatalf("NewStore error = %v, want ErrEngineNotAvailable", err)
	}
}

func TestNewStoreDispatchesNewFileToRegisteredEngine(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "registered-new")
	wantErr := errors.New("registered driver called")
	driver := &registryTestDriver{engine: engine, format: C1ZFormatV3, err: wantErr}
	if err := RegisterEngine(driver); err != nil {
		t.Fatalf("RegisterEngine: %v", err)
	}
	path := filepath.Join(t.TempDir(), "registered.c1z")
	_, err := NewStore(ctx, path, WithEngine(engine), WithReadOnly(true))
	if !errors.Is(err, wantErr) {
		t.Fatalf("NewStore error = %v, want %v", err, wantErr)
	}
	if !driver.called {
		t.Fatal("registered driver was not called")
	}
	if driver.path != path {
		t.Fatalf("driver path = %q, want %q", driver.path, path)
	}
	if !driver.opts.ReadOnly {
		t.Fatal("driver options lost WithReadOnly")
	}
}

func TestNewStoreDispatchesExistingV3ByManifestEngine(t *testing.T) {
	ctx := context.Background()
	engine := uniqueEngineName(t, "registered-v3")
	wantErr := errors.New("v3 driver called")
	driver := &registryTestDriver{engine: engine, format: C1ZFormatV3, err: wantErr}
	if err := RegisterEngine(driver); err != nil {
		t.Fatalf("RegisterEngine: %v", err)
	}
	path := filepath.Join(t.TempDir(), "existing.c1z")
	writeV3EnvelopeForEngine(t, path, engine)

	_, err := NewStore(ctx, path)
	if !errors.Is(err, wantErr) {
		t.Fatalf("NewStore error = %v, want %v", err, wantErr)
	}
	if !driver.called {
		t.Fatal("registered v3 driver was not called")
	}
}

func TestNewStoreExistingV3MissingDriver(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing-v3.c1z")
	writeV3EnvelopeForEngine(t, path, uniqueEngineName(t, "missing-v3"))

	_, err := NewStore(ctx, path)
	if !errors.Is(err, ErrEngineNotAvailable) {
		t.Fatalf("NewStore error = %v, want ErrEngineNotAvailable", err)
	}
}

func TestNewStoreExistingV1IgnoresRequestedEngine(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "existing-v1.c1z")
	f, err := NewC1ZFile(ctx, path)
	if err != nil {
		t.Fatalf("NewC1ZFile: %v", err)
	}
	if _, err := f.StartNewSync(ctx, connectorstore.SyncTypeFull, ""); err != nil {
		t.Fatalf("StartNewSync: %v", err)
	}
	if err := f.EndSync(ctx); err != nil {
		t.Fatalf("EndSync: %v", err)
	}
	if err := f.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store, err := NewStore(ctx, path, WithEngine(uniqueEngineName(t, "ignored")))
	if err != nil {
		t.Fatalf("NewStore existing v1: %v", err)
	}
	defer func() { _ = store.Close(ctx) }()
	if _, ok := store.(*C1File); !ok {
		t.Fatalf("NewStore existing v1 type = %T, want *C1File", store)
	}
}
