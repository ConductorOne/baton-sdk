package sync //nolint:revive,nolintlint // Backwards-compatible package name.

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/stretchr/testify/require"
)

type ledgerAttachmentStoreKey struct{}
type ledgerAttachmentDriver struct{}

func (ledgerAttachmentDriver) Engine() c1zstore.Engine  { return "ledger-attachment-test" }
func (ledgerAttachmentDriver) Format() dotc1z.C1ZFormat { return dotc1z.C1ZFormatV3 }
func (ledgerAttachmentDriver) OpenStore(ctx context.Context, _ string, _ dotc1z.StoreOptions) (c1zstore.Store, error) {
	store, ok := ctx.Value(ledgerAttachmentStoreKey{}).(c1zstore.Store)
	if !ok {
		return nil, errors.New("attachment fixture missing store")
	}
	return store, nil
}

var registerLedgerAttachmentDriver = sync.OnceValue(func() error { return dotc1z.RegisterEngine(ledgerAttachmentDriver{}) })

type ledgerAttachmentCloseStore struct {
	c1zstore.Store
	closes int
}

func (s *ledgerAttachmentCloseStore) Close(context.Context) error { s.closes++; return nil }

func TestLedgerPublicRegisteredPathAttachment(t *testing.T) {
	require.NoError(t, registerLedgerAttachmentDriver())
	for _, engine := range []string{"pebble", "sqlite", "", "other"} {
		for _, capability := range []bool{false, true} {
			label := engine + "/without-ledger"
			if capability {
				label = engine + "/with-ledger"
			}
			t.Run(label, func(t *testing.T) {
				f := newLedgerFixture(t)
				closeStore := &ledgerAttachmentCloseStore{Store: f.store}
				metadataStore := ledgerMetadataStore{Store: closeStore, engine: engine}
				var store c1zstore.Store = metadataStore
				if capability {
					store = ledgerMetadataCapabilityStore{ledgerMetadataStore: metadataStore, PageLedgerStore: f.ledger}
				}
				ctx := context.WithValue(t.Context(), ledgerAttachmentStoreKey{}, store)
				before := ledgerRawSnapshot(t, f.engine)
				f.audit.mu.Lock()
				writes := len(f.audit.events)
				f.audit.mu.Unlock()
				created, err := NewSyncer(ctx, ledgerAttachmentConnector{}, WithC1ZPath(filepath.Join(t.TempDir(), "attach.c1z")), WithStorageEngine(ledgerAttachmentDriver{}.Engine()))
				require.NoError(t, err)
				s := created.(*syncer)
				err = s.loadStore(ctx)
				require.True(t, equalLedgerSnapshot(before, ledgerRawSnapshot(t, f.engine)))
				f.audit.mu.Lock()
				afterWrites := len(f.audit.events)
				f.audit.mu.Unlock()
				require.Equal(t, writes, afterWrites)
				valid := engine == "pebble" && capability || engine == "sqlite" && !capability
				if !valid {
					require.Error(t, err)
					require.Equal(t, 1, closeStore.closes)
					return
				}
				require.NoError(t, err)
				require.Equal(t, engine == "pebble", s.ledgered)
				require.Zero(t, closeStore.closes)
				require.NoError(t, s.Close(ctx))
				require.Equal(t, 1, closeStore.closes)
			})
		}
	}
}

func TestLedgerNilStoreFallsBackToPath(t *testing.T) {
	for _, engine := range []c1zstore.Engine{c1zstore.EnginePebble, c1zstore.EngineSQLite} {
		for _, nilFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/nil-first-%t", engine, nilFirst), func(t *testing.T) {
				path := WithC1ZPath(filepath.Join(t.TempDir(), "fallback.c1z"))
				opts := []SyncOpt{path, WithConnectorStore(nil), WithStorageEngine(engine)}
				if nilFirst {
					opts[0], opts[1] = opts[1], opts[0]
				}
				created, err := NewSyncer(t.Context(), ledgerAttachmentConnector{}, opts...)
				require.NoError(t, err)
				s := created.(*syncer)
				require.NoError(t, s.loadStore(t.Context()))
				require.Equal(t, engine == c1zstore.EnginePebble, s.ledgered)
				require.NoError(t, s.Close(t.Context()))
			})
		}
	}
	_, err := NewSyncer(t.Context(), ledgerAttachmentConnector{}, WithConnectorStore(nil))
	require.Error(t, err)
}
