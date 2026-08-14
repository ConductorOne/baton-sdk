package c1api

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	sdkSync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// Service-mode configuration reaches the sync engine through a chain of long
// positional constructors: connectorrunner -> NewC1TaskManager ->
// c1ApiTaskManager -> newFullSyncTaskHandler -> fullSyncTaskHandler.syncOpts.
// A value dropped anywhere along that chain compiles, runs, and silently syncs
// with the wrong settings -- which is exactly what happened to
// externalPrincipalIndex on this branch's first pass. These tests walk the
// handler half of the chain for real.

type syncOptsTestHelpers struct {
	tempDir string
}

func (h *syncOptsTestHelpers) ConnectorClient() types.ConnectorClient { return nil }
func (h *syncOptsTestHelpers) Upload(ctx context.Context, r io.ReadSeeker) error {
	return nil
}

func (h *syncOptsTestHelpers) FinishTask(ctx context.Context, resp proto.Message, annos annotations.Annotations, err error) error {
	return nil
}

func (h *syncOptsTestHelpers) HeartbeatTask(ctx context.Context, annos annotations.Annotations) (context.Context, error) {
	return ctx, nil
}
func (h *syncOptsTestHelpers) TempDir() string { return h.tempDir }

// syncerUnderOpts applies a handler's assembled options to a real sync engine
// and returns it for inspection.
//
// The fields the options set are unexported, and deliberately so -- there is no
// public accessor to assert against. Reading them reflectively is a test-only
// coupling to pkg/sync's internals, taken because the alternative is asserting
// nothing: a SyncOpt is an opaque closure that cannot be identified or applied
// from outside its own package, so the only way to prove an option survived the
// plumbing is to let it act on an engine and look at the result.
func syncerUnderOpts(t *testing.T, opts []sdkSync.SyncOpt) reflect.Value {
	t.Helper()

	created, err := sdkSync.NewSyncer(t.Context(), nil, opts...)
	require.NoError(t, err)

	value := reflect.ValueOf(created)
	require.Equal(t, reflect.Ptr, value.Kind(), "NewSyncer must return a pointer to inspect")
	return value.Elem()
}

// syncerField reads one field off the engine by name, failing with an
// actionable message if pkg/sync renamed it.
func syncerField(t *testing.T, engine reflect.Value, name string) reflect.Value {
	t.Helper()

	field := engine.FieldByName(name)
	require.True(t, field.IsValid(),
		"pkg/sync syncer has no field %q: this test reads it by name to prove service-mode "+
			"configuration reaches the sync engine, so a rename needs updating here too", name)
	return field
}

func newSyncOptsTestHandler(t *testing.T, externalPrincipalIndex bool, traits []v2.ResourceType_Trait) *fullSyncTaskHandler {
	t.Helper()

	task := v1.Task_builder{
		Id:       "task-1",
		SyncFull: &v1.Task_SyncFullTask{},
	}.Build()

	// Built through the production constructor, not a struct literal: the
	// positional argument list is itself part of what these tests guard.
	handler := newFullSyncTaskHandler(
		task,
		&syncOptsTestHelpers{tempDir: t.TempDir()},
		false, // skipFullSync
		"",    // externalResourceC1ZPath
		"",    // externalResourceEntitlementIdFilter
		traits,
		externalPrincipalIndex,
		nil, // targetedSyncResources
		nil, // syncResourceTypeIDs
		1,   // workerCount
		"",  // storageEngine
		"",  // previousSyncSparePath
	)

	fullSync, ok := handler.(*fullSyncTaskHandler)
	require.True(t, ok)
	return fullSync
}

// engineUnder builds a handler the way service mode does and runs its assembled
// options through a real sync engine.
func engineUnder(t *testing.T, externalPrincipalIndex bool, traits []v2.ResourceType_Trait) reflect.Value {
	t.Helper()
	handler := newSyncOptsTestHandler(t, externalPrincipalIndex, traits)
	c1zPath := filepath.Join(t.TempDir(), "sync.c1z")
	return syncerUnderOpts(t, handler.syncOpts(zap.NewNop(), c1zPath, nil))
}

func TestFullSyncTaskHandlerThreadsExternalPrincipalIndex(t *testing.T) {
	t.Run("enabled", func(t *testing.T) {
		handler := newSyncOptsTestHandler(t, true, nil)
		require.True(t, handler.externalPrincipalIndex,
			"the constructor must carry the flag onto the handler")

		engine := engineUnder(t, true, nil)
		require.True(t, syncerField(t, engine, "externalPrincipalIndexEnabled").Bool(),
			"a service-mode task configured for the indexed matcher must reach the engine with it enabled")
	})

	t.Run("disabled", func(t *testing.T) {
		handler := newSyncOptsTestHandler(t, false, nil)
		require.False(t, handler.externalPrincipalIndex)

		engine := engineUnder(t, false, nil)
		require.False(t, syncerField(t, engine, "externalPrincipalIndexEnabled").Bool(),
			"the linear scan is the default and must not be opted out of implicitly")
	})
}

// The manager -> handler hop is the other half of the chain, and the one that
// silently dropped the value the first time. It is asserted here rather than
// through Process(), which needs a live task queue and service client.
func TestC1TaskManagerThreadsExternalPrincipalIndexToHandler(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%t", enabled), func(t *testing.T) {
			manager := &c1ApiTaskManager{externalPrincipalIndex: enabled}
			task := v1.Task_builder{Id: "task-1", SyncFull: &v1.Task_SyncFullTask{}}.Build()

			handler, ok := manager.newFullSyncHandler(task, &syncOptsTestHelpers{tempDir: t.TempDir()}).(*fullSyncTaskHandler)
			require.True(t, ok)
			require.Equal(t, enabled, handler.externalPrincipalIndex,
				"the manager must hand its own configuration to the handler")
		})
	}
}

// externalResourceTraits reaches the engine through the same chain and had the
// same untested gap; it is cheap to cover here alongside.
func TestFullSyncTaskHandlerThreadsExternalResourceTraits(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		engine := engineUnder(t, false, []v2.ResourceType_Trait{v2.ResourceType_TRAIT_APP})
		traits := syncerField(t, engine, "externalResourceTraits")
		require.Equal(t, 1, traits.Len())
		require.Equal(t, int64(v2.ResourceType_TRAIT_APP), traits.Index(0).Int())
	})

	t.Run("unset", func(t *testing.T) {
		engine := engineUnder(t, false, nil)
		require.Zero(t, syncerField(t, engine, "externalResourceTraits").Len())
	})
}
