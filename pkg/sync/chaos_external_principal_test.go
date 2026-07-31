package sync

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	chaosoracle "github.com/conductorone/baton-sdk/internal/chaosconnector/oracle"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/stretchr/testify/require"
)

var errChaosExternalPrincipalCut = errors.New("chaos: external principal rewrite cut")

type chaosExternalPrincipalCutStore struct {
	c1zstore.Store
	failDeleteAt int64
	deleteCalls  atomic.Int64
}

func (s *chaosExternalPrincipalCutStore) DeleteGrantByRefs(
	ctx context.Context,
	grant *v2.Grant,
) error {
	if s.deleteCalls.Add(1) == s.failDeleteAt {
		return errChaosExternalPrincipalCut
	}
	deleter, ok := s.Store.(grantByRefsDeleter)
	if !ok {
		return errors.New("chaos: store lacks refs-based grant deletion")
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

func TestChaosConnectorExternalPrincipalCorpus(t *testing.T) {
	for _, corpusCase := range chaosconnector.ExternalPrincipalCorpus() {
		for _, transport := range []chaosTransport{chaosTransportDirect, chaosTransportGRPC} {
			t.Run(corpusCase.Name+"/"+transport.String(), func(t *testing.T) {
				ctx := t.Context()
				tmpDir := t.TempDir()
				externalPath := filepath.Join(tmpDir, "external.c1z")
				internalPath := filepath.Join(tmpDir, "internal.c1z")

				externalScenario, internalScenario, err := corpusCase.Build()
				require.NoError(t, err)
				externalRun, err := chaosconnector.NewRun(
					externalScenario,
					chaosconnector.NewSchedule(),
				)
				require.NoError(t, err)
				externalHarness := newChaosHarness(
					t,
					ctx,
					externalRun,
					externalPath,
					tmpDir,
					chaosTransportDirect,
					WithWorkerCount(1),
				)
				externalHarness.SyncAndClose(t, ctx)
				if corpusCase.Name == "external-principal/group-profile-case-fold" {
					externalStore, openErr := dotc1z.NewStore(
						ctx,
						externalPath,
						dotc1z.WithEngine(c1zstore.EnginePebble),
						dotc1z.WithTmpDir(tmpDir),
						dotc1z.WithReadOnly(true),
					)
					require.NoError(t, openErr)
					response, listErr := externalStore.ListResources(
						ctx,
						v2.ResourcesServiceListResourcesRequest_builder{
							ResourceTypeId: chaosconnector.ExternalGroupTypeID,
						}.Build(),
					)
					require.NoError(t, listErr)
					require.Len(t, response.GetList(), 1)
					value, present := rs.GetProfileStringValue(
						rs.GetProfile(response.GetList()[0]),
						corpusCase.Key,
					)
					require.True(t, present)
					require.Equal(t, corpusCase.Value, value)
					require.NoError(t, externalStore.Close(ctx))
				}

				internalRun, err := chaosconnector.NewRun(
					internalScenario,
					chaosconnector.NewSchedule(),
				)
				require.NoError(t, err)
				internalHarness := newChaosHarness(
					t,
					ctx,
					internalRun,
					internalPath,
					tmpDir,
					transport,
					WithWorkerCount(1),
					WithExternalResourceC1ZPath(externalPath),
				)
				internalHarness.SyncAndClose(t, ctx)

				manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
				require.NoError(t, err)
				require.Len(t, manifest.Entitlements, 1)

				store, err := dotc1z.NewStore(
					ctx,
					internalPath,
					dotc1z.WithEngine(c1zstore.EnginePebble),
					dotc1z.WithTmpDir(tmpDir),
					dotc1z.WithReadOnly(true),
				)
				require.NoError(t, err)
				defer func() { require.NoError(t, store.Close(ctx)) }()
				observation, err := chaosoracle.ReadExternalPrincipal(
					ctx,
					store,
					manifest.Entitlements[0].GetId(),
					chaosconnector.ExternalPlaceholderID,
				)
				require.NoError(t, err)
				require.NoError(t, chaosoracle.CompareExternalPrincipal(
					externalPrincipalExpectation(t, corpusCase, externalScenario),
					observation,
				))
			})
		}
	}
}

func TestChaosConnectorExternalPrincipalCorpusResumesAfterRewriteCut(t *testing.T) {
	for _, corpusCase := range chaosconnector.ExternalPrincipalCorpus() {
		t.Run(corpusCase.Name, func(t *testing.T) {
			ctx := t.Context()
			tmpDir := t.TempDir()
			externalPath := filepath.Join(tmpDir, "external.c1z")
			internalPath := filepath.Join(tmpDir, "internal.c1z")

			externalScenario, internalScenario, err := corpusCase.Build()
			require.NoError(t, err)
			runExternalPrincipalSource(t, externalScenario, externalPath, tmpDir)

			internalStore, err := dotc1z.NewStore(
				ctx,
				internalPath,
				dotc1z.WithEngine(c1zstore.EnginePebble),
				dotc1z.WithTmpDir(tmpDir),
			)
			require.NoError(t, err)
			cutStore := &chaosExternalPrincipalCutStore{
				Store:        internalStore,
				failDeleteAt: 1,
			}
			cutRun, err := chaosconnector.NewRun(
				internalScenario,
				chaosconnector.NewSchedule(),
			)
			require.NoError(t, err)
			cutHarness := newChaosHarness(
				t,
				ctx,
				cutRun,
				internalPath,
				tmpDir,
				chaosTransportDirect,
				WithWorkerCount(1),
				WithExternalResourceC1ZPath(externalPath),
				WithConnectorStore(cutStore),
			)
			require.ErrorIs(t, cutHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
			require.NoError(t, cutHarness.Close(ctx))

			manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
			require.NoError(t, err)
			require.Len(t, manifest.Entitlements, 1)
			interrupted := readExternalPrincipalObservation(
				t,
				internalPath,
				tmpDir,
				manifest.Entitlements[0].GetId(),
			)
			require.False(t, interrupted.Sealed)
			require.Equal(t, 1, interrupted.CarrierCount)

			resumeRun, err := chaosconnector.NewRun(
				internalScenario,
				chaosconnector.NewSchedule(),
			)
			require.NoError(t, err)
			resumeHarness := newChaosHarness(
				t,
				ctx,
				resumeRun,
				internalPath,
				tmpDir,
				chaosTransportDirect,
				WithWorkerCount(1),
				WithExternalResourceC1ZPath(externalPath),
			)
			resumeHarness.SyncAndClose(t, ctx)

			final := readExternalPrincipalObservation(
				t,
				internalPath,
				tmpDir,
				manifest.Entitlements[0].GetId(),
			)
			require.NoError(t, chaosoracle.CompareExternalPrincipal(
				externalPrincipalExpectation(t, corpusCase, externalScenario),
				final,
			))
		})
	}
}

func TestChaosConnectorExternalPrincipalResumeUsesCurrentExternalAnswer(t *testing.T) {
	var corpusCase chaosconnector.ExternalPrincipalCase
	for _, candidate := range chaosconnector.ExternalPrincipalCorpus() {
		if candidate.Name == "external-principal/all-users" {
			corpusCase = candidate
			break
		}
	}
	require.NotEmpty(t, corpusCase.Name)

	ctx := t.Context()
	tmpDir := t.TempDir()
	firstExternalPath := filepath.Join(tmpDir, "external-first.c1z")
	resumeExternalPath := filepath.Join(tmpDir, "external-resume.c1z")
	internalPath := filepath.Join(tmpDir, "internal.c1z")

	firstExternal, internalScenario, err := corpusCase.Build()
	require.NoError(t, err)
	runExternalPrincipalSource(t, firstExternal, firstExternalPath, tmpDir)

	internalStore, err := dotc1z.NewStore(
		ctx,
		internalPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	cutStore := &chaosExternalPrincipalCutStore{
		Store:        internalStore,
		failDeleteAt: 1,
	}
	cutRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	cutHarness := newChaosHarness(
		t,
		ctx,
		cutRun,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(firstExternalPath),
		WithConnectorStore(cutStore),
	)
	require.ErrorIs(t, cutHarness.Syncer.Sync(ctx), errChaosExternalPrincipalCut)
	require.NoError(t, cutHarness.Close(ctx))

	resumeExternal, _, err := corpusCase.Build()
	require.NoError(t, err)
	resumeDataset := resumeExternal.Epochs[resumeExternal.InitialEpoch]
	users := resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""]
	require.Len(t, users.List, 2)
	users.List = users.List[1:]
	resumeDataset.Resources[chaosconnector.ExternalUserTypeID][""] = users
	runExternalPrincipalSource(t, resumeExternal, resumeExternalPath, tmpDir)

	resumeRun, err := chaosconnector.NewRun(internalScenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	resumeHarness := newChaosHarness(
		t,
		ctx,
		resumeRun,
		internalPath,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
		WithExternalResourceC1ZPath(resumeExternalPath),
	)
	resumeHarness.SyncAndClose(t, ctx)

	manifest, err := internalScenario.Manifest(internalScenario.InitialEpoch)
	require.NoError(t, err)
	require.Len(t, manifest.Entitlements, 1)
	final := readExternalPrincipalObservation(
		t,
		internalPath,
		tmpDir,
		manifest.Entitlements[0].GetId(),
	)
	require.NoError(t, chaosoracle.CompareExternalPrincipal(
		chaosoracle.ExternalPrincipalExpectation{
			PrincipalIDs:  []string{"external-user-2"},
			RequireSealed: true,
		},
		final,
	))
}

func runExternalPrincipalSource(
	t *testing.T,
	scenario *chaosconnector.Scenario,
	path string,
	tmpDir string,
) {
	t.Helper()
	run, err := chaosconnector.NewRun(scenario, chaosconnector.NewSchedule())
	require.NoError(t, err)
	harness := newChaosHarness(
		t,
		t.Context(),
		run,
		path,
		tmpDir,
		chaosTransportDirect,
		WithWorkerCount(1),
	)
	harness.SyncAndClose(t, t.Context())
}

func externalPrincipalExpectation(
	t *testing.T,
	corpusCase chaosconnector.ExternalPrincipalCase,
	externalScenario *chaosconnector.Scenario,
) chaosoracle.ExternalPrincipalExpectation {
	t.Helper()
	expectation := chaosoracle.ExternalPrincipalExpectation{
		PrincipalIDs:  corpusCase.ExpectedPrincipalIDs,
		RequireSealed: true,
	}
	if !corpusCase.Expandable {
		return expectation
	}
	manifest, err := externalScenario.Manifest(externalScenario.InitialEpoch)
	require.NoError(t, err)
	for _, entitlement := range manifest.Entitlements {
		if entitlement.GetResource().GetId().GetResource() == corpusCase.Value {
			expectation.ExpandableEntitlementIDs = append(
				expectation.ExpandableEntitlementIDs,
				entitlement.GetId(),
			)
		}
	}
	require.NotEmpty(t, expectation.ExpandableEntitlementIDs)
	return expectation
}

func readExternalPrincipalObservation(
	t *testing.T,
	path string,
	tmpDir string,
	entitlementID string,
) chaosoracle.ExternalPrincipalObservation {
	t.Helper()
	store, err := dotc1z.NewStore(
		t.Context(),
		path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	observation, err := chaosoracle.ReadExternalPrincipal(
		t.Context(),
		store,
		entitlementID,
		chaosconnector.ExternalPlaceholderID,
	)
	require.NoError(t, err)
	return observation
}
