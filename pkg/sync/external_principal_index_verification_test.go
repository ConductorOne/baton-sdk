//go:build verification

package sync //nolint:revive,nolintlint // matches the existing package name

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/types/resource"
)

// linearUserMatchPositions is a test-only reference model copied from the
// phase-6a implementation. It deliberately does not share bucketing or merge
// logic with externalPrincipalIndex.
//
// The email comparison is spelled out here rather than calling a production
// helper. An oracle that borrows the code it is checking cannot report a
// divergence in that code, and the relation being pinned -- strings.EqualFold,
// the contract asserted by TestExternalResourceMatch*FoldingContract -- is the
// whole point of the comparison, so the model states it directly.
func linearUserMatchPositions(principals []*v2.Resource, key, value string) []int {
	matches := make([]int, 0)
	for i, principal := range principals {
		userTrait, err := resource.GetUserTrait(principal)
		if err != nil {
			continue
		}
		if key == "email" && slices.ContainsFunc(userTrait.GetEmails(), func(e *v2.UserTrait_Email) bool {
			return strings.EqualFold(e.GetAddress(), value)
		}) {
			matches = append(matches, i)
			continue
		}
		profileValue, ok := resource.GetProfileStringValue(resource.GetProfile(principal), key)
		if ok && strings.EqualFold(profileValue, value) {
			matches = append(matches, i)
		}
	}
	return matches
}

func indexedUserMatchPositions(principals []*v2.Resource, key, value string) []int {
	idx := newExternalUserPrincipalIndex(principals, zap.NewNop())
	matches := idx.matchProfile(key, value)
	if key == "email" {
		matches = mergePositions(idx.matchUserTraitEmail(value), matches)
	}
	return matches
}

func TestVerificationExternalPrincipalIndexMatchesPhase6AReference(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "profile", map[string]any{
			"upn":   "Target@Example.com",
			"email": "profile@example.com",
		}),
		testUserPrincipal(t, "trait", nil,
			resource.WithEmail("trait@example.com", true),
			resource.WithEmail("secondary@example.com", false),
		),
		testUserPrincipal(t, "both", map[string]any{"email": "shared@example.com"},
			resource.WithEmail("SHARED@example.com", true),
		),
		testUserPrincipal(t, "none", map[string]any{"upn": "other@example.com"}),
	}

	for _, tc := range []struct {
		key   string
		value string
	}{
		{key: "upn", value: "target@example.com"},
		{key: "email", value: "TRAIT@example.com"},
		{key: "email", value: "secondary@example.com"},
		{key: "email", value: "shared@example.com"},
		{key: "missing", value: "anything"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			require.Equal(t,
				linearUserMatchPositions(principals, tc.key, tc.value),
				indexedUserMatchPositions(principals, tc.key, tc.value),
			)
		})
	}
}

func TestVerificationExternalPrincipalIndexRandomizedASCIIParity(t *testing.T) {
	rng := rand.New(rand.NewSource(0xC1))
	keys := []string{"email", "upn", "login", "external_id"}
	values := []string{"alpha@example.com", "BRAVO@example.com", "charlie", "DELTA", ""}

	for topology := 0; topology < 200; topology++ {
		principalCount := 1 + rng.Intn(20)
		principals := make([]*v2.Resource, 0, principalCount)
		for principalIndex := 0; principalIndex < principalCount; principalIndex++ {
			profile := map[string]any{
				keys[rng.Intn(len(keys))]: values[rng.Intn(len(values))],
			}
			if rng.Intn(7) == 0 {
				// The phase-6a path skipped a principal with an unreadable user
				// trait even when its profile would otherwise match.
				principal, err := resource.NewResource(
					fmt.Sprintf("malformed-%d-%d", topology, principalIndex),
					userResourceType,
					"malformed",
					resource.WithResourceProfile(profile),
				)
				require.NoError(t, err)
				principals = append(principals, principal)
				continue
			}

			var traitOptions []resource.UserTraitOption
			if rng.Intn(2) == 0 {
				traitOptions = append(traitOptions,
					resource.WithEmail(values[rng.Intn(len(values))], true),
				)
			}
			principals = append(principals, testUserPrincipal(
				t,
				fmt.Sprintf("user-%d-%d", topology, principalIndex),
				profile,
				traitOptions...,
			))
		}

		for _, key := range keys {
			for _, value := range values {
				for _, query := range []string{value, strings.ToUpper(value)} {
					require.Equal(t,
						linearUserMatchPositions(principals, key, query),
						indexedUserMatchPositions(principals, key, query),
						"topology=%d key=%q query=%q", topology, key, query,
					)
				}
			}
		}
	}
}

func TestVerificationExternalPrincipalIndexUnicodeEqualFoldParity(t *testing.T) {
	// EqualFold treats Greek sigma and final sigma as equal, while ToLower
	// produces different bucket keys ("σ" and "ς"). The phase-6a scan therefore
	// matches this pair and the index prefilter currently does not.
	const stored = "Σ"
	const query = "ς"
	require.True(t, strings.EqualFold(stored, query), "test premise")

	principals := []*v2.Resource{
		testUserPrincipal(t, "unicode", map[string]any{"upn": stored}),
	}
	require.Equal(t,
		linearUserMatchPositions(principals, "upn", query),
		indexedUserMatchPositions(principals, "upn", query),
	)
}

func TestVerificationExternalPrincipalIndexDeduplicatesRepeatedTraitEmail(t *testing.T) {
	principals := []*v2.Resource{
		testUserPrincipal(t, "duplicate", nil,
			resource.WithEmail("same@example.com", true),
			resource.WithEmail("SAME@example.com", false),
		),
	}

	reference := linearUserMatchPositions(principals, "email", "same@example.com")
	indexed := indexedUserMatchPositions(principals, "email", "same@example.com")
	require.Equal(t, reference, indexed)
	require.Equal(t, []int{0}, indexed, "one principal must be emitted at most once")
}

var errVerificationDeleteCut = errors.New("verification: injected delete cut")

// interruptingExternalMatchStore delegates to a real store and injects a
// process-like stop at a selected delete. Embedding preserves the complete
// c1zstore.Store contract while the explicit DeleteGrantByRefs and
// DeleteGrantsByRefs methods ensure processGrantsWithExternalPrincipals takes
// the production Pebble fast path (which is the batched one).
type interruptingExternalMatchStore struct {
	c1zstore.Store
	failDeleteAt  int64
	deleteCalls   atomic.Int64
	putBatchSizes []int
}

func (s *interruptingExternalMatchStore) PutGrants(ctx context.Context, grants ...*v2.Grant) error {
	s.putBatchSizes = append(s.putBatchSizes, len(grants))
	return s.Store.PutGrants(ctx, grants...)
}

func (s *interruptingExternalMatchStore) DeleteGrantByRefs(ctx context.Context, grant *v2.Grant) error {
	call := s.deleteCalls.Add(1)
	if s.failDeleteAt > 0 && call == s.failDeleteAt {
		return errVerificationDeleteCut
	}
	deleter, ok := s.Store.(grantByRefsDeleter)
	if !ok {
		return fmt.Errorf("verification premise: wrapped store lacks DeleteGrantByRefs")
	}
	return deleter.DeleteGrantByRefs(ctx, grant)
}

// DeleteGrantsByRefs keeps the cut per-GRANT (not per-batch) so the injected
// stop still lands between two individual deletes: everything before
// failDeleteAt commits durably, the cut grant and everything after it does
// not. That is the interruption shape the resume-to-golden assertion needs,
// and routing through the real store's batch method keeps the wrapper on the
// production path.
func (s *interruptingExternalMatchStore) DeleteGrantsByRefs(ctx context.Context, grants ...*v2.Grant) error {
	deleter, ok := s.Store.(grantsByRefsBatchDeleter)
	if !ok {
		return fmt.Errorf("verification premise: wrapped store lacks DeleteGrantsByRefs")
	}
	if s.failDeleteAt <= 0 {
		s.deleteCalls.Add(int64(len(grants)))
		return deleter.DeleteGrantsByRefs(ctx, grants...)
	}
	for _, grant := range grants {
		if s.deleteCalls.Add(1) == s.failDeleteAt {
			return errVerificationDeleteCut
		}
		if err := deleter.DeleteGrantsByRefs(ctx, grant); err != nil {
			return err
		}
	}
	return nil
}

func externalMatchGrantIDs(ctx context.Context, store c1zstore.Store) ([]string, error) {
	var ids []string
	for ga, err := range store.Grants().ListWithAnnotations(ctx) {
		if err != nil {
			return nil, err
		}
		annos := annotations.Annotations(ga.Grant.GetAnnotations())
		if annos.ContainsAny(&v2.ExternalResourceMatchAll{}, &v2.ExternalResourceMatch{}, &v2.ExternalResourceMatchID{}) {
			ids = append(ids, ga.Grant.GetId())
		}
	}
	slices.Sort(ids)
	return ids, nil
}

func verificationPrincipals(t *testing.T) []*v2.Resource {
	t.Helper()
	principals := make([]*v2.Resource, 0, 3)
	for i := 0; i < 3; i++ {
		principal := testUserPrincipal(
			t,
			fmt.Sprintf("external-user-%d", i),
			map[string]any{"upn": "target@example.com"},
		)
		annos := annotations.Annotations(principal.GetAnnotations())
		annos.Update(&v2.BatonID{})
		principal.SetAnnotations(annos)
		principals = append(principals, principal)
	}
	return principals
}

func seedExternalMatchVerificationStore(
	t *testing.T,
	path string,
) (c1zstore.Store, string, []*v2.Resource) {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(filepath.Dir(path)),
	)
	require.NoError(t, err)

	syncID, err := store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	placeholder := v2.ResourceId_builder{
		ResourceType: userResourceType.GetId(),
		Resource:     "placeholder",
	}.Build()
	var carriers []*v2.Grant
	for i := 0; i < 3; i++ {
		group := testGroupPrincipal(t, fmt.Sprintf("group-%d", i), nil)
		carriers = append(carriers, gt.NewGrant(
			group,
			fmt.Sprintf("member-%d", i),
			placeholder,
			gt.WithAnnotation(v2.ExternalResourceMatch_builder{
				Key:          "upn",
				Value:        "target@example.com",
				ResourceType: v2.ResourceType_TRAIT_USER,
			}.Build()),
		))
	}
	require.NoError(t, store.PutGrants(ctx, carriers...))

	state := newState()
	state.SetHasExternalResourcesGrants()
	state.PushAction(ctx, Action{Op: SyncExternalResourcesOp})
	token, err := state.Marshal()
	require.NoError(t, err)
	require.NoError(t, store.CheckpointSync(ctx, token))

	return store, syncID, verificationPrincipals(t)
}

func finishExternalMatchVerificationSync(
	t *testing.T,
	store c1zstore.Store,
	state *state,
) {
	t.Helper()
	ctx := t.Context()
	action := state.Current()
	require.NotNil(t, action)
	require.Equal(t, SyncExternalResourcesOp, action.Op)
	state.FinishAction(ctx, action)
	token, err := state.Marshal()
	require.NoError(t, err)
	require.NoError(t, store.CheckpointSync(ctx, token))
	require.NoError(t, store.EndSync(ctx))
	require.NoError(t, store.Close(ctx))
}

func grantDigest(t *testing.T, path, syncID string) []string {
	t.Helper()
	ctx := t.Context()
	store, err := dotc1z.NewStore(ctx, path,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(filepath.Dir(path)),
		dotc1z.WithReadOnly(true),
	)
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(ctx)) }()
	require.NoError(t, store.SetCurrentSync(ctx, syncID))

	var digest []string
	pageToken := ""
	for {
		resp, err := store.ListGrants(ctx, v2.GrantsServiceListGrantsRequest_builder{
			PageToken: pageToken,
		}.Build())
		require.NoError(t, err)
		for _, grant := range resp.GetList() {
			wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(grant)
			require.NoError(t, err)
			digest = append(digest, hex.EncodeToString(wire))
		}
		pageToken = resp.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	slices.Sort(digest)
	return digest
}

func TestVerificationExternalPrincipalMatchDeleteCutResumesToGolden(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	goldenPath := filepath.Join(tmpDir, "golden.c1z")
	goldenStore, goldenSyncID, principals := seedExternalMatchVerificationStore(t, goldenPath)
	goldenState := newState()
	goldenState.SetHasExternalResourcesGrants()
	goldenState.PushAction(ctx, Action{Op: SyncExternalResourcesOp})
	goldenSyncer := &syncer{state: goldenState}
	goldenSyncer.setStore(goldenStore)
	require.NoError(t, goldenSyncer.processGrantsWithExternalPrincipals(ctx, principals))
	finishExternalMatchVerificationSync(t, goldenStore, goldenState)
	goldenDigest := grantDigest(t, goldenPath, goldenSyncID)

	cutPath := filepath.Join(tmpDir, "cut.c1z")
	cutStore, cutSyncID, principals := seedExternalMatchVerificationStore(t, cutPath)
	cutWrapper := &interruptingExternalMatchStore{
		Store:        cutStore,
		failDeleteAt: 2,
	}
	cutState := newState()
	currentToken, err := cutStore.CurrentSyncStep(ctx)
	require.NoError(t, err)
	require.NoError(t, cutState.Unmarshal(currentToken))
	cutSyncer := &syncer{state: cutState}
	cutSyncer.setStore(cutWrapper)
	err = cutSyncer.processGrantsWithExternalPrincipals(ctx, principals)
	require.ErrorIs(t, err, errVerificationDeleteCut)
	require.Equal(t, []int{9}, cutWrapper.putBatchSizes, "test premise: cut happened after the bulk put")
	require.NoError(t, cutStore.Close(ctx))

	resumedStore, err := dotc1z.NewStore(ctx, cutPath,
		dotc1z.WithEngine(c1zstore.EnginePebble),
		dotc1z.WithTmpDir(tmpDir),
	)
	require.NoError(t, err)
	require.NoError(t, resumedStore.SetCurrentSync(ctx, cutSyncID))
	resumedToken, err := resumedStore.CurrentSyncStep(ctx)
	require.NoError(t, err)
	resumedState := newState()
	require.NoError(t, resumedState.Unmarshal(resumedToken))
	require.NotNil(t, resumedState.Current(), "unfinished action must survive the cut")

	resumedWrapper := &interruptingExternalMatchStore{Store: resumedStore}
	resumedSyncer := &syncer{state: resumedState}
	resumedSyncer.setStore(resumedWrapper)
	require.NoError(t, resumedSyncer.processGrantsWithExternalPrincipals(ctx, principals))
	require.Equal(t, []int{33}, resumedWrapper.putBatchSizes,
		"resume scans two remaining carriers plus nine expanded grants, each matching three principals")

	// Replay the same unfinished action again before advancing its token. This
	// is the same-checkpoint-twice cell from the guide.
	require.NoError(t, resumedSyncer.processGrantsWithExternalPrincipals(ctx, principals))
	require.Equal(t, []int{33, 27}, resumedWrapper.putBatchSizes,
		"the second replay processes nine stable expanded rows, each matching three principals")
	finishExternalMatchVerificationSync(t, resumedStore, resumedState)

	require.Equal(t, goldenDigest, grantDigest(t, cutPath, cutSyncID))
}

func BenchmarkVerificationExternalPrincipalIndexCostCurve(b *testing.B) {
	for _, tc := range []struct {
		principals int
		keys       int
	}{
		{principals: 1_000, keys: 1},
		{principals: 1_000, keys: 10},
		{principals: 1_000, keys: 100},
		{principals: 10_000, keys: 3},
	} {
		b.Run(fmt.Sprintf("principals=%d/keys=%d", tc.principals, tc.keys), func(b *testing.B) {
			principals := make([]*v2.Resource, 0, tc.principals)
			for principalIndex := 0; principalIndex < tc.principals; principalIndex++ {
				profile := make(map[string]any, tc.keys)
				for keyIndex := 0; keyIndex < tc.keys; keyIndex++ {
					profile[fmt.Sprintf("key-%03d", keyIndex)] = fmt.Sprintf("value-%06d", principalIndex)
				}
				principal, err := resource.NewUserResource(
					fmt.Sprintf("principal-%06d", principalIndex),
					userResourceType,
					fmt.Sprintf("Principal %d", principalIndex),
					nil,
					resource.WithResourceProfile(profile),
				)
				require.NoError(b, err)
				principals = append(principals, principal)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(tc.principals), "principals")
			b.ReportMetric(float64(tc.keys), "profile-keys")
			b.ResetTimer()
			for range b.N {
				idx := newExternalUserPrincipalIndex(principals, zap.NewNop())
				for keyIndex := 0; keyIndex < tc.keys; keyIndex++ {
					matches := idx.matchProfile(
						fmt.Sprintf("key-%03d", keyIndex),
						fmt.Sprintf("value-%06d", tc.principals-1),
					)
					if len(matches) != 1 {
						b.Fatalf("verification premise: got %d matches", len(matches))
					}
				}
				runtime.KeepAlive(idx)
			}
		})
	}
}
