package chaosconnector

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestScheduleRoundTripAndRequiredRule(t *testing.T) {
	schedule := NewSchedule(Rule{
		ID: "resources-transient",
		Match: Matcher{
			Domain:  DomainConnector,
			Service: "ResourcesService",
			Method:  "ListResources",
			Attempt: 1,
			Phase:   PhaseBeforeCall,
		},
		Effects: []Effect{{
			Kind:    EffectError,
			Code:    codes.Unavailable,
			Message: "injected transient",
		}},
		MinFires: 1,
		MaxFires: 1,
	})

	encoded, err := json.Marshal(schedule)
	require.NoError(t, err)
	var replay Schedule
	require.NoError(t, json.Unmarshal(encoded, &replay))
	require.Equal(t, schedule, replay)

	runtime, err := NewRuntime(replay, &Trace{})
	require.NoError(t, err)
	require.ErrorContains(t, runtime.VerifyRequired(), "resources-transient")

	op := runtime.Begin(Operation{
		Domain:  DomainConnector,
		Service: "ResourcesService",
		Method:  "ListResources",
	})
	op.Phase = PhaseBeforeCall
	fired := runtime.Match(op)
	require.Len(t, fired, 1)
	err = runtime.ApplyControlEffects(t.Context(), op, fired)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NoError(t, runtime.VerifyRequired())
	require.Equal(t, map[string]int{"resources-transient": 1}, runtime.FireCounts())
}

func TestLostResponseDefaultsToRetryableUnavailable(t *testing.T) {
	runtime, err := NewRuntime(NewSchedule(Rule{
		ID:       "lost-response",
		Match:    Matcher{Phase: PhaseAfterDelegate},
		Effects:  []Effect{{Kind: EffectLoseResponse}},
		MinFires: 1,
		MaxFires: 1,
	}), &Trace{})
	require.NoError(t, err)
	op := runtime.Begin(Operation{Domain: DomainConnector})
	op.Phase = PhaseAfterDelegate
	err = runtime.ApplyControlEffects(t.Context(), op, runtime.Match(op))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.ErrorContains(t, err, "response lost after delegate")
}

func TestLogicalAttemptsAreConcurrencySafe(t *testing.T) {
	runtime, err := NewRuntime(NewSchedule(), &Trace{})
	require.NoError(t, err)

	const count = 32
	attempts := make(chan int, count)
	var wg sync.WaitGroup
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := runtime.Begin(Operation{
				Domain:       DomainConnector,
				Service:      "GrantsService",
				Method:       "ListGrants",
				ResourceType: "group",
				PageToken:    "p1",
			})
			attempts <- op.Attempt
		}()
	}
	wg.Wait()
	close(attempts)

	got := make([]int, 0, count)
	for attempt := range attempts {
		got = append(got, attempt)
	}
	sort.Ints(got)
	want := make([]int, count)
	for i := range want {
		want[i] = i + 1
	}
	require.Equal(t, want, got)
}

func TestBlockEffectUsesDeterministicBarrier(t *testing.T) {
	schedule := NewSchedule(Rule{
		ID: "barrier",
		Match: Matcher{
			Method: "ListEntitlements",
			Phase:  PhaseBeforeCall,
		},
		Effects:  []Effect{{Kind: EffectBlock, Barrier: "release-entitlements"}},
		MinFires: 1,
		MaxFires: 1,
	})
	runtime, err := NewRuntime(schedule, &Trace{})
	require.NoError(t, err)

	op := runtime.Begin(Operation{Method: "ListEntitlements"})
	op.Phase = PhaseBeforeCall
	fired := runtime.Match(op)
	done := make(chan error, 1)
	go func() {
		done <- runtime.ApplyControlEffects(context.Background(), op, fired)
	}()

	select {
	case err := <-done:
		require.Failf(t, "barrier returned early", "error: %v", err)
	default:
	}
	runtime.ReleaseBarrier("release-entitlements")
	require.NoError(t, <-done)
	require.NoError(t, runtime.VerifyRequired())
}

func TestScenarioManifestIsIndependentClone(t *testing.T) {
	resourceType := v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
	}.Build()
	scenario := &Scenario{
		Name:         "clone",
		InitialEpoch: "initial",
		Epochs: map[string]*Dataset{
			"initial": {
				ResourceTypes: []*v2.ResourceType{resourceType},
			},
		},
	}

	manifest, err := scenario.Manifest("initial")
	require.NoError(t, err)
	require.Len(t, manifest.ResourceTypes, 1)
	require.NotSame(t, resourceType, manifest.ResourceTypes[0])
	manifest.ResourceTypes[0].SetDisplayName("Changed")
	require.Equal(t, "Group", resourceType.GetDisplayName())
}

func TestScheduleRejectsVacuousOrAmbiguousRules(t *testing.T) {
	tests := []struct {
		name     string
		schedule Schedule
	}{
		{
			name:     "wrong version",
			schedule: Schedule{Version: ScheduleVersion + 1},
		},
		{
			name: "missing id",
			schedule: NewSchedule(Rule{
				Effects: []Effect{{Kind: EffectError}},
			}),
		},
		{
			name: "duplicate id",
			schedule: NewSchedule(
				Rule{ID: "same", Effects: []Effect{{Kind: EffectError}}},
				Rule{ID: "same", Effects: []Effect{{Kind: EffectError}}},
			),
		},
		{
			name: "empty effects",
			schedule: NewSchedule(Rule{
				ID: "empty",
			}),
		},
		{
			name: "unknown mutation",
			schedule: NewSchedule(Rule{
				ID:      "mutation",
				Effects: []Effect{{Kind: EffectMutate}},
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, tc.schedule.Validate())
		})
	}
}
