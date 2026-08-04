//go:build crashharness

// Command baton-crash-harness is the connector half of the real-binary
// interruption instrument (see driver_test.go). It runs one sync session of
// a deterministic connector as a real OS process and exits — the driver
// composes sessions into production-shaped histories:
//
//   - `-run-duration-ms` bounds a session the way production budgets bound a
//     connector task: on expiry the syncer force-checkpoints, the store
//     close durably saves the c1z, and the process exits cleanly reporting
//     NOT_COMPLETE. The next session — a new process, like the next task on
//     a possibly different machine — resumes from that artifact.
//   - a SIGKILL from the driver lands between two arbitrary instructions,
//     taking the connector, working store, and temp state with it —
//     including mid-save. The next session must fall back to whatever
//     artifact survived.
//
// The dataset is a pure function of the flags (no fixtures, no randomness),
// so any two completed syncs of the same flags must produce identical
// stores — that determinism is the driver's oracle. `-page-delay-ms`
// stretches every paginated connector call so sessions are interruptible
// mid-action and sequential syncs outlive the production checkpoint cadence
// (minCheckpointInterval).
//
// The storage engine and worker count come from BATON_STORAGE_ENGINE and
// BATON_WORKERS, mirroring the CI demo matrix. The final stdout line is
// HARNESS_RESULT <json> for the driver to parse.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/conductorone/baton-sdk/internal/chaosconnector"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	sdksync "github.com/conductorone/baton-sdk/pkg/sync"
	"github.com/conductorone/baton-sdk/pkg/types"
	et "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	gt "github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
)

const (
	resourcePageSize = 50
	grantPageSize    = 10
	usersPerGroup    = 20
)

type crashConnector struct {
	delay time.Duration

	userType  *v2.ResourceType
	groupType *v2.ResourceType
	users     []*v2.Resource
	groups    []*v2.Resource
	entsBy    map[string]*v2.Entitlement
	grantsBy  map[string][]*v2.Grant

	v2.AssetServiceClient
	v2.GrantManagerServiceClient
	v2.ResourceManagerServiceClient
	v2.AccountManagerServiceClient
	v2.ResourceDeleterServiceClient
	v2.CredentialManagerServiceClient
	v2.EventServiceClient
	v2.TicketsServiceClient
	v2.ActionServiceClient
	v2.ResourceGetterServiceClient
	v2.EntitlementsServiceClient
}

func newCrashConnector(users, groups int, delay time.Duration) (*crashConnector, error) {
	c := &crashConnector{
		delay:    delay,
		entsBy:   make(map[string]*v2.Entitlement, groups),
		grantsBy: make(map[string][]*v2.Grant, groups),
	}
	c.userType = v2.ResourceType_builder{
		Id:          "user",
		DisplayName: "User",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_USER},
		// Users carry no entitlements or grants; without this annotation
		// the syncer enumerates all 3000 anyway and every empty response
		// pays the page delay (+150s sequential).
		Annotations: annotations.New(&v2.SkipEntitlementsAndGrants{}),
	}.Build()
	c.groupType = v2.ResourceType_builder{
		Id:          "group",
		DisplayName: "Group",
		Traits:      []v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP},
	}.Build()
	for i := 0; i < users; i++ {
		id := fmt.Sprintf("user-%05d", i)
		user, err := rs.NewUserResource(id, c.userType, id, nil)
		if err != nil {
			return nil, err
		}
		c.users = append(c.users, user)
	}
	for i := 0; i < groups; i++ {
		id := fmt.Sprintf("group-%04d", i)
		group, err := rs.NewGroupResource(id, c.groupType, id, nil)
		if err != nil {
			return nil, err
		}
		c.groups = append(c.groups, group)
		key := group.GetId().GetResource()
		c.entsBy[key] = et.NewAssignmentEntitlement(group, "member", et.WithGrantableTo(c.userType))
		members := make([]*v2.Grant, 0, usersPerGroup)
		for k := 0; k < usersPerGroup; k++ {
			// Deterministic spread across the user population; distinct
			// within a group (k*13 < len(users) for the driver's sizes).
			user := c.users[(i*7+k*13)%len(c.users)]
			members = append(members, gt.NewGrant(group, "member", user.GetId()))
		}
		c.grantsBy[key] = members
	}
	return c, nil
}

// parsePageToken decodes the "off-N" cursor this connector hands out.
func parsePageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(token, "off-"))
	if err != nil {
		return 0, fmt.Errorf("bad page token %q: %w", token, err)
	}
	return n, nil
}

// pageOf returns items[start:start+size] plus the cursor for the next page.
func pageOf[T any](items []T, start, size int) ([]T, string) {
	end := start + size
	next := ""
	if end < len(items) {
		next = fmt.Sprintf("off-%d", end)
	} else {
		end = len(items)
	}
	return items[start:end], next
}

func (c *crashConnector) ListResourceTypes(
	_ context.Context, _ *v2.ResourceTypesServiceListResourceTypesRequest, _ ...grpc.CallOption,
) (*v2.ResourceTypesServiceListResourceTypesResponse, error) {
	return v2.ResourceTypesServiceListResourceTypesResponse_builder{
		List: []*v2.ResourceType{c.userType, c.groupType},
	}.Build(), nil
}

func (c *crashConnector) ListResources(
	_ context.Context, in *v2.ResourcesServiceListResourcesRequest, _ ...grpc.CallOption,
) (*v2.ResourcesServiceListResourcesResponse, error) {
	time.Sleep(c.delay)
	start, err := parsePageToken(in.GetPageToken())
	if err != nil {
		return nil, err
	}
	var pool []*v2.Resource
	switch in.GetResourceTypeId() {
	case "user":
		pool = c.users
	case "group":
		pool = c.groups
	default:
		return nil, fmt.Errorf("unknown resource type %q", in.GetResourceTypeId())
	}
	page, next := pageOf(pool, start, resourcePageSize)
	return v2.ResourcesServiceListResourcesResponse_builder{
		List:          page,
		NextPageToken: next,
	}.Build(), nil
}

func (c *crashConnector) ListEntitlements(
	_ context.Context, in *v2.EntitlementsServiceListEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListEntitlementsResponse, error) {
	ent, ok := c.entsBy[in.GetResource().GetId().GetResource()]
	if !ok {
		// Empty pages are free so the page delay only prices real work.
		return v2.EntitlementsServiceListEntitlementsResponse_builder{}.Build(), nil
	}
	time.Sleep(c.delay)
	return v2.EntitlementsServiceListEntitlementsResponse_builder{
		List: []*v2.Entitlement{ent},
	}.Build(), nil
}

func (c *crashConnector) ListStaticEntitlements(
	_ context.Context, _ *v2.EntitlementsServiceListStaticEntitlementsRequest, _ ...grpc.CallOption,
) (*v2.EntitlementsServiceListStaticEntitlementsResponse, error) {
	return v2.EntitlementsServiceListStaticEntitlementsResponse_builder{}.Build(), nil
}

func (c *crashConnector) ListGrants(
	_ context.Context, in *v2.GrantsServiceListGrantsRequest, _ ...grpc.CallOption,
) (*v2.GrantsServiceListGrantsResponse, error) {
	start, err := parsePageToken(in.GetPageToken())
	if err != nil {
		return nil, err
	}
	members := c.grantsBy[in.GetResource().GetId().GetResource()]
	if len(members) == 0 {
		// Empty pages are free so the page delay only prices real work.
		return v2.GrantsServiceListGrantsResponse_builder{}.Build(), nil
	}
	time.Sleep(c.delay)
	page, next := pageOf(members, start, grantPageSize)
	return v2.GrantsServiceListGrantsResponse_builder{
		List:          page,
		NextPageToken: next,
	}.Build(), nil
}

func (c *crashConnector) GetMetadata(
	_ context.Context, _ *v2.ConnectorServiceGetMetadataRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceGetMetadataResponse, error) {
	return v2.ConnectorServiceGetMetadataResponse_builder{
		Metadata: v2.ConnectorMetadata_builder{DisplayName: "crash-harness"}.Build(),
	}.Build(), nil
}

func (c *crashConnector) Validate(
	_ context.Context, _ *v2.ConnectorServiceValidateRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceValidateResponse, error) {
	return v2.ConnectorServiceValidateResponse_builder{}.Build(), nil
}

func (c *crashConnector) Cleanup(
	_ context.Context, _ *v2.ConnectorServiceCleanupRequest, _ ...grpc.CallOption,
) (*v2.ConnectorServiceCleanupResponse, error) {
	return v2.ConnectorServiceCleanupResponse_builder{}.Build(), nil
}

// harnessResult is the machine-readable summary the driver parses from the
// final HARNESS_RESULT stdout line.
type harnessResult struct {
	Complete    bool   `json:"complete"`
	NotComplete bool   `json:"not_complete"`
	SyncErr     string `json:"sync_err,omitempty"`
}

func run() error {
	c1zPath := flag.String("c1z", "", "path to the c1z file")
	mode := flag.String("mode", "default", "connector scenario: default or chaos-lifecycle-retain")
	users := flag.Int("users", 3000, "number of user resources to serve")
	groups := flag.Int("groups", 150, "number of group resources to serve")
	pageDelayMs := flag.Int("page-delay-ms", 0, "sleep per paginated connector call")
	runDurationMs := flag.Int("run-duration-ms", 0, "session budget; 0 runs to completion")
	flag.Parse()
	if *c1zPath == "" {
		return errors.New("usage: -c1z path [-users n] [-groups n] [-page-delay-ms n] [-run-duration-ms n]")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	ctx = ctxzap.ToContext(ctx, logger)

	var connector types.ConnectorClient
	switch *mode {
	case "default":
		connector, err = newCrashConnector(*users, *groups, time.Duration(*pageDelayMs)*time.Millisecond)
		if err != nil {
			return err
		}
	case "chaos-lifecycle-retain":
		corpusCase, ok := chaosconnector.LifecycleCaseByName(chaosconnector.LifecycleRetainCaseName)
		if !ok {
			return errors.New("chaos lifecycle retain scenario is unavailable")
		}
		scenario, scenarioErr := corpusCase.BuildResume()
		if scenarioErr != nil {
			return scenarioErr
		}
		schedule := chaosconnector.NewSchedule()
		if *pageDelayMs > 0 {
			schedule = chaosconnector.NewSchedule(chaosconnector.Rule{
				ID: "delay-resume-page",
				Match: chaosconnector.Matcher{
					Domain:       chaosconnector.DomainConnector,
					Method:       chaosconnector.ExactString("ListEntitlements"),
					ResourceType: chaosconnector.ExactString(chaosconnector.FullCapabilityResourceTypeID),
					PageToken:    chaosconnector.ExactString("cut"),
					Phase:        chaosconnector.PhaseBeforeCall,
				},
				Effects: []chaosconnector.Effect{{
					Kind:  chaosconnector.EffectDelay,
					Delay: int64(*pageDelayMs),
				}},
				MinFires: 1,
			})
		}
		chaosRun, runErr := chaosconnector.NewRun(scenario, schedule)
		if runErr != nil {
			return runErr
		}
		builder, builderErr := chaosconnector.NewBuilder(chaosRun)
		if builderErr != nil {
			return builderErr
		}
		server, serverErr := builder.Server(ctx)
		if serverErr != nil {
			return serverErr
		}
		connector = chaosconnector.NewDirectClient(ctx, server, chaosRun)
	default:
		return fmt.Errorf("unknown mode %q", *mode)
	}

	opts := []sdksync.SyncOpt{
		sdksync.WithC1ZPath(*c1zPath),
		sdksync.WithTmpDir(os.TempDir()),
	}
	if engine := os.Getenv("BATON_STORAGE_ENGINE"); engine != "" {
		opts = append(opts, sdksync.WithStorageEngine(c1zstore.Engine(engine)))
	}
	if workers := os.Getenv("BATON_WORKERS"); workers != "" {
		n, err := strconv.Atoi(workers)
		if err != nil {
			return fmt.Errorf("bad BATON_WORKERS %q: %w", workers, err)
		}
		if n > 0 {
			opts = append(opts, sdksync.WithWorkerCount(n))
		}
	}
	if *runDurationMs > 0 {
		opts = append(opts, sdksync.WithRunDuration(time.Duration(*runDurationMs)*time.Millisecond))
	}

	s, err := sdksync.NewSyncer(ctx, connector, opts...)
	if err != nil {
		return fmt.Errorf("new syncer: %w", err)
	}
	syncErr := s.Sync(ctx)
	if closeErr := s.Close(ctx); closeErr != nil {
		return fmt.Errorf("close: %w", closeErr)
	}

	result := harnessResult{Complete: syncErr == nil}
	if syncErr != nil {
		result.SyncErr = syncErr.Error()
		result.NotComplete = errors.Is(syncErr, sdksync.ErrSyncNotComplete)
		if !result.NotComplete {
			return fmt.Errorf("sync: %w", syncErr)
		}
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	fmt.Printf("HARNESS_RESULT %s\n", encoded) //nolint:forbidigo // the result line is the driver protocol, not logging
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}
}
