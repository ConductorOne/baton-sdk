package sync

import (
	"sync"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/stretchr/testify/require"
)

func TestGrantPool_AcquireReturnsValidGrant(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	g := pool.Acquire()
	require.NotNil(t, g)

	// Should be a zero-value grant
	require.Empty(t, g.GetId())
	require.Nil(t, g.GetEntitlement())
	require.Nil(t, g.GetPrincipal())
}

func TestGrantPool_AcquiredGrantsCanBePopulated(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	g := pool.Acquire()

	// Populate the grant
	g.Id = "test-grant-id"
	g.Entitlement = &v2.Entitlement{
		Id: "test-entitlement-id",
		Resource: &v2.Resource{
			Id: &v2.ResourceId{
				ResourceType: "group",
				Resource:     "group-123",
			},
		},
	}
	g.Principal = &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "user-456",
		},
	}

	// Verify the data is set
	require.Equal(t, "test-grant-id", g.GetId())
	require.Equal(t, "test-entitlement-id", g.GetEntitlement().GetId())
	require.Equal(t, "user-456", g.GetPrincipal().GetId().GetResource())
}

func TestGrantPool_ReleaseResetsGrants(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	// Acquire and populate a grant
	g := pool.Acquire()
	g.Id = "test-grant-id"
	g.Entitlement = &v2.Entitlement{Id: "ent-id"}
	g.Principal = &v2.Resource{Id: &v2.ResourceId{Resource: "user-123"}}

	// Release it
	pool.Release([]*v2.Grant{g})

	// Acquire again - should get a clean grant
	g2 := pool.Acquire()
	require.NotNil(t, g2)

	// The grant should be reset (no data from previous use)
	require.Empty(t, g2.GetId())
	require.Nil(t, g2.GetEntitlement())
	require.Nil(t, g2.GetPrincipal())
}

func TestGrantPool_ReleaseMultipleGrants(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	grants := make([]*v2.Grant, 100)
	for i := 0; i < 100; i++ {
		grants[i] = pool.Acquire()
		grants[i].Id = "grant-id"
	}

	// Release all at once
	pool.Release(grants)

	// Acquire new grants - all should be clean
	for i := 0; i < 100; i++ {
		g := pool.Acquire()
		require.NotNil(t, g)
		require.Empty(t, g.GetId(), "Grant %d should be reset", i)
	}
}

func TestGrantPool_ConcurrentAccess(t *testing.T) {
	var wg sync.WaitGroup

	// Note: sync.Pool is safe for concurrent Get/Put, but caller-side
	// tracking of grants needs its own synchronization if shared.
	// This test verifies the pool itself works when each goroutine
	// uses its own pool (the expected use case).

	numGoroutines := 10
	grantsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			// Each goroutine uses its own pool (realistic usage pattern)
			localPool := dotc1z.NewGrantPool()
			localGrants := make([]*v2.Grant, 0, grantsPerGoroutine)

			for j := 0; j < grantsPerGoroutine; j++ {
				g := localPool.Acquire()
				require.NotNil(t, g)
				g.Id = "worker-grant"
				localGrants = append(localGrants, g)
			}

			localPool.Release(localGrants)

			// Verify grants are clean after release
			g := localPool.Acquire()
			require.Empty(t, g.GetId())
		}(i)
	}

	wg.Wait()
}

func TestGrantPool_NoDataLeakageBetweenReleaseAndAcquire(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	// First cycle: populate with sensitive data
	g1 := pool.Acquire()
	g1.Id = "sensitive-id-12345"
	g1.Entitlement = &v2.Entitlement{
		Id:          "secret-entitlement",
		DisplayName: "Admin Access",
		Description: "Full admin privileges",
	}
	g1.Principal = &v2.Resource{
		Id: &v2.ResourceId{
			ResourceType: "user",
			Resource:     "admin-user",
		},
		DisplayName: "Administrator",
	}

	pool.Release([]*v2.Grant{g1})

	// Second cycle: acquire and verify no leakage
	g2 := pool.Acquire()

	require.Empty(t, g2.GetId(), "ID should be empty after reset")
	require.Nil(t, g2.GetEntitlement(), "Entitlement should be nil after reset")
	require.Nil(t, g2.GetPrincipal(), "Principal should be nil after reset")
}

func TestGrantPool_LargeScale(t *testing.T) {
	pool := dotc1z.NewGrantPool()
	const numGrants = 10000

	// Simulate real-world usage: acquire many grants
	grants := make([]*v2.Grant, numGrants)
	for i := 0; i < numGrants; i++ {
		grants[i] = pool.Acquire()
		grants[i].Id = "grant-id"
		grants[i].Entitlement = &v2.Entitlement{
			Id: "entitlement-id",
			Resource: &v2.Resource{
				Id: &v2.ResourceId{
					ResourceType: "type",
					Resource:     "resource",
				},
			},
		}
	}

	// Release all
	pool.Release(grants)

	// Re-acquire and verify all are clean
	for i := 0; i < numGrants; i++ {
		g := pool.Acquire()
		require.Empty(t, g.GetId(), "Grant %d should have empty ID", i)
		require.Nil(t, g.GetEntitlement(), "Grant %d should have nil entitlement", i)
	}
}

func TestGrantPool_RepeatedCycles(t *testing.T) {
	pool := dotc1z.NewGrantPool()
	const cycles = 5
	const grantsPerCycle = 100

	for cycle := 0; cycle < cycles; cycle++ {
		// Acquire and populate
		grants := make([]*v2.Grant, grantsPerCycle)
		for i := 0; i < grantsPerCycle; i++ {
			grants[i] = pool.Acquire()
			grants[i].Id = "cycle-grant"
			grants[i].Entitlement = &v2.Entitlement{Id: "ent"}
		}

		// Release
		pool.Release(grants)
	}

	// Final verification
	g := pool.Acquire()
	require.Empty(t, g.GetId())
	require.Nil(t, g.GetEntitlement())
}

func TestGrantPool_NilHandling(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	// Create a slice with some nil values
	grants := make([]*v2.Grant, 10)
	for i := 0; i < 10; i++ {
		if i%2 == 0 {
			grants[i] = pool.Acquire()
			grants[i].Id = "has-id"
		}
		// Leave odd indices nil
	}

	// Release should handle nil grants gracefully
	require.NotPanics(t, func() {
		pool.Release(grants)
	})

	// Subsequent acquire should work
	g := pool.Acquire()
	require.NotNil(t, g)
}

func TestGrantPool_ReleaseEmptySlice(t *testing.T) {
	pool := dotc1z.NewGrantPool()

	// Releasing an empty slice should not panic
	require.NotPanics(t, func() {
		pool.Release([]*v2.Grant{})
		pool.Release(nil)
	})
}

func TestGrantPool_CallerManagesTracking(t *testing.T) {
	// This test demonstrates the expected usage pattern where the caller
	// tracks which grants came from the pool and releases them explicitly.
	pool := dotc1z.NewGrantPool()

	// Simulate processing multiple pages of grants
	for page := 0; page < 3; page++ {
		// Track grants from this page
		pageGrants := make([]*v2.Grant, 50)
		for i := 0; i < 50; i++ {
			pageGrants[i] = pool.Acquire()
			pageGrants[i].Id = "page-grant"
		}

		// Process the grants (simulated)
		// ...

		// Release after processing
		pool.Release(pageGrants)
	}

	// All grants should be clean
	g := pool.Acquire()
	require.Empty(t, g.GetId())
}
