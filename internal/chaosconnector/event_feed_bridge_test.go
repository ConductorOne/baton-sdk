package chaosconnector

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/conductorone/baton-sdk/pkg/tasks/local"
	"github.com/conductorone/baton-sdk/pkg/types"
)

// TestBridge_LocalEventFeed drives the real pkg/tasks/local event feed task
// manager -- the CLI-facing code CE-1027 added -- against this chaos
// connector over a real client boundary, instead of the hand-rolled
// types.ConnectorClient fakes in pkg/tasks/local/event_feed_test.go.
// tasks.Manager.Process takes the client as a parameter, so any
// types.ConnectorClient this harness produces plugs in directly; no new
// harness machinery or subprocess is needed for this bridge.
func TestBridge_LocalEventFeed(t *testing.T) {
	adapters := []struct {
		name string
		open func(*testing.T, *Run, types.ConnectorServer) types.ConnectorClient
	}{
		{
			name: "direct",
			open: func(t *testing.T, run *Run, server types.ConnectorServer) types.ConnectorClient {
				return NewDirectClient(t.Context(), server, run)
			},
		},
		{
			name: "grpc",
			open: func(t *testing.T, run *Run, server types.ConnectorServer) types.ConnectorClient {
				client, err := NewGRPCClient(t.Context(), server, run, true, true)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, client.Close()) })
				return client
			},
		},
	}

	// NewFullScenario declares 3 events on ChaosEventFeedID (fixture.go), so
	// these sizes exercise 1 page, a partial-remainder page split, and a
	// fully single-page traversal.
	pageSizes := []uint32{1, 2, 100}

	for _, adapter := range adapters {
		for _, pageSize := range pageSizes {
			t.Run(fmt.Sprintf("%s/page-size-%d", adapter.name, pageSize), func(t *testing.T) {
				run, server := newFullConnector(t)
				cc := adapter.open(t, run, server)

				mgr := local.NewEventFeed(t.Context(), ChaosEventFeedID, time.Time{}, "", local.WithEventFeedPageSize(pageSize))
				task, _, err := mgr.Next(t.Context())
				require.NoError(t, err)
				require.NoError(t, mgr.Process(t.Context(), task, cc))

				cursors := listEventsRequestCursors(run)
				require.NotEmpty(t, cursors, "expected at least one ListEvents call")

				// Cursor chaining: offsets must strictly advance call to
				// call, proving each request carried forward the prior
				// response's cursor rather than resending a stale or root
				// token. Read from the harness's own trace oracle, not a
				// hand-rolled fake's captured-request list.
				prevOffset := -1
				for _, cursor := range cursors {
					offset, ok := decodeEventCursor(cursor)
					require.True(t, ok, "cursor %q must be one this feed produced", cursor)
					require.Greater(t, offset, prevOffset, "cursor offsets must strictly advance page to page")
					prevOffset = offset
				}
			})
		}
	}
}

// listEventsRequestCursors returns the ordered request cursor of every
// ListEvents call recorded in the run's trace. The trace is populated
// identically across all three client adapters (client.go's faultConn and
// the server-fault interceptor both record through Trace.Record), so this
// works whichever adapter drove the calls.
func listEventsRequestCursors(run *Run) []string {
	var out []string
	for _, event := range run.Trace().Events() {
		if event.Operation.Method != "ListEvents" {
			continue
		}
		out = append(out, event.Operation.PageToken)
	}
	return out
}
