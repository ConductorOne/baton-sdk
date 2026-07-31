package connectorrunner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	connectorwrapperV1 "github.com/conductorone/baton-sdk/pb/c1/connector_wrapper/v1"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestRun_DoesNotPollAllFreeSlotsWhenNoTasksAvailable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(ErrSigTerm)

	tm := newIdleTaskManager(time.Hour)
	runner := &connectorRunner{
		tasks:           tm,
		taskConcurrency: 3,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.run(ctx)
	}()

	select {
	case <-tm.firstNext:
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for first Next call")
	}

	select {
	case <-tm.secondNext:
		require.Fail(t, fmt.Sprintf("expected one Next call before next poll interval, got %d", tm.nextCalls.Load()))
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, int64(1), tm.nextCalls.Load())
	cancel(ErrSigTerm)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for runner to stop")
	}
}

func TestRun_TaskConcurrencyCapsProcessing(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(ErrSigTerm)

	tm := newBlockingTaskManager(5)
	runner := &connectorRunner{
		cw:              noopClientWrapper{},
		tasks:           tm,
		taskConcurrency: 2,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.run(ctx)
	}()

	waitForStartedTasks(t, tm.started, 2)

	select {
	case <-tm.started:
		require.Fail(t, fmt.Sprintf("started more than %d tasks without a free slot", runner.taskConcurrency))
	case <-time.After(50 * time.Millisecond):
	}

	require.Equal(t, int64(2), tm.nextCalls.Load(), "runner should not claim a third task before a slot is free")
	require.LessOrEqual(t, tm.maxActive.Load(), int64(2))

	tm.releaseOne()
	waitForStartedTasks(t, tm.started, 1)

	require.Equal(t, int64(3), tm.nextCalls.Load(), "runner should claim the next task after a slot is free")
	require.LessOrEqual(t, tm.maxActive.Load(), int64(2))

	cancel(ErrSigTerm)
	tm.releaseAll()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for runner to stop")
	}
}

// TestRun_DrainsInFlightTaskBeforeReturningOnCancellation covers the gap
// between catching SIGTERM and actually protecting an in-flight sync's
// forced checkpoint: every task is dispatched to an untracked goroutine
// (`go func(t *v1.Task)` in run()), and every caller of this runner closes
// the connector client (Close -> c.cw.Close()) immediately after Run()
// returns. Without draining, run() returns as soon as ctx is Done
// regardless of what that goroutine is still doing -- including a
// checkpoint write deliberately running on a context.WithoutCancel(ctx)
// scope specifically so cancellation doesn't cut it off.
//
// detachedTaskManager's Process ignores ctx.Done() (mirroring that
// detached-context write) and only returns when explicitly released, so
// this test can assert run() actually waits for it instead of just
// hoping the timing works out.
func TestRun_DrainsInFlightTaskBeforeReturningOnCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(ErrSigTerm)

	tm := newDetachedTaskManager()
	runner := &connectorRunner{
		cw:              noopClientWrapper{},
		tasks:           tm,
		taskConcurrency: 1,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- runner.run(ctx)
	}()

	waitForStartedTasks(t, tm.started, 1)

	cancel(ErrSigTerm)

	select {
	case <-errCh:
		require.Fail(t, "run() returned before the in-flight task finished; the shutdown drain did not wait for it")
	case <-time.After(100 * time.Millisecond):
	}

	close(tm.release)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "timed out waiting for runner to stop after releasing the in-flight task")
	}
}

func waitForStartedTasks(t *testing.T, started <-chan struct{}, count int) {
	t.Helper()
	for range count {
		select {
		case <-started:
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("timed out waiting for %d started task(s)", count))
		}
	}
}

type idleTaskManager struct {
	nextPoll   time.Duration
	nextCalls  atomic.Int64
	once       sync.Once
	firstNext  chan struct{}
	secondNext chan struct{}
}

func newIdleTaskManager(nextPoll time.Duration) *idleTaskManager {
	return &idleTaskManager{
		nextPoll:   nextPoll,
		firstNext:  make(chan struct{}),
		secondNext: make(chan struct{}),
	}
}

func (m *idleTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	calls := m.nextCalls.Add(1)
	m.once.Do(func() {
		close(m.firstNext)
	})
	if calls == 2 {
		close(m.secondNext)
	}

	return nil, m.nextPoll, nil
}

func (m *idleTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	return errors.New("idleTaskManager should not process tasks")
}

func (m *idleTaskManager) ShouldDebug() bool {
	return false
}

func (m *idleTaskManager) GetTempDir() string {
	return ""
}

type blockingTaskManager struct {
	totalTasks int
	nextCalls  atomic.Int64
	active     atomic.Int64
	maxActive  atomic.Int64
	started    chan struct{}
	release    chan struct{}
}

func newBlockingTaskManager(totalTasks int) *blockingTaskManager {
	return &blockingTaskManager{
		totalTasks: totalTasks,
		started:    make(chan struct{}, totalTasks),
		release:    make(chan struct{}),
	}
}

func (m *blockingTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	call := m.nextCalls.Add(1)
	if int(call) > m.totalTasks {
		return nil, time.Hour, nil
	}

	return v1.Task_builder{
		Id:     fmt.Sprintf("task-%d", call),
		Status: v1.Task_STATUS_PENDING,
		Hello:  &v1.Task_HelloTask{},
	}.Build(), 0, nil
}

func (m *blockingTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	active := m.active.Add(1)
	m.updateMaxActive(active)
	defer m.active.Add(-1)

	m.started <- struct{}{}

	select {
	case <-m.release:
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (m *blockingTaskManager) updateMaxActive(active int64) {
	for {
		current := m.maxActive.Load()
		if active <= current {
			return
		}
		if m.maxActive.CompareAndSwap(current, active) {
			return
		}
	}
}

func (m *blockingTaskManager) releaseOne() {
	m.release <- struct{}{}
}

func (m *blockingTaskManager) releaseAll() {
	close(m.release)
}

func (m *blockingTaskManager) ShouldDebug() bool {
	return false
}

func (m *blockingTaskManager) GetTempDir() string {
	return ""
}

// detachedTaskManager's Process deliberately does not select on ctx.Done()
// -- it mirrors a sync's forced checkpoint write, which runs on a
// context.WithoutCancel(ctx) scope specifically so the caller's cancellation
// doesn't cut it off. It only returns once explicitly released.
type detachedTaskManager struct {
	nextCalls atomic.Int64
	started   chan struct{}
	release   chan struct{}
}

func newDetachedTaskManager() *detachedTaskManager {
	return &detachedTaskManager{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (m *detachedTaskManager) Next(ctx context.Context) (*v1.Task, time.Duration, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}
	if m.nextCalls.Add(1) > 1 {
		return nil, time.Hour, nil
	}
	return v1.Task_builder{
		Id:     "task-1",
		Status: v1.Task_STATUS_PENDING,
		Hello:  &v1.Task_HelloTask{},
	}.Build(), 0, nil
}

func (m *detachedTaskManager) Process(ctx context.Context, task *v1.Task, cc types.ConnectorClient) error {
	m.started <- struct{}{}
	<-m.release
	return nil
}

func (m *detachedTaskManager) ShouldDebug() bool {
	return false
}

func (m *detachedTaskManager) GetTempDir() string {
	return ""
}

type noopClientWrapper struct{}

func (noopClientWrapper) C(ctx context.Context) (types.ConnectorClient, error) {
	return nil, nil
}

func (noopClientWrapper) Run(ctx context.Context, cfg *connectorwrapperV1.ServerConfig) error {
	return nil
}

func (noopClientWrapper) Close() error {
	return nil
}

func TestExtractDefaultCapabilitiesConnectorFactory(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil when no factory option is set", func(t *testing.T) {
		f, err := ExtractDefaultCapabilitiesConnectorFactory(ctx)
		require.NoError(t, err)
		require.Nil(t, f)
	})

	t.Run("returns the registered factory", func(t *testing.T) {
		sentinel := errors.New("from factory")
		factory := func(ctx context.Context) (types.ConnectorServer, error) {
			return nil, sentinel
		}

		f, err := ExtractDefaultCapabilitiesConnectorFactory(ctx, WithDefaultCapabilitiesConnectorFactory(factory))
		require.NoError(t, err)
		require.NotNil(t, f)

		// Confirm it's the factory we registered by observing its behavior.
		_, gotErr := f(ctx)
		require.ErrorIs(t, gotErr, sentinel)
	})
}
