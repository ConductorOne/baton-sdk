package connectorrunner

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	engine "github.com/conductorone/baton-sdk/pkg/dotc1z/engine/pebble"
	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

type rollbackAPI struct {
	v1.UnimplementedBatonServiceServer
	mu                      sync.Mutex
	taskID                  string
	claimed                 bool
	finished                *v1.BatonServiceFinishTaskRequest
	upload                  []byte
	idle                    chan struct{}
	hello, heartbeats, auth int
	batched                 bool
}

func (s *rollbackAPI) arm(id string, newProcess bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.taskID, s.claimed, s.finished, s.upload, s.idle = id, false, nil, nil, make(chan struct{}, 1)
	s.heartbeats = 0
	if newProcess {
		s.hello, s.auth = 0, 0
	}
}
func (s *rollbackAPI) Hello(context.Context, *v1.BatonServiceHelloRequest) (*v1.BatonServiceHelloResponse, error) {
	s.mu.Lock()
	s.hello++
	s.mu.Unlock()
	return &v1.BatonServiceHelloResponse{}, nil
}
func (s *rollbackAPI) next() *v1.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished != nil {
		select {
		case s.idle <- struct{}{}:
		default:
		}
	}
	if s.claimed {
		return nil
	}
	s.claimed = true
	return v1.Task_builder{Id: s.taskID, SyncFull: v1.Task_SyncFullTask_builder{
		StorageEngine: "pebble", SkipEntitlementsAndGrants: true, SkipExpandGrants: true,
	}.Build()}.Build()
}
func (s *rollbackAPI) GetTask(context.Context, *v1.BatonServiceGetTaskRequest) (*v1.BatonServiceGetTaskResponse, error) {
	if s.batched {
		return nil, fmt.Errorf("expected batched task polling")
	}
	return v1.BatonServiceGetTaskResponse_builder{Task: s.next(), NextPoll: durationpb.New(20 * time.Millisecond)}.Build(), nil
}
func (s *rollbackAPI) GetTasks(context.Context, *v1.BatonServiceGetTasksRequest) (*v1.BatonServiceGetTasksResponse, error) {
	if !s.batched {
		return nil, fmt.Errorf("expected single-task polling")
	}
	var tasks []*v1.Task
	if task := s.next(); task != nil {
		tasks = append(tasks, task)
	}
	return v1.BatonServiceGetTasksResponse_builder{Tasks: tasks, NextPoll: durationpb.New(20 * time.Millisecond)}.Build(), nil
}
func (s *rollbackAPI) Heartbeat(context.Context, *v1.BatonServiceHeartbeatRequest) (*v1.BatonServiceHeartbeatResponse, error) {
	s.mu.Lock()
	s.heartbeats++
	s.mu.Unlock()
	return v1.BatonServiceHeartbeatResponse_builder{NextHeartbeat: durationpb.New(time.Second)}.Build(), nil
}
func (s *rollbackAPI) FinishTask(_ context.Context, req *v1.BatonServiceFinishTaskRequest) (*v1.BatonServiceFinishTaskResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetTaskId() != s.taskID {
		return nil, fmt.Errorf("wrong completed task %s", req.GetTaskId())
	}
	s.finished = req
	return &v1.BatonServiceFinishTaskResponse{}, nil
}
func (s *rollbackAPI) UploadAsset(stream grpc.ClientStreamingServer[v1.BatonServiceUploadAssetRequest, v1.BatonServiceUploadAssetResponse]) error {
	var data []byte
	var checksum []byte
	var taskID string
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		if req.GetMetadata() != nil {
			taskID = req.GetMetadata().GetTaskId()
		}
		if req.GetData() != nil {
			data = append(data, req.GetData().GetData()...)
		}
		if req.GetEof() != nil {
			checksum = req.GetEof().GetSha256Checksum()
		}
	}
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], checksum) {
		return fmt.Errorf("upload checksum mismatch")
	}
	s.mu.Lock()
	if taskID != s.taskID {
		s.mu.Unlock()
		return fmt.Errorf("upload task mismatch")
	}
	s.upload = data
	s.mu.Unlock()
	return stream.SendAndClose(&v1.BatonServiceUploadAssetResponse{})
}
func (s *rollbackAPI) await(t *testing.T) (*v1.BatonServiceFinishTaskRequest, []byte) {
	t.Helper()
	select {
	case <-s.idle:
	case <-time.After(45 * time.Second):
		t.Fatal("daemon did not finish and return to polling")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Positive(t, s.auth)
	require.Positive(t, s.hello)
	require.Positive(t, s.heartbeats)
	return s.finished, bytes.Clone(s.upload)
}

type rollbackProcess struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
}

func startRollbackProcess(t *testing.T, binary, dir, mode, host, cert, secret string, keep, batched bool) *rollbackProcess {
	t.Helper()
	log, err := os.CreateTemp(t.TempDir(), "daemon-*.log")
	require.NoError(t, err)
	t.Cleanup(func() { _ = log.Close() })
	cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestLedgerServiceRollbackChild$", "-test.v") // #nosec G702 -- Operator-selected test binary; fixed arguments, no shell.
	cmd.Env = append(os.Environ(), "BATON_ROLLBACK_DIR="+dir, "BATON_ROLLBACK_MODE="+mode,
		"BATON_ROLLBACK_SECRET="+secret, "BATON_C1_API_HOST="+host, "SSL_CERT_FILE="+cert,
		fmt.Sprintf("BATON_ROLLBACK_KEEP=%t", keep), fmt.Sprintf("BATON_GET_TASKS=%t", batched))
	cmd.Stdout, cmd.Stderr = log, log
	require.NoError(t, cmd.Start())
	p := &rollbackProcess{cmd: cmd, done: make(chan struct{})}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		select {
		case <-p.done:
		case <-time.After(5 * time.Second):
			t.Error("daemon cleanup did not reap process")
		}
		if t.Failed() {
			b, _ := os.ReadFile(log.Name())
			t.Log(string(b))
		}
	})
	return p
}
func (p *rollbackProcess) stop(t *testing.T, kill bool) {
	t.Helper()
	if kill {
		require.NoError(t, p.cmd.Process.Kill())
	} else {
		require.NoError(t, p.cmd.Process.Signal(os.Interrupt))
	}
	select {
	case <-p.done:
		if kill {
			require.Error(t, p.err)
		} else {
			require.NoError(t, p.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not exit")
	}
}
func assertRollbackUpload(t *testing.T, finish *v1.BatonServiceFinishTaskRequest, data []byte) {
	t.Helper()
	require.NotNil(t, finish.GetSuccess(), "finish: %s", finish)
	require.Nil(t, finish.GetError())
	require.NotEmpty(t, data)
	file, err := os.CreateTemp(t.TempDir(), "uploaded-*.c1z")
	require.NoError(t, err)
	_, err = file.Write(data)
	require.NoError(t, err)
	require.NoError(t, file.Close())
	store, err := dotc1z.NewStore(t.Context(), file.Name(), dotc1z.WithReadOnly(true))
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close(t.Context())) }()
	require.Equal(t, "pebble", store.Metadata().Engine)
	resources, err := store.ListResources(t.Context(), &v2.ResourcesServiceListResourcesRequest{})
	require.NoError(t, err)
	var ids []string
	for _, r := range resources.GetList() {
		ids = append(ids, r.GetId().GetResource())
		require.Equal(t, r.GetId().GetResource(), r.GetDisplayName())
	}
	require.ElementsMatch(t, []string{"first", "second"}, ids)
	finished, err := store.SyncMeta().LatestFinishedSyncOfAnyType(t.Context())
	require.NoError(t, err)
	require.NotNil(t, finished)
}
func assertRollbackPartialLedger(t *testing.T, dir string) {
	t.Helper()
	dbs, err := filepath.Glob(filepath.Join(dir, "c1z-pebble*", "db"))
	require.NoError(t, err)
	found := false
	for _, db := range dbs {
		e, err := engine.Open(t.Context(), db, engine.WithReadOnly(true))
		require.NoError(t, err)
		pending, initialized, err := e.Ledger().PendingWork(t.Context(), 0, 100)
		require.NoError(t, err)
		if initialized && len(pending) > 0 {
			found = true
		}

		require.NoError(t, e.Close())
	}
	require.True(t, found, "killed new SDK must leave committed ledger work")
}
func TestLedgerServiceModeRollback(t *testing.T) {
	old := os.Getenv("BATON_ROLLBACK_OLD_BINARY")
	if old == "" {
		t.Skip("requires an old SDK test binary built with the daemon fixture")
	}
	current, err := os.Executable()
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	jwk, err := (&jose.JSONWebKey{Key: key}).MarshalJSON()
	require.NoError(t, err)
	secret := "fixture:fixture:v1:" + base64.RawURLEncoding.EncodeToString(jwk)
	for _, batched := range []bool{false, true} {
		for _, keep := range []bool{false, true} {
			for _, mode := range []string{"crash", "error"} {
				t.Run(fmt.Sprintf("batch-%t/spare-%t/%s", batched, keep, mode), func(t *testing.T) {
					api := &rollbackAPI{batched: batched}
					gs := grpc.NewServer()
					v1.RegisterBatonServiceServer(gs, api)
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/auth/v1/token" {
							api.mu.Lock()
							api.auth++
							api.mu.Unlock()
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"access_token":"fixture-token","token_type":"Bearer","expires_in":3600}`)
							return
						}
						gs.ServeHTTP(w, r)
					}))
					server.EnableHTTP2 = true
					server.StartTLS()
					t.Cleanup(func() { gs.Stop(); server.Close() })
					cert := filepath.Join(t.TempDir(), "ca.pem")
					require.NoError(t, os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw}), 0600))
					host := strings.TrimPrefix(server.URL, "https://")
					dir := t.TempDir()
					if keep {
						api.arm("warmup", true)
						warm := startRollbackProcess(t, current, dir, "success", host, cert, secret, keep, batched)
						finish, data := api.await(t)
						assertRollbackUpload(t, finish, data)
						warm.stop(t, false)
						spares, err := filepath.Glob(filepath.Join(dir, "baton-previous-sync-*"))
						require.NoError(t, err)
						require.Len(t, spares, 1)
					}
					api.arm("rollback-task", true)
					newer := startRollbackProcess(t, current, dir, mode, host, cert, secret, keep, batched)
					if mode == "crash" {
						require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(dir, "rollback-ready")); return err == nil }, 45*time.Second, 20*time.Millisecond)
						newer.stop(t, true)
						assertRollbackPartialLedger(t, dir)
					} else {
						finish, data := api.await(t)
						require.NotNil(t, finish.GetError())
						require.False(t, finish.GetError().GetNonRetryable())
						require.EqualValues(t, codes.PermissionDenied, finish.GetStatus().GetCode())
						require.Empty(t, data)
						newer.stop(t, false)
						partials, err := filepath.Glob(filepath.Join(dir, "baton-sdk-sync-upload*"))
						require.NoError(t, err)
						require.Empty(t, partials)
					}
					api.arm("rollback-task", true)
					older := startRollbackProcess(t, old, dir, "success", host, cert, secret, keep, batched)
					finish, data := api.await(t)
					assertRollbackUpload(t, finish, data)
					api.arm("subsequent-task", false)
					finish, data = api.await(t)
					assertRollbackUpload(t, finish, data)
					older.stop(t, false)
					api.arm("roll-forward", true)
					forward := startRollbackProcess(t, current, dir, "success", host, cert, secret, keep, batched)
					finish, data = api.await(t)
					assertRollbackUpload(t, finish, data)
					forward.stop(t, false)
				})
			}
		}
	}
}
