package tasks

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/segmentio/ksuid"
)

type Task interface {
	GetTaskType() string
	GetTaskId() string
}

type Manager interface {
	Next(ctx context.Context) (Task, error)
	Finish(ctx context.Context, taskID string) error
	Add(ctx context.Context, task Task) error
}

type naiveManager struct {
	tasks      []Task
	nextTaskID int
}

func (m *naiveManager) Next(ctx context.Context) (Task, error) {
	if len(m.tasks) == 0 {
		return nil, nil
	}

	ret := m.tasks[m.nextTaskID]

	return ret, nil
}

func (m *naiveManager) Add(ctx context.Context, tsk Task) error {
	m.tasks = append(m.tasks, tsk)

	return nil
}

func (m *naiveManager) Finish(ctx context.Context, taskID string) error {
	idx := -1

	for ii, t := range m.tasks {
		if t.GetTaskId() == taskID {
			idx = ii
			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("unexpected task ID was provided: %s", taskID)
	}

	newTasks := m.tasks[:idx]
	// If this wasn't the last index in the slice, include everything after the idx that matches our task ID
	if idx != len(m.tasks)-1 {
		newTasks = append(newTasks, m.tasks[idx:]...)
	}

	m.tasks = newTasks

	return nil
}

func NewNaiveManager(ctx context.Context) (*naiveManager, error) {
	nm := &naiveManager{}

	fmt.Println("HUP THIS PID: ", os.Getpid())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)
	go func() {
		for range sigChan {
			err := nm.Add(ctx, NewSyncTask())
			if err != nil {
				panic(err)
			}
		}
	}()

	return nm, nil
}

type SyncTask struct {
	taskID string
}

func (s SyncTask) GetTaskType() string {
	return "sync"
}

func (s SyncTask) GetTaskId() string {
	return s.taskID
}

func NewSyncTask() *SyncTask {
	id := ksuid.New().String()
	return &SyncTask{taskID: id}
}
