package naive_manager

import (
	"context"
	"fmt"

	tasks2 "github.com/conductorone/baton-sdk/pkg/tasks"
)

type naiveManager struct {
	tasks      []tasks2.Task
	nextTaskID int
}

func (m *naiveManager) Next(ctx context.Context) (tasks2.Task, error) {
	if len(m.tasks) == 0 {
		return nil, nil
	}

	ret := m.tasks[m.nextTaskID]

	return ret, nil
}

func (m *naiveManager) Add(ctx context.Context, tsk tasks2.Task) error {
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

// NewNaiveManager returns a task manager that queues a sync task.
func NewNaiveManager(ctx context.Context) (*naiveManager, error) {
	nm := &naiveManager{}

	return nm, nil
}
