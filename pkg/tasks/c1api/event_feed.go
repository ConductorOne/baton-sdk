package c1api

//
// type eventFeedHelper interface {
// 	ConnectorClient() types.ConnectorClient
// 	FinishTask(ctx context.Context, annos annotations.Annotations, err error) error
// 	HeartbeatTask(ctx context.Context, annos annotations.Annotations) (context.Context, error)
// }
//
// type eventFeedTaskHandler struct {
// 	task    *v1.Task
// 	helpers eventFeedHelper
// }
//
// func (c *eventFeedTaskHandler) sync(ctx context.Context) error {
// 	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))
//
// 	return nil
// }
//
// func (c *eventFeedTaskHandler) HandleTask(ctx context.Context) error {
// 	ctx, cancel := context.WithCancel(ctx)
// 	defer cancel()
//
// 	l := ctxzap.Extract(ctx).With(zap.String("task_id", c.task.GetId()), zap.Stringer("task_type", tasks.GetType(c.task)))
// 	l.Info("Handling event feed task.")
//
// 	var err error
// 	ctx, err = c.helpers.HeartbeatTask(ctx, nil)
// 	if err != nil {
// 		l.Error("failed to heartbeat task", zap.Error(err))
// 		return err
// 	}
//
// 	return c.helpers.FinishTask(ctx, nil, nil)
// }
//
// func newEventFeedTaskHandler(task *v1.Task, helpers fullSyncHelpers) tasks.TaskHandler {
// 	return &fullSyncTaskHandler{
// 		task:    task,
// 		helpers: helpers,
// 	}
// }
