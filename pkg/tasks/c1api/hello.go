package c1api

import (
	"context"
	"errors"
	"runtime/debug"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	v1 "github.com/conductorone/baton-sdk/pb/c1/connectorapi/baton/v1"
	"github.com/conductorone/baton-sdk/pkg/tasks"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/shirou/gopsutil/v3/host"
	"go.uber.org/zap"
)

type helloHelpers interface {
	ConnectorClient() types.ConnectorClient
	HelloClient() batonHelloClient
}

type helloTaskHandler struct {
	task    *v1.Task
	helpers helloHelpers
}

func (c *helloTaskHandler) osInfo(ctx context.Context) (*v1.BatonServiceHelloRequest_OSInfo, error) {
	l := ctxzap.Extract(ctx)

	info, err := host.InfoWithContext(ctx)
	if err != nil {
		l.Error("failed to get host info", zap.Error(err))
		return nil, err
	}

	if info.VirtualizationSystem == "" {
		info.VirtualizationSystem = "none"
	}

	return &v1.BatonServiceHelloRequest_OSInfo{
		Hostname:             info.Hostname,
		Os:                   info.OS,
		Platform:             info.Platform,
		PlatformFamily:       info.PlatformFamily,
		PlatformVersion:      info.PlatformVersion,
		KernelVersion:        info.KernelVersion,
		KernelArch:           info.KernelArch,
		VirtualizationSystem: info.VirtualizationSystem,
	}, nil
}

func (c *helloTaskHandler) buildInfo(ctx context.Context) *v1.BatonServiceHelloRequest_BuildInfo {
	l := ctxzap.Extract(ctx)

	bi, ok := debug.ReadBuildInfo()
	if !ok {
		l.Error("failed to get build info")
		return &v1.BatonServiceHelloRequest_BuildInfo{}
	}

	return &v1.BatonServiceHelloRequest_BuildInfo{
		LangVersion:    bi.GoVersion,
		Package:        bi.Main.Path,
		PackageVersion: bi.Main.Version,
	}
}

func (c *helloTaskHandler) HandleTask(ctx context.Context) error {
	if c.task == nil {
		return errors.New("cannot handle task: task is nil")
	}

	l := ctxzap.Extract(ctx).With(
		zap.String("task_id", c.task.GetId()),
		zap.Stringer("task_type", tasks.GetType(c.task)),
	)

	cc := c.helpers.ConnectorClient()
	mdResp, err := cc.GetMetadata(ctx, &v2.ConnectorServiceGetMetadataRequest{})
	if err != nil {
		return err
	}

	taskID := c.task.GetId()

	osInfo, err := c.osInfo(ctx)
	if err != nil {
		return err
	}
	_, err = c.helpers.HelloClient().Hello(ctx, &v1.BatonServiceHelloRequest{
		TaskId:            taskID,
		BuildInfo:         c.buildInfo(ctx),
		OsInfo:            osInfo,
		ConnectorMetadata: mdResp.GetMetadata(),
	})
	if err != nil {
		l.Error("failed while sending hello", zap.Error(err))
		return err
	}

	return nil
}

func newHelloTaskHandler(task *v1.Task, helpers helloHelpers) *helloTaskHandler {
	return &helloTaskHandler{
		task:    task,
		helpers: helpers,
	}
}
