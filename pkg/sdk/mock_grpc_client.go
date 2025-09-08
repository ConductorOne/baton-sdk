package sdk

import (
	"context"

	"google.golang.org/grpc/metadata"
)

type MockBidiClient[Req any, Resp any] struct {
}

func (m MockBidiClient[Req, Resp]) Send(req *Req) error {
	return nil
}

func (m MockBidiClient[Req, Resp]) Recv() (*Resp, error) {
	var res Resp
	return &res, nil
}

func (m MockBidiClient[Req, Resp]) Header() (metadata.MD, error) {
	return nil, nil
}

func (m MockBidiClient[Req, Resp]) Trailer() metadata.MD {
	return nil
}

func (m MockBidiClient[Req, Resp]) CloseSend() error {
	return nil
}

func (m MockBidiClient[Req, Resp]) Context() context.Context {
	return context.Background()
}

func (m MockBidiClient[Req, Resp]) SendMsg(msg any) error {
	return nil
}

func (m MockBidiClient[Req, Resp]) RecvMsg(msg any) error {
	return nil
}
