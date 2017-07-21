package data

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/golang/protobuf/proto"
)

var (
	traceIdKey = struct{}{}
)

type Service interface {
	Import(context.Context, *ImportRequest) (*ImportReply, error)
	Upload(context.Context, *UploadRequest) (*UploadReply, error)
	Describe(context.Context, *DescribeRequest) (*DescribeReply, error)
	Update(context.Context, *UpdateRequest) (*UpdateReply, error)
	Get(context.Context, *GetRequest) (*GetReply, error)
}

type ServiceClient interface {
	Import(context.Context, *ImportRequest, ...transport.RequestOption) (*ImportReply, error)
	Upload(context.Context, *UploadRequest, ...transport.RequestOption) (*UploadReply, error)
	Describe(context.Context, *DescribeRequest, ...transport.RequestOption) (*DescribeReply, error)
	Update(context.Context, *UpdateRequest, ...transport.RequestOption) (*UpdateReply, error)
	Get(context.Context, *GetRequest, ...transport.RequestOption) (*GetReply, error)
}

// serviceClient an implementation of Service client.
type serviceClient struct {
	tp transport.Transport
}

func (c *serviceClient) Import(ctx context.Context, req *ImportRequest, opts ...transport.RequestOption) (*ImportReply, error) {
	var rep ImportReply

	_, err := c.tp.Request("data.Import", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Upload(ctx context.Context, req *UploadRequest, opts ...transport.RequestOption) (*UploadReply, error) {
	var rep UploadReply

	_, err := c.tp.Request("data.Upload", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Describe(ctx context.Context, req *DescribeRequest, opts ...transport.RequestOption) (*DescribeReply, error) {
	var rep DescribeReply

	_, err := c.tp.Request("data.Describe", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Update(ctx context.Context, req *UpdateRequest, opts ...transport.RequestOption) (*UpdateReply, error) {
	var rep UpdateReply

	_, err := c.tp.Request("data.Update", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Get(ctx context.Context, req *GetRequest, opts ...transport.RequestOption) (*GetReply, error) {
	var rep GetReply

	_, err := c.tp.Request("data.Get", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

// NewServiceClient creates a new Service client.
func NewServiceClient(tp transport.Transport) ServiceClient {
	return &serviceClient{tp}
}

type ServiceServer struct {
	tp  transport.Transport
	svc Service
}

func NewServiceServer(tp transport.Transport, svc Service) *ServiceServer {
	return &ServiceServer{
		tp:  tp,
		svc: svc,
	}
}

func (s *ServiceServer) Serve(ctx context.Context, opts ...transport.SubscribeOption) error {
	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()
	}()

	var err error

	_, err = s.tp.Subscribe("data.Import", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req ImportRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Import(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("data.Upload", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req UploadRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Upload(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("data.Describe", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req DescribeRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Describe(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("data.Update", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req UpdateRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Update(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("data.Get", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req GetRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Get(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan

	return nil
}
