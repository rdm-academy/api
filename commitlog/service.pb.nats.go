package commitlog

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
	Commit(context.Context, *CommitRequest) (*CommitReply, error)
	History(context.Context, *HistoryRequest) (*HistoryReply, error)
	Pending(context.Context, *PendingRequest) (*PendingReply, error)
}

type ServiceClient interface {
	Commit(context.Context, *CommitRequest, ...transport.RequestOption) (*CommitReply, error)
	History(context.Context, *HistoryRequest, ...transport.RequestOption) (*HistoryReply, error)
	Pending(context.Context, *PendingRequest, ...transport.RequestOption) (*PendingReply, error)
}

// serviceClient an implementation of Service client.
type serviceClient struct {
	tp transport.Transport
}

func (c *serviceClient) Commit(ctx context.Context, req *CommitRequest, opts ...transport.RequestOption) (*CommitReply, error) {
	var rep CommitReply

	_, err := c.tp.Request("commitlog.Commit", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) History(ctx context.Context, req *HistoryRequest, opts ...transport.RequestOption) (*HistoryReply, error) {
	var rep HistoryReply

	_, err := c.tp.Request("commitlog.History", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Pending(ctx context.Context, req *PendingRequest, opts ...transport.RequestOption) (*PendingReply, error) {
	var rep PendingReply

	_, err := c.tp.Request("commitlog.Pending", req, &rep, opts...)
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

	_, err = s.tp.Subscribe("commitlog.Commit", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req CommitRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Commit(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("commitlog.History", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req HistoryRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.History(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("commitlog.Pending", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req PendingRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Pending(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan

	return nil
}
