package nodes

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
	Create(context.Context, *CreateRequest) (*NoReply, error)
	SetTitle(context.Context, *SetTitleRequest) (*NoReply, error)
	SetNotes(context.Context, *SetNotesRequest) (*NoReply, error)
	AddFiles(context.Context, *AddFilesRequest) (*NoReply, error)
	RemoveFiles(context.Context, *RemoveFilesRequest) (*NoReply, error)
	Get(context.Context, *GetRequest) (*GetReply, error)
}

type ServiceClient interface {
	Create(context.Context, *CreateRequest, ...transport.RequestOption) (*NoReply, error)
	SetTitle(context.Context, *SetTitleRequest, ...transport.RequestOption) (*NoReply, error)
	SetNotes(context.Context, *SetNotesRequest, ...transport.RequestOption) (*NoReply, error)
	AddFiles(context.Context, *AddFilesRequest, ...transport.RequestOption) (*NoReply, error)
	RemoveFiles(context.Context, *RemoveFilesRequest, ...transport.RequestOption) (*NoReply, error)
	Get(context.Context, *GetRequest, ...transport.RequestOption) (*GetReply, error)
}

// serviceClient an implementation of Service client.
type serviceClient struct {
	tp transport.Transport
}

func (c *serviceClient) Create(ctx context.Context, req *CreateRequest, opts ...transport.RequestOption) (*NoReply, error) {
	var rep NoReply

	_, err := c.tp.Request("nodes.Create", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) SetTitle(ctx context.Context, req *SetTitleRequest, opts ...transport.RequestOption) (*NoReply, error) {
	var rep NoReply

	_, err := c.tp.Request("nodes.SetTitle", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) SetNotes(ctx context.Context, req *SetNotesRequest, opts ...transport.RequestOption) (*NoReply, error) {
	var rep NoReply

	_, err := c.tp.Request("nodes.SetNotes", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) AddFiles(ctx context.Context, req *AddFilesRequest, opts ...transport.RequestOption) (*NoReply, error) {
	var rep NoReply

	_, err := c.tp.Request("nodes.AddFiles", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) RemoveFiles(ctx context.Context, req *RemoveFilesRequest, opts ...transport.RequestOption) (*NoReply, error) {
	var rep NoReply

	_, err := c.tp.Request("nodes.RemoveFiles", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) Get(ctx context.Context, req *GetRequest, opts ...transport.RequestOption) (*GetReply, error) {
	var rep GetReply

	_, err := c.tp.Request("nodes.Get", req, &rep, opts...)
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

	_, err = s.tp.Subscribe("nodes.Create", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req CreateRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.Create(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("nodes.SetTitle", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req SetTitleRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.SetTitle(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("nodes.SetNotes", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req SetNotesRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.SetNotes(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("nodes.AddFiles", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req AddFilesRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.AddFiles(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("nodes.RemoveFiles", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req RemoveFilesRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.RemoveFiles(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("nodes.Get", func(msg *transport.Message) (proto.Message, error) {
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
