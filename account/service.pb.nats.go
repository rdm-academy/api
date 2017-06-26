package account

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
	CreateUser(context.Context, *CreateUserRequest) (*CreateUserResponse, error)
	DeleteUser(context.Context, *DeleteUserRequest) (*DeleteUserResponse, error)
	GetUser(context.Context, *GetUserRequest) (*GetUserResponse, error)
}

type ServiceClient interface {
	CreateUser(context.Context, *CreateUserRequest, ...transport.RequestOption) (*CreateUserResponse, error)
	DeleteUser(context.Context, *DeleteUserRequest, ...transport.RequestOption) (*DeleteUserResponse, error)
	GetUser(context.Context, *GetUserRequest, ...transport.RequestOption) (*GetUserResponse, error)
}

// serviceClient an implementation of Service client.
type serviceClient struct {
	tp transport.Transport
}

func (c *serviceClient) CreateUser(ctx context.Context, req *CreateUserRequest, opts ...transport.RequestOption) (*CreateUserResponse, error) {
	var rep CreateUserResponse

	_, err := c.tp.Request("account.CreateUser", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) DeleteUser(ctx context.Context, req *DeleteUserRequest, opts ...transport.RequestOption) (*DeleteUserResponse, error) {
	var rep DeleteUserResponse

	_, err := c.tp.Request("account.DeleteUser", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) GetUser(ctx context.Context, req *GetUserRequest, opts ...transport.RequestOption) (*GetUserResponse, error) {
	var rep GetUserResponse

	_, err := c.tp.Request("account.GetUser", req, &rep, opts...)
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

	_, err = s.tp.Subscribe("account.CreateUser", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req CreateUserRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.CreateUser(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("account.DeleteUser", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req DeleteUserRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.DeleteUser(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("account.GetUser", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req GetUserRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.GetUser(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan

	return nil
}
