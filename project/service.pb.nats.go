package project

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
	CreateProject(context.Context, *CreateProjectRequest) (*CreateProjectResponse, error)
	GetProject(context.Context, *GetProjectRequest) (*GetProjectResponse, error)
	ListProjects(context.Context, *ListProjectsRequest) (*ListProjectsResponse, error)
	UpdateProject(context.Context, *UpdateProjectRequest) (*UpdateProjectResponse, error)
	UpdateWorkflow(context.Context, *UpdateWorkflowRequest) (*UpdateWorkflowResponse, error)
	DeleteProject(context.Context, *DeleteProjectRequest) (*DeleteProjectResponse, error)
}

type ServiceClient interface {
	CreateProject(context.Context, *CreateProjectRequest, ...transport.RequestOption) (*CreateProjectResponse, error)
	GetProject(context.Context, *GetProjectRequest, ...transport.RequestOption) (*GetProjectResponse, error)
	ListProjects(context.Context, *ListProjectsRequest, ...transport.RequestOption) (*ListProjectsResponse, error)
	UpdateProject(context.Context, *UpdateProjectRequest, ...transport.RequestOption) (*UpdateProjectResponse, error)
	UpdateWorkflow(context.Context, *UpdateWorkflowRequest, ...transport.RequestOption) (*UpdateWorkflowResponse, error)
	DeleteProject(context.Context, *DeleteProjectRequest, ...transport.RequestOption) (*DeleteProjectResponse, error)
}

// serviceClient an implementation of Service client.
type serviceClient struct {
	tp transport.Transport
}

func (c *serviceClient) CreateProject(ctx context.Context, req *CreateProjectRequest, opts ...transport.RequestOption) (*CreateProjectResponse, error) {
	var rep CreateProjectResponse

	_, err := c.tp.Request("project.CreateProject", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) GetProject(ctx context.Context, req *GetProjectRequest, opts ...transport.RequestOption) (*GetProjectResponse, error) {
	var rep GetProjectResponse

	_, err := c.tp.Request("project.GetProject", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) ListProjects(ctx context.Context, req *ListProjectsRequest, opts ...transport.RequestOption) (*ListProjectsResponse, error) {
	var rep ListProjectsResponse

	_, err := c.tp.Request("project.ListProjects", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) UpdateProject(ctx context.Context, req *UpdateProjectRequest, opts ...transport.RequestOption) (*UpdateProjectResponse, error) {
	var rep UpdateProjectResponse

	_, err := c.tp.Request("project.UpdateProject", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowRequest, opts ...transport.RequestOption) (*UpdateWorkflowResponse, error) {
	var rep UpdateWorkflowResponse

	_, err := c.tp.Request("project.UpdateWorkflow", req, &rep, opts...)
	if err != nil {
		return nil, err
	}

	return &rep, nil
}

func (c *serviceClient) DeleteProject(ctx context.Context, req *DeleteProjectRequest, opts ...transport.RequestOption) (*DeleteProjectResponse, error) {
	var rep DeleteProjectResponse

	_, err := c.tp.Request("project.DeleteProject", req, &rep, opts...)
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

	_, err = s.tp.Subscribe("project.CreateProject", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req CreateProjectRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.CreateProject(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("project.GetProject", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req GetProjectRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.GetProject(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("project.ListProjects", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req ListProjectsRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.ListProjects(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("project.UpdateProject", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req UpdateProjectRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.UpdateProject(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("project.UpdateWorkflow", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req UpdateWorkflowRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.UpdateWorkflow(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}
	_, err = s.tp.Subscribe("project.DeleteProject", func(msg *transport.Message) (proto.Message, error) {
		ctx := context.WithValue(ctx, traceIdKey, msg.Id)

		var req DeleteProjectRequest
		if err := msg.Decode(&req); err != nil {
			return nil, err
		}

		return s.svc.DeleteProject(ctx, &req)
	}, opts...)
	if err != nil {
		return err
	}

	sigchan := make(chan os.Signal)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	<-sigchan

	return nil
}
