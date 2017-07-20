package project

import (
	"context"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	uuid "github.com/satori/go.uuid"
)

const (
	projectsCol = "projects"
)

type workflow struct {
	Source   string    `bson:"source"`
	Modified time.Time `bson:"modified"`
}

type project struct {
	ID          string      `bson:"_id"`
	Account     string      `bson:"account"`
	Name        string      `bson:"name"`
	Description string      `bson:"description"`
	Created     time.Time   `bson:"created"`
	Modified    time.Time   `bson:"modified"`
	Workflows   []*workflow `bson:"workflows"`

	// Internal fields for lookups.
	NormName string `bson:"_name"`
}

type service struct {
	db *mgo.Database
}

func (s *service) CreateProject(ctx context.Context, req *CreateProjectRequest) (*CreateProjectResponse, error) {
	if req.Account == "" {
		return nil, status.Error(codes.InvalidArgument, "account required")
	}

	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}

	now := time.Now()

	p := project{
		ID:          uuid.NewV4().String(),
		Account:     req.Account,
		Name:        req.Name,
		Description: req.Description,
		Created:     now,
		Modified:    now,

		// Used for the index uniqueness check.
		NormName: strings.ToLower(req.Name),
	}

	if err := s.db.C(projectsCol).Insert(&p); err != nil {
		// Already exists by name for account.
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "project already exists")
		}

		// Unknown.
		return nil, err
	}

	rep := &Project{
		Id:          p.ID,
		Account:     p.Account,
		Name:        p.Name,
		Description: p.Description,
		Created:     p.Created.Unix(),
		Modified:    p.Modified.Unix(),
		Workflow:    &Workflow{},
	}

	return &CreateProjectResponse{
		Project: rep,
	}, nil
}

func (s *service) UpdateProject(ctx context.Context, req *UpdateProjectRequest) (*UpdateProjectResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "name required")
	}

	// Query of current project.
	q := bson.M{
		"_id":     req.Id,
		"account": req.Account,
	}

	// Update doc.
	u := bson.M{
		"$set": bson.M{
			"name":        req.Name,
			"_name":       strings.ToLower(req.Name),
			"description": req.Description,
		},
		"$currentDate": bson.M{
			"modified": true,
		},
	}

	if err := s.db.C(projectsCol).Update(q, u); err != nil {
		// Already exists by name for account.
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "project already exists")
		}

		// Unknown.
		return nil, err
	}

	return &UpdateProjectResponse{}, nil
}

func (s *service) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowRequest) (*UpdateWorkflowResponse, error) {

	// Query of current project.
	q := bson.M{
		"_id":     req.Id,
		"account": req.Account,
	}

	// Append new workflow revision to inner array.
	u := bson.M{
		"$push": bson.M{
			"workflows": &workflow{
				Source:   req.Source,
				Modified: time.Now(),
			},
		},
	}

	if err := s.db.C(projectsCol).Update(q, u); err != nil {
		return nil, err
	}

	return &UpdateWorkflowResponse{}, nil
}

func (s *service) GetProject(ctx context.Context, req *GetProjectRequest) (*GetProjectResponse, error) {
	q := bson.M{
		"_id":     req.Id,
		"account": req.Account,
	}

	// Only select the last revision of the workflow.
	x := bson.M{
		"workflows": bson.M{
			"$slice": -1,
		},
	}

	var p project
	if err := s.db.C(projectsCol).Find(q).Select(x).One(&p); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "project not found")
		}

		return nil, err
	}

	// Convert to response workflow.
	var w *Workflow
	if len(p.Workflows) == 1 {
		w = &Workflow{
			Source:   p.Workflows[0].Source,
			Modified: p.Workflows[0].Modified.Unix(),
		}
	} else {
		w = &Workflow{}
	}

	return &GetProjectResponse{
		Project: &Project{
			Id:          p.ID,
			Account:     p.Account,
			Name:        p.Name,
			Description: p.Description,
			Created:     p.Created.Unix(),
			Modified:    p.Modified.Unix(),
			Workflow:    w,
		},
	}, nil
}

func (s *service) ListProjects(ctx context.Context, req *ListProjectsRequest) (*ListProjectsResponse, error) {
	q := bson.M{
		"account": req.Account,
	}

	x := bson.M{
		"workflows": bson.M{
			"$slice": -1,
		},
	}

	var docs []*project
	if err := s.db.C(projectsCol).Find(q).Select(x).All(&docs); err != nil {
		return nil, err
	}

	list := make([]*Project, len(docs))
	for i, p := range docs {

		// Convert to response workflow.
		var w *Workflow
		if len(p.Workflows) == 1 {
			w = &Workflow{
				Source:   p.Workflows[0].Source,
				Modified: p.Workflows[0].Modified.Unix(),
			}
		} else {
			w = &Workflow{}
		}

		list[i] = &Project{
			Id:          p.ID,
			Account:     p.Account,
			Name:        p.Name,
			Description: p.Description,
			Created:     p.Created.Unix(),
			Modified:    p.Modified.Unix(),
			Workflow:    w,
		}
	}

	return &ListProjectsResponse{
		Projects: list,
	}, nil
}

func (s *service) DeleteProject(ctx context.Context, req *DeleteProjectRequest) (*DeleteProjectResponse, error) {
	q := bson.M{
		"_id":     req.Id,
		"account": req.Account,
	}

	if err := s.db.C(projectsCol).Remove(q); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "project not found")
		}

		return nil, err
	}

	return &DeleteProjectResponse{}, nil
}

func NewService(db *mgo.Database) (Service, error) {
	err := db.C(projectsCol).EnsureIndex(mgo.Index{
		Key:    []string{"account", "_name"},
		Unique: true,
	})
	if err != nil {
		return nil, err
	}

	return &service{
		db: db,
	}, nil
}
