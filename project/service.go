package project

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/rdm-academy/api/commitlog"
	uuid "github.com/satori/go.uuid"
)

const (
	projectsCol = "projects"
)

type logEvent struct {
	Project string
	Type    string
	Author  string
	Data    interface{}
}

type node struct {
	Type   string   `bson:"type"`
	Title  string   `bson:"title"`
	Input  []string `bson:"input"`
	Output []string `bson:"output"`
}

type workflow struct {
	ID       string           `bson:"id"`
	Source   string           `bson:"source"`
	Modified time.Time        `bson:"modified"`
	Nodes    map[string]*node `bson:"nodes"`
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
	tp transport.Transport
}

func (s *service) logEvent(t string, e *logEvent) {
	b, err := json.Marshal(e.Data)
	if err != nil {
		log.Printf("eventlog: data encoding error: %s", err)
	}

	_, err = s.tp.Publish(t, &commitlog.Event{
		Project: e.Project,
		Time:    time.Now().Unix(),
		Type:    e.Type,
		Author:  e.Author,
		Data:    b,
	})
	if err != nil {
		log.Printf("eventlog: publish error: %s", err)
	}
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

	// Log the event.
	s.logEvent("events.project", &logEvent{
		Type:    "project.created",
		Project: p.ID,
		Author:  p.Account,
		Data: map[string]interface{}{
			"name": p.Name,
		},
	})

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

	s.logEvent("events.project", &logEvent{
		Project: req.Id,
		Type:    "project.updated",
		Author:  req.Account,
		Data: map[string]interface{}{
			"name":        req.Name,
			"description": req.Description,
		},
	})

	return &UpdateProjectResponse{}, nil
}

func (s *service) UpdateWorkflow(ctx context.Context, req *UpdateWorkflowRequest) (*UpdateWorkflowResponse, error) {
	// TODO: parse and validate workflow source.
	g, err := ParseSource(req.Source)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	rep, err := s.GetProject(ctx, &GetProjectRequest{
		Id:      req.Id,
		Account: req.Account,
	})
	if err != nil {
		return nil, err
	}

	ag := &Graph{Nodes: rep.Project.Workflow.Nodes}
	gd := DiffGraph(ag, g)

	if gd == nil {
		return nil, status.Error(codes.FailedPrecondition, "nothing changed")
	}

	// Query of current project.
	q := bson.M{
		"_id":     req.Id,
		"account": req.Account,
	}

	wid := uuid.NewV4().String()

	nodes := make(map[string]*node, len(g.Nodes))
	for k, v := range g.Nodes {
		nodes[k] = &node{
			Type:   v.Type,
			Title:  v.Title,
			Output: v.Output,
			Input:  v.Input,
		}
	}

	// Append new workflow revision to inner array.
	u := bson.M{
		"$push": bson.M{
			"workflows": &workflow{
				ID:       wid,
				Source:   req.Source,
				Modified: time.Now(),
				Nodes:    nodes,
			},
		},
	}

	if err := s.db.C(projectsCol).Update(q, u); err != nil {
		return nil, err
	}

	for k, n := range gd.Added {
		s.logEvent("events.project", &logEvent{
			Project: req.Id,
			Type:    "node.added",
			Author:  req.Account,
			Data: map[string]interface{}{
				"workflow": wid,
				"node": map[string]interface{}{
					"key":    k,
					"type":   n.Type,
					"title":  n.Title,
					"input":  n.Input,
					"output": n.Output,
				},
			},
		})
	}

	for k, n := range gd.Removed {
		s.logEvent("events.project", &logEvent{
			Project: req.Id,
			Type:    "node.removed",
			Author:  req.Account,
			Data: map[string]interface{}{
				"workflow": wid,
				"node": map[string]interface{}{
					"key":    k,
					"type":   n.Type,
					"title":  n.Title,
					"input":  n.Input,
					"output": n.Output,
				},
			},
		})
	}

	for k, n := range gd.Changed {
		// Rename.
		if n.ToTitle != "" {
			s.logEvent("events.project", &logEvent{
				Project: req.Id,
				Type:    "node.renamed",
				Author:  req.Account,
				Data: map[string]interface{}{
					"workflow": wid,
					"node": map[string]interface{}{
						"key":  k,
						"from": n.FromTitle,
						"to":   n.ToTitle,
					},
				},
			})
		}

		// Added/removed inputs.
		for e, ok := range n.Input {
			var t string
			if ok {
				t = "node.input-added"
			} else {
				t = "node.input-removed"
			}

			s.logEvent("events.project", &logEvent{
				Project: req.Id,
				Type:    t,
				Author:  req.Account,
				Data: map[string]interface{}{
					"workflow": wid,
					"node": map[string]interface{}{
						"key":   k,
						"input": e,
					},
				},
			})
		}

		// Added/removed outputs.
		for e, ok := range n.Output {
			var t string
			if ok {
				t = "node.output-added"
			} else {
				t = "node.output-removed"
			}

			s.logEvent("events.project", &logEvent{
				Project: req.Id,
				Type:    t,
				Author:  req.Account,
				Data: map[string]interface{}{
					"workflow": wid,
					"node": map[string]interface{}{
						"key":    k,
						"output": e,
					},
				},
			})
		}
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

		nodes := p.Workflows[0].Nodes
		if nodes != nil {
			w.Nodes = make(map[string]*Node, len(nodes))
			for k, v := range nodes {
				w.Nodes[k] = &Node{
					Type:   v.Type,
					Title:  v.Title,
					Input:  v.Input,
					Output: v.Output,
				}
			}
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

			nodes := p.Workflows[0].Nodes
			if nodes != nil {
				w.Nodes = make(map[string]*Node, len(nodes))
				for k, v := range nodes {
					w.Nodes[k] = &Node{
						Type:   v.Type,
						Title:  v.Title,
						Input:  v.Input,
						Output: v.Output,
					}
				}
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

	s.logEvent("events.project", &logEvent{
		Project: req.Id,
		Type:    "project.deleted",
		Author:  req.Account,
	})

	return &DeleteProjectResponse{}, nil
}

func NewService(tp transport.Transport, db *mgo.Database) (Service, error) {
	err := db.C(projectsCol).EnsureIndex(mgo.Index{
		Key:    []string{"account", "_name"},
		Unique: true,
	})
	if err != nil {
		return nil, err
	}

	return &service{
		db: db,
		tp: tp,
	}, nil
}
