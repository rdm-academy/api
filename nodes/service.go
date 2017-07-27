package nodes

import (
	"context"
	"strings"
	"time"

	"github.com/chop-dbhi/nats-rpc/transport"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type nodeIdent interface {
	GetId() string
	GetProject() string
}

type file struct {
	ID   string `bson:"id"`
	Name string `bson:"name"`
}

type node struct {
	ID       string    `bson:"_id"`
	Project  string    `bson:"project"`
	Type     NodeType  `bson:"type"`
	Title    string    `bson:"title"`
	Notes    string    `bson:"notes"`
	Files    []file    `bson:"files"`
	Created  time.Time `bson:"created"`
	Modified time.Time `bson:"modified"`
}

type service struct {
	tp transport.Transport
	db *mgo.Database
}

// nodeHasIdent checks that the node has non-empty identifiers.
func (s *service) nodeHasIdent(v nodeIdent) bool {
	return strings.TrimSpace(v.GetId()) != "" && strings.TrimSpace(v.GetProject()) != ""
}

// nodeExists checks whether the node exists.
func (s *service) nodeExists(v nodeIdent) (bool, error) {
	n, err := s.db.C(v.GetProject()).FindId(v.GetId()).Count()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

func (s *service) Create(ctx context.Context, req *CreateRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	if req.Type == NodeType_UNKNOWN {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}

	n := node{
		ID:      req.Id,
		Project: req.Project,
		Type:    req.Type,
		Title:   req.Title,
		Created: time.Now(),
	}

	if err := s.db.C(req.Project).Insert(&n); err != nil {
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "node already exists")
		}

		return nil, err
	}

	return &NoReply{}, nil
}

func (s *service) SetTitle(ctx context.Context, req *SetTitleRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	u := bson.M{
		"$set": bson.M{
			"title": req.Title,
		},
		"$currentDate": bson.M{
			"modified": true,
		},
	}

	if err := s.db.C(req.Project).UpdateId(req.Id, u); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "node does not exist")
		}
		return nil, err
	}

	return &NoReply{}, nil
}

func (s *service) SetNotes(ctx context.Context, req *SetNotesRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	u := bson.M{
		"$set": bson.M{
			"notes": req.Notes,
		},
		"$currentDate": bson.M{
			"modified": true,
		},
	}

	if err := s.db.C(req.Project).UpdateId(req.Id, u); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "node does not exist")
		}
		return nil, err
	}

	return &NoReply{}, nil
}

func (s *service) AddFiles(ctx context.Context, req *AddFilesRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	if len(req.Files) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no files specified")
	}

	files := make([]*file, len(req.Files))

	for i, f := range req.Files {
		files[i] = &file{
			ID:   f.Id,
			Name: f.Name,
		}
	}

	u := bson.M{
		"$push": bson.M{
			"files": bson.M{
				"$each": files,
			},
		},
		"$currentDate": bson.M{
			"modified": true,
		},
	}

	if err := s.db.C(req.Project).UpdateId(req.Id, u); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "node does not exist")
		}
		return nil, err
	}

	return &NoReply{}, nil
}

func (s *service) RemoveFiles(ctx context.Context, req *RemoveFilesRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	if len(req.FileIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no files specified")
	}

	u := bson.M{
		"$pull": bson.M{
			"files": bson.M{
				"id": bson.M{
					"$in": req.FileIds,
				},
			},
		},
		"$currentDate": bson.M{
			"modified": true,
		},
	}

	if err := s.db.C(req.Project).UpdateId(req.Id, u); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "node does not exist")
		}
		return nil, err
	}

	return &NoReply{}, nil
}

func (s *service) Get(ctx context.Context, req *GetRequest) (*GetReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	var n node
	if err := s.db.C(req.Project).FindId(req.Id).One(&n); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "node does not exist")
		}
		return nil, err
	}

	files := make([]*File, len(n.Files))
	for i, f := range n.Files {
		files[i] = &File{
			Id:   f.ID,
			Name: f.Name,
		}
	}

	r := GetReply{
		Id:    n.ID,
		Type:  n.Type,
		Title: n.Title,
		Notes: n.Notes,
		Files: files,
	}

	return &r, nil
}

func NewService(tp transport.Transport, db *mgo.Database) (Service, error) {
	return &service{
		tp: tp,
		db: db,
	}, nil
}
