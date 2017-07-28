package nodes

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/golang/protobuf/proto"
	"github.com/rdm-academy/api/commitlog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const eventSubject = "events.>"

type logEvent struct {
	Project string
	Type    string
	Author  string
	Data    interface{}
}

type nodeAddedEventData struct {
	Workflow string
	Node     struct {
		Key   string
		Type  string
		Title string
	}
}

type nodeRenamedEventData struct {
	Workflow string
	Node     struct {
		Key  string
		From string
		To   string
	}
}

func decodeType(s string) NodeType {
	c, ok := NodeType_value[strings.ToUpper(s)]
	if !ok {
		return NodeType_UNKNOWN
	}
	return NodeType(c)
}

// newEventHandler initializes a handler taking events from a stream
// and recording them into project-specific collections.
func newEventHandler(s Service) transport.Handler {
	return func(msg *transport.Message) (proto.Message, error) {
		ctx := context.Background()

		var e commitlog.Event
		if err := msg.Decode(&e); err != nil {
			return nil, err
		}

		switch e.Type {
		case "node.added":
			var d nodeAddedEventData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return nil, err
			}

			_, err := s.Create(ctx, &CreateRequest{
				Project: e.Project,
				Id:      d.Node.Key,
				Type:    decodeType(d.Node.Type),
				Title:   d.Node.Title,
			})

			return nil, err

		case "node.renamed":
			var d nodeRenamedEventData
			if err := json.Unmarshal(e.Data, &d); err != nil {
				return nil, err
			}

			_, err := s.SetTitle(ctx, &SetTitleRequest{
				Project: e.Project,
				Id:      d.Node.Key,
				Title:   d.Node.To,
			})

			return nil, err
		}

		return nil, nil
	}
}

type nodeIdent interface {
	GetId() string
	GetProject() string
}

type file struct {
	ID   string `bson:"id" json:"id"`
	Name string `bson:"name" json:"name"`
}

type node struct {
	ID       string    `bson:"_id"`
	Project  string    `bson:"project"`
	Type     NodeType  `bson:"type"`
	Title    string    `bson:"title"`
	Notes    string    `bson:"notes"`
	Files    []*file   `bson:"files"`
	Created  time.Time `bson:"created"`
	Modified time.Time `bson:"modified"`
}

type service struct {
	tp transport.Transport
	db *mgo.Database
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

	// Log the event.
	s.logEvent("events.project", &logEvent{
		Type:    "node.updated-notes",
		Project: req.Project,
		Author:  req.Account,
		Data: map[string]interface{}{
			"id":    req.Id,
			"notes": req.Notes,
		},
	})

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

	// Log the event.
	s.logEvent("events.project", &logEvent{
		Type:    "node.added-files",
		Project: req.Project,
		Author:  req.Account,
		Data: map[string]interface{}{
			"id":    req.Id,
			"files": files,
		},
	})

	return &NoReply{}, nil
}

func (s *service) RemoveFiles(ctx context.Context, req *RemoveFilesRequest) (*NoReply, error) {
	if !s.nodeHasIdent(req) {
		return nil, status.Error(codes.InvalidArgument, "project and id required")
	}

	if len(req.FileIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "no files specified")
	}

	// Get the file names that are going to be removed for the event.
	var n node
	p := bson.M{
		"files": 1,
	}
	if err := s.db.C(req.Project).FindId(req.Id).Select(p).One(&n); err != nil {
		return nil, err
	}

	// Index node files.
	fdx := make(map[string]*file, len(n.Files))
	for _, f := range n.Files {
		fdx[f.ID] = f
	}

	// Check that the files being removed exist.
	files := make([]*file, len(req.FileIds))
	for i, id := range req.FileIds {
		f, ok := fdx[id]
		if !ok {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("file `%s` does not exist", id))
		}

		files[i] = f
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

	// Log the event.
	s.logEvent("events.project", &logEvent{
		Type:    "node.removed-files",
		Project: req.Project,
		Author:  req.Account,
		Data: map[string]interface{}{
			"id":    req.Id,
			"files": files,
		},
	})

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
	s := &service{
		tp: tp,
		db: db,
	}
	// This will be auto-sunscribed when the transport is closed.
	_, err := tp.Subscribe(eventSubject, newEventHandler(s))
	if err != nil {
		return nil, err
	}

	return s, nil
}
