package commitlog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/chop-dbhi/nats-rpc/transport"
	"github.com/golang/protobuf/proto"
)

const eventSubject = "events.>"

type dbEvent struct {
	ID     bson.ObjectId          `bson:"_id"`
	Time   int64                  `bson:"time"`
	Type   string                 `bson:"type"`
	Author string                 `bson:"author"`
	Data   map[string]interface{} `bson:"data"`
}

type dbCommit struct {
	ID     bson.ObjectId `bson:"_id"`
	Msg    string        `bson:"msg"`
	Author string        `bson:"author"`
	Time   int64         `bson:"time"`
	Parent string        `bson:"parent"`
	Event  bson.ObjectId `bson:"event"`
}

// newEventHandler initializes a handler taking events from a stream
// and recording them into project-specific collections.
func newEventHandler(db *mgo.Database) transport.Handler {
	return func(msg *transport.Message) (proto.Message, error) {
		var event Event
		if err := msg.Decode(&event); err != nil {
			return nil, err
		}

		var data map[string]interface{}
		if event.Data != nil {
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return nil, err
			}
		}

		e := &dbEvent{
			ID:     bson.NewObjectId(),
			Time:   event.Time,
			Type:   event.Type,
			Author: event.Author,
			Data:   data,
		}

		eventCol := fmt.Sprintf("%s_events", event.Project)
		if err := db.C(eventCol).Insert(e); err != nil {
			return nil, err
		}

		return nil, nil
	}
}

type service struct {
	db *mgo.Database
	tp transport.Transport
}

func (s *service) Commit(ctx context.Context, req *CommitRequest) (*CommitReply, error) {
	if req.Project == "" {
		return nil, status.Error(codes.InvalidArgument, "project required")
	}

	msg := strings.TrimSpace(req.Msg)
	if msg == "" {
		return nil, status.Error(codes.InvalidArgument, "message required")
	}

	eventCol := fmt.Sprintf("%s_events", req.Project)
	commitCol := fmt.Sprintf("%s_commit", req.Project)

	// Get latest commit in order fetch the events later than
	// the one committed.
	var latestCommit dbCommit
	err := s.db.C(commitCol).
		Find(nil).
		Sort("-_id").
		Select(bson.M{"event": 1}).
		Limit(1).
		One(&latestCommit)

	var q bson.M

	if err == mgo.ErrNotFound {
		q = nil
	} else if err != nil {
		return nil, err
	} else {
		q = bson.M{
			"_id": bson.M{
				"$gt": latestCommit.Event,
			},
		}
	}

	// Get latest event.
	var latestEvent dbEvent
	err = s.db.C(eventCol).
		Find(q).
		Sort("-_id").
		Limit(1).
		One(&latestEvent)

	if err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.FailedPrecondition, "nothing to commit")
		}
		return nil, fmt.Errorf("failed to find latest event: %s", err)
	}

	var parent string
	if latestCommit.ID != "" {
		parent = latestCommit.ID.Hex()
	}

	// Insert the commit.
	// TODO: there is a race condition here.
	c := dbCommit{
		ID:     bson.NewObjectId(),
		Time:   time.Now().Unix(),
		Author: req.Author,
		Msg:    msg,
		Event:  latestEvent.ID,
		Parent: parent,
	}

	err = s.db.C(commitCol).
		Insert(c)

	if err != nil {
		return nil, fmt.Errorf("commit insert failed: %s", err)
	}

	return &CommitReply{
		Id: c.ID.Hex(),
	}, nil
}

func (s *service) History(ctx context.Context, req *HistoryRequest) (*HistoryReply, error) {
	if req.Project == "" {
		return nil, status.Error(codes.InvalidArgument, "project required")
	}

	eventCol := fmt.Sprintf("%s_events", req.Project)
	commitCol := fmt.Sprintf("%s_commit", req.Project)

	var commit dbCommit

	// No commit specified, find latest one.
	if req.Commit == "" {
		err := s.db.C(commitCol).
			Find(nil).
			Sort("-_id").
			Limit(1).
			One(&commit)

			// No commits.
		if err == mgo.ErrNotFound {
			return &HistoryReply{}, nil
		}

		if err != nil {
			return nil, err
		}
	} else {
		// Confirm the commit exists.
		err := s.db.C(commitCol).
			FindId(bson.ObjectIdHex(req.Commit)).
			Limit(1).
			One(&commit)

		if err != nil {
			if err == mgo.ErrNotFound {
				return nil, status.Error(codes.NotFound, "commit not found")
			}

			return nil, err
		}
	}

	// Get the next commit for the event boundary.
	var nextCommit dbCommit

	q := bson.M{
		"_id": bson.M{
			"$lt": commit.ID,
		},
	}

	err := s.db.C(commitCol).
		Find(q).
		Select(bson.M{"event": 1}).
		Sort("-_id").
		Limit(1).
		One(&nextCommit)

	// Hit the edge. Get the remaining events.
	if err == mgo.ErrNotFound {
		q = bson.M{
			"_id": bson.M{
				"$lte": commit.Event,
			},
		}
	} else if err != nil {
		return nil, err
	} else {
		q = bson.M{
			"_id": bson.M{
				"$lte": commit.Event,
				"$gt":  nextCommit.Event,
			},
		}
	}

	var events []*dbEvent
	err = s.db.C(eventCol).
		Find(q).
		Sort("-_id").
		All(&events)

	if err != nil {
		return nil, err
	}

	cm := Commit{
		Id:     commit.ID.Hex(),
		Msg:    commit.Msg,
		Author: commit.Author,
		Time:   commit.Time,
	}

	cm.Events = make([]*Event, len(events))

	for i, e := range events {
		var b []byte
		if e.Data != nil {
			b, _ = json.Marshal(e.Data)
		}

		cm.Events[i] = &Event{
			Id:     e.ID.Hex(),
			Time:   e.Time,
			Type:   e.Type,
			Author: e.Author,
			Data:   b,
		}
	}

	var nextId string
	if nextCommit.ID != "" {
		nextId = nextCommit.ID.Hex()
	}

	return &HistoryReply{
		Commit: &cm,
		Next:   nextId,
	}, nil
}

func (s *service) Pending(ctx context.Context, req *PendingRequest) (*PendingReply, error) {
	if req.Project == "" {
		return nil, status.Error(codes.InvalidArgument, "project required")
	}

	eventCol := fmt.Sprintf("%s_events", req.Project)
	commitCol := fmt.Sprintf("%s_commit", req.Project)

	// Get the latest commit.
	var lastCommit dbCommit
	err := s.db.C(commitCol).
		Find(nil).
		Sort("-_id").
		Limit(1).
		One(&lastCommit)

	// Not found means there are no commits yet.
	if err != nil && err != mgo.ErrNotFound {
		return nil, err
	}

	// Get all events later than the head event on the latest commit
	// if one exist.
	var q bson.M
	if lastCommit.ID != "" {
		q = bson.M{
			"_id": bson.M{
				"$gt": lastCommit.Event,
			},
		}
	}

	var dbEvents []*dbEvent
	err = s.db.C(eventCol).
		Find(q).
		Sort("-_id").
		All(&dbEvents)

	if err != nil {
		return nil, err
	}

	events := make([]*Event, len(dbEvents))

	for i, e := range dbEvents {
		var b []byte
		if e.Data != nil {
			b, _ = json.Marshal(e.Data)
		}

		events[i] = &Event{
			Id:     e.ID.Hex(),
			Time:   e.Time,
			Type:   e.Type,
			Author: e.Author,
			Data:   b,
		}
	}

	return &PendingReply{
		Events: events,
	}, nil
}

func NewService(tp transport.Transport, db *mgo.Database) (Service, error) {
	// This will be auto-sunscribed when the transport is closed.
	_, err := tp.Subscribe(eventSubject, newEventHandler(db))
	if err != nil {
		return nil, err
	}

	return &service{
		db: db,
		tp: tp,
	}, nil
}
