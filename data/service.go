package data

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/rdm-academy/api/storage"
	uuid "github.com/satori/go.uuid"
)

const (
	objectsCol = "objects"
)

var (
	urlExpiryTime = time.Hour
	importTimeout = 10 * time.Minute
)

type Config struct {
	DB      *mgo.Database
	Storage storage.Storage
	Bucket  string
}

type object struct {
	ID         string    `bson:"_id"`
	CreateTime time.Time `bson:"create_time"`

	State State  `bson:"state"`
	Error string `bson:"error"`

	Bucket  string `bson:"bucket"`
	Storage string `bson:"storage"`

	ImportURL  string    `bson:"import_url"`
	ImportTime time.Time `bson:"import_time"`

	PutTime time.Time `bson:"put_time"`

	Hash        string `bson:"hash"`
	Size        int64  `bson:"size"`
	Mediatype   string `bson:"mediatype"`
	Compression string `bson:"compression"`

	Version      int       `bson:"version"`
	ModifiedTime time.Time `bson:"modified_time"`
}

type fileMeta struct {
	Size      int64
	Mediatype string
	Hash      string
}

type service struct {
	db      *mgo.Database
	storage storage.Storage
	bucket  string
}

func (s *service) fetch(ctx context.Context, o *object) {
	// Closure to simplify error handling.
	upload := func(ctx context.Context) (*fileMeta, error) {
		req, err := http.NewRequest(http.MethodGet, o.ImportURL, nil)
		if err != nil {
			return nil, fmt.Errorf("invalid request: %s", err)
		}

		req = req.WithContext(ctx)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("request error: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := ioutil.ReadAll(resp.Body)
			return nil, fmt.Errorf("http error: %s\n%s", resp.Status, string(b))
		}

		// Get a writer for this object.
		w, err := s.storage.Bucket(o.Bucket).Object(o.ID).Writer(ctx)
		if err != nil {
			return nil, err
		}

		// Compute the hash of the object.
		hsh := sha256.New()

		// Stream the response body to both the hash function and
		// a second reader for the put request.
		tr := io.TeeReader(resp.Body, hsh)

		size, err := io.Copy(w, tr)
		if err != nil {
			w.Close()
			return nil, fmt.Errorf("copy error: %s", err)
		}

		if err := w.Close(); err != nil {
			return nil, err
		}

		mediatype := resp.Header.Get("content-type")
		hash := fmt.Sprintf("sha256:%x", hsh.Sum(nil))

		return &fileMeta{
			Size:      size,
			Hash:      hash,
			Mediatype: mediatype,
		}, nil
	}

	// Set to in-progress.
	_, err := s.Update(ctx, &UpdateRequest{
		Id:         o.ID,
		State:      State_INPROGRESS,
		ImportTime: time.Now().Unix(),
	})

	if err != nil {
		log.Printf("failed to set to in progress: %s", err)
		return
	}

	// Wrap to set a limit on how long to wait.
	tctx, cancel := context.WithTimeout(ctx, importTimeout)
	defer cancel()

	// Perform the import.
	meta, err := upload(tctx)
	if err != nil {
		_, err2 := s.Update(ctx, &UpdateRequest{
			Id:    o.ID,
			State: State_ERROR,
			Error: err.Error(),
		})

		if err2 != nil {
			log.Printf("failed to set ERROR state: %s\n%s\noriginal error: %s", o.ID, err2, err)
		}
		return
	}

	// Update state.
	_, err = s.Update(ctx, &UpdateRequest{
		Id:        o.ID,
		State:     State_DONE,
		Mediatype: meta.Mediatype,
		PutTime:   time.Now().Unix(),
		Size:      meta.Size,
		Hash:      meta.Hash,
	})

	// Log so this can be manually set if need be.
	if err != nil {
		log.Printf("failed to set DONE state: %s\n%s", o.ID, err)
	}
}

func (s *service) Import(ctx context.Context, req *ImportRequest) (*ImportReply, error) {
	if req.Url == "" {
		return nil, status.Error(codes.InvalidArgument, "url required")
	}

	id := uuid.NewV4().String()

	d := &object{
		ID:           id,
		State:        State_CREATED,
		CreateTime:   time.Now(),
		Bucket:       s.bucket,
		Storage:      s.storage.Name(),
		ImportURL:    req.Url,
		ModifiedTime: time.Now(),
	}

	if err := s.db.C(objectsCol).Insert(d); err != nil {
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "object already exists")
		}

		return nil, err
	}

	go s.fetch(context.TODO(), d)

	return &ImportReply{
		Id: id,
	}, nil
}

func (s *service) Upload(ctx context.Context, _ *UploadRequest) (*UploadReply, error) {
	id := uuid.NewV4().String()

	d := &object{
		ID:           id,
		State:        State_CREATED,
		CreateTime:   time.Now(),
		Bucket:       s.bucket,
		Storage:      s.storage.Name(),
		ModifiedTime: time.Now(),
	}

	if err := s.db.C(objectsCol).Insert(d); err != nil {
		if mgo.IsDup(err) {
			return nil, status.Error(codes.AlreadyExists, "object already exists")
		}

		return nil, err
	}

	// Generate pre-signed URL for upload.
	obj := s.storage.Bucket(s.bucket).Object(id)

	url, err := obj.URL().Put(urlExpiryTime)
	if err != nil {
		return nil, err
	}

	return &UploadReply{
		Id:        id,
		SignedUrl: url,
	}, nil
}

func (s *service) Describe(ctx context.Context, req *DescribeRequest) (*DescribeReply, error) {
	var d object

	if err := s.db.C(objectsCol).FindId(req.Id).One(&d); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "object not found")
		}

		return nil, err
	}

	return &DescribeReply{
		Id:           d.ID,
		CreateTime:   d.CreateTime.Unix(),
		State:        d.State,
		Error:        d.Error,
		ImportUrl:    d.ImportURL,
		ImportTime:   d.ImportTime.Unix(),
		PutTime:      d.PutTime.Unix(),
		Hash:         d.Hash,
		Size:         d.Size,
		Mediatype:    d.Mediatype,
		Compression:  d.Compression,
		ModifiedTime: d.ModifiedTime.Unix(),
	}, nil
}

func (s *service) Update(ctx context.Context, req *UpdateRequest) (*UpdateReply, error) {
	var d object
	if err := s.db.C(objectsCol).FindId(req.Id).One(&d); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "object not found")
		}

		return nil, err
	}

	if d.State == State_DONE {
		return nil, status.Error(codes.FailedPrecondition, "object is done")
	}

	if req.State == State_UNKNOWN {
		return nil, status.Error(codes.InvalidArgument, "state required")
	}

	set := bson.M{}

	switch req.State {
	case State_INPROGRESS:
		// Can be transitioned to from any state.
		set["state"] = req.State

		// Reset the error.
		set["error"] = ""

		// Set import time. This may be empty if this is an upload.
		set["import_time"] = time.Unix(req.ImportTime, 0)

	case State_ERROR:
		set["state"] = req.State
		set["error"] = req.Error

	case State_DONE:
		// Can be transitioned to from in-progress.
		if d.State != State_INPROGRESS {
			return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("object cannot transition to %s from %s", State_DONE, req.State))
		}

		set["state"] = req.State
		set["put_time"] = time.Unix(req.PutTime, 0)

		// These may be empty.
		set["hash"] = req.Hash
		set["size"] = req.Size

	default:
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("object cannot transition to state %s", req.State))
	}

	// Optimistic update predicated on the version.
	q := bson.M{
		"_id": req.Id,
		"version": bson.M{
			"$eq": d.Version,
		},
	}

	// Increment version and modified time.
	set["version"] = d.Version + 1
	set["modified_time"] = time.Now()

	u := bson.M{
		"$set": set,
	}

	if err := s.db.C(objectsCol).Update(q, u); err != nil {
		// Not found means the object was updated in the meantime.
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.Unavailable, "object update conflict")
		}

		return nil, err
	}

	return &UpdateReply{}, nil
}

func (s *service) Get(ctx context.Context, req *GetRequest) (*GetReply, error) {
	var d object

	if err := s.db.C(objectsCol).FindId(req.Id).One(&d); err != nil {
		if err == mgo.ErrNotFound {
			return nil, status.Error(codes.NotFound, "object not found")
		}

		return nil, err
	}

	obj := s.storage.Bucket(d.Bucket).Object(d.ID)

	url, err := obj.URL().Get(urlExpiryTime)
	if err != nil {
		return nil, err
	}

	return &GetReply{
		SignedUrl: url,
	}, nil
}

func NewService(cfg Config) (Service, error) {
	if cfg.Storage == nil {
		return nil, errors.New("storage required")
	}

	if cfg.Bucket == "" {
		return nil, errors.New("bucket required")
	}

	if cfg.DB == nil {
		return nil, errors.New("mongo database required")
	}

	return &service{
		db:      cfg.DB,
		storage: cfg.Storage,
		bucket:  cfg.Bucket,
	}, nil
}
