package gcp

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"time"

	gcs "cloud.google.com/go/storage"
	"github.com/rdm-academy/api/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	Context            context.Context
	Project            string
	ServiceAccountFile string

	sa *serviceAccount
}

type serviceAccount struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
}

func readServiceAccountFile(file string) (*serviceAccount, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var sa serviceAccount
	if err := json.NewDecoder(f).Decode(&sa); err != nil {
		return nil, err
	}

	return &sa, nil
}

type URL struct {
	object *Object
	cfg    *Config
}

func (u *URL) Get(expiry time.Duration) (string, error) {
	return gcs.SignedURL(u.object.bucket, u.object.name, &gcs.SignedURLOptions{
		GoogleAccessID: u.cfg.sa.ClientEmail,
		PrivateKey:     []byte(u.cfg.sa.PrivateKey),
		Method:         "GET",
		Expires:        time.Now().Add(expiry),
	})
}

func (u *URL) Put(expiry time.Duration) (string, error) {
	return gcs.SignedURL(u.object.bucket, u.object.name, &gcs.SignedURLOptions{
		GoogleAccessID: u.cfg.sa.ClientEmail,
		PrivateKey:     []byte(u.cfg.sa.PrivateKey),
		Method:         "PUT",
		Expires:        time.Now().Add(expiry),
	})
}

type Object struct {
	name   string
	bucket string
	cfg    *Config
	bkt    *gcs.BucketHandle
	obj    *gcs.ObjectHandle
}

func (o *Object) Name() string {
	return o.name
}

func (o *Object) Bucket() string {
	return o.bucket
}

func (o *Object) Writer(ctx context.Context) (io.WriteCloser, error) {
	return o.obj.NewWriter(ctx), nil
}

func (o *Object) Reader(ctx context.Context) (io.ReadCloser, error) {
	return o.obj.NewReader(ctx)
}

func (o *Object) Exists(ctx context.Context) (bool, error) {
	iter := o.bkt.Objects(ctx, &gcs.Query{
		Prefix: o.name,
	})

	_, err := iter.Next()
	if err == nil {
		return true, nil
	}

	if err == iterator.Done {
		return false, nil
	}

	return false, err
}

func (o *Object) Delete(ctx context.Context) error {
	return o.obj.Delete(ctx)
}

func (o *Object) Move(ctx context.Context, name string) (storage.Object, error) {
	dest := o.bkt.Object(name)

	_, err := dest.CopierFrom(o.obj).Run(ctx)
	if err != nil {
		return nil, err
	}

	if err := o.Delete(ctx); err != nil {
		return nil, err
	}

	return &Object{
		name:   name,
		bucket: o.bucket,
		cfg:    o.cfg,
		bkt:    o.bkt,
		obj:    dest,
	}, nil
}

func (o *Object) URL() storage.URL {
	return &URL{
		object: o,
		cfg:    o.cfg,
	}
}

type Bucket struct {
	name  string
	cfg   *Config
	bkt   *gcs.BucketHandle
	attrs *gcs.BucketAttrs
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Create(ctx context.Context) error {
	return b.bkt.Create(ctx, b.cfg.Project, b.attrs)
}

func (b *Bucket) Delete(ctx context.Context) error {
	return b.bkt.Delete(ctx)
}

func (b *Bucket) Object(name string) storage.Object {
	return &Object{
		name:   name,
		bucket: b.name,
		cfg:    b.cfg,
		bkt:    b.bkt,
		obj:    b.bkt.Object(name),
	}
}

type Storage struct {
	cfg    *Config
	client *gcs.Client
}

func (s *Storage) Name() string {
	return "gcp-gcs"
}

func (s *Storage) Close() error {
	return s.client.Close()
}

func (s *Storage) Bucket(name string) storage.Bucket {
	bkt := s.client.Bucket(name)

	return &Bucket{
		name: name,
		cfg:  s.cfg,
		bkt:  bkt,
		attrs: &gcs.BucketAttrs{
			Name: name,
			// Location: "US",
			StorageClass: "STANDARD",
			// TODO: ACL
		},
	}
}

func NewStorage(cfg Config) (*Storage, error) {
	if cfg.Project == "" {
		return nil, errors.New("project is required")
	}

	if cfg.Context == nil {
		cfg.Context = context.Background()
	}

	client, err := gcs.NewClient(
		cfg.Context,
		option.WithServiceAccountFile(cfg.ServiceAccountFile),
	)
	if err != nil {
		return nil, err
	}

	// Extract out details for URL signing.
	sa, err := readServiceAccountFile(cfg.ServiceAccountFile)
	if err != nil {
		return nil, err
	}

	cfg.sa = sa

	return &Storage{
		cfg:    &cfg,
		client: client,
	}, nil
}
