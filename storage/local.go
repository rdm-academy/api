package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	DirPerm  = 0755
	FilePerm = 0644
)

var ErrSignedURLUnsupported = errors.New("signed urls are not supported")

type Config struct {
	Base string
}

type url struct{}

func (u *url) Get(expiry time.Duration) (string, error) {
	return "", ErrSignedURLUnsupported
}

func (u *url) Put(expiry time.Duration) (string, error) {
	return "", ErrSignedURLUnsupported
}

type object struct {
	name   string
	bucket string
	cfg    *Config
}

func (o *object) Name() string {
	return o.name
}

func (o *object) Bucket() string {
	return o.bucket
}

func (o *object) Exists(ctx context.Context) (bool, error) {
	path := filepath.Join(o.cfg.Base, o.bucket, o.name)
	_, err := os.Stat(path)

	switch err {
	case nil:
		return true, nil

	case os.ErrNotExist:
		return false, nil
	}

	return false, err
}

func (o *object) Delete(ctx context.Context) error {
	path := filepath.Join(o.cfg.Base, o.bucket, o.name)
	return os.Remove(path)
}

func (o *object) Reader(ctx context.Context) (io.ReadCloser, error) {
	path := filepath.Join(o.cfg.Base, o.bucket, o.name)
	return os.Open(path)
}

func (o *object) Writer(ctx context.Context) (io.WriteCloser, error) {
	path := filepath.Join(o.cfg.Base, o.bucket, o.name)

	// Create enclosing directory.
	dirpath := filepath.Dir(path)
	if err := os.MkdirAll(dirpath, 0755); err != nil {
		return nil, err
	}

	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_EXCL, FilePerm)
}

func (o *object) Move(ctx context.Context, name string) (Object, error) {
	srcPath := filepath.Join(o.cfg.Base, o.bucket, o.name)
	destPath := filepath.Join(o.cfg.Base, o.bucket, name)

	if err := os.Rename(srcPath, destPath); err != nil {
		return nil, err
	}

	return &object{
		name:   name,
		bucket: o.bucket,
		cfg:    o.cfg,
	}, nil
}

func (o *object) URL() URL {
	return &url{}
}

type bucket struct {
	name string
	cfg  *Config
}

func (b *bucket) Name() string {
	return b.name
}

func (b *bucket) Object(name string) Object {
	return &object{
		name:   name,
		bucket: b.name,
		cfg:    b.cfg,
	}
}

func (b *bucket) Create(ctx context.Context) error {
	path := filepath.Join(b.cfg.Base, b.name)
	return os.MkdirAll(path, DirPerm)
}

func (b *bucket) Delete(ctx context.Context) error {
	path := filepath.Join(b.cfg.Base, b.name)
	return os.RemoveAll(path)
}

type Local struct {
	cfg *Config
}

func (s *Local) Name() string {
	return "local"
}

func (s *Local) Close() error {
	return nil
}

func (s *Local) Bucket(name string) Bucket {
	return &bucket{
		name: name,
		cfg:  s.cfg,
	}
}

func New(ctx context.Context, cfg Config) (*Local, error) {
	if cfg.Base == "" {
		return nil, errors.New("base path required")
	}

	return &Local{
		cfg: &cfg,
	}, nil
}
