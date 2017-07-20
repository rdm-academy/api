package aws

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rdm-academy/api/storage"
)

type Config struct {
	Credentials          *credentials.Credentials
	Region               string
	ServerSideEncryption string
}

type URL struct {
	object *Object
	client *s3.S3
}

func (u *URL) Get(expiry time.Duration) (string, error) {
	req, _ := u.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(u.object.bucket),
		Key:    aws.String(u.object.name),
	})

	return req.Presign(expiry)
}

func (u *URL) Put(expiry time.Duration) (string, error) {
	req, _ := u.client.PutObjectRequest(&s3.PutObjectInput{
		Bucket:               aws.String(u.object.bucket),
		Key:                  aws.String(u.object.name),
		ServerSideEncryption: aws.String(u.object.cfg.ServerSideEncryption),
	})

	return req.Presign(expiry)
}

type Object struct {
	name   string
	bucket string
	cfg    *Config
	client *s3.S3
}

func (o *Object) Name() string {
	return o.name
}

func (o *Object) Bucket() string {
	return o.bucket
}

func (o *Object) Exists(ctx context.Context) (bool, error) {
	req := &s3.HeadObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.name),
	}

	_, err := o.client.HeadObjectWithContext(ctx, req)
	if err == nil {
		return true, nil
	}

	if aerr, ok := err.(awserr.Error); ok {
		if aerr.Code() == "NoSuchKey" {
			return false, nil
		}
	}

	return false, err
}

type objectWriter struct {
	ctx context.Context

	up  *s3manager.Uploader
	req *s3manager.UploadInput
	pr  *io.PipeReader
	pw  *io.PipeWriter

	opened bool
	err    error
	donec  chan struct{}
}

func (w *objectWriter) open() error {
	w.opened = true

	go func() {
		defer close(w.donec)

		_, err := w.up.UploadWithContext(w.ctx, w.req)
		if err != nil {
			w.err = err
			w.pr.CloseWithError(w.err)
			return
		}
	}()

	return nil
}

func (w *objectWriter) Write(buf []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	if !w.opened {
		if err := w.open(); err != nil {
			return 0, err
		}
	}

	return w.pw.Write(buf)
}

func (w *objectWriter) Close() error {
	if !w.opened {
		if err := w.open(); err != nil {
			return err
		}
	}

	if err := w.pw.Close(); err != nil {
		return err
	}

	<-w.donec
	return w.err
}

func (o *Object) Writer(ctx context.Context) (io.WriteCloser, error) {
	pr, pw := io.Pipe()

	req := &s3manager.UploadInput{
		Bucket:               aws.String(o.bucket),
		Key:                  aws.String(o.name),
		Body:                 pr,
		ServerSideEncryption: aws.String(o.cfg.ServerSideEncryption),
	}

	up := s3manager.NewUploaderWithClient(o.client)
	w := &objectWriter{
		ctx:   ctx,
		up:    up,
		pw:    pw,
		pr:    pr,
		req:   req,
		donec: make(chan struct{}),
	}

	return w, nil
}

func (o *Object) Reader(ctx context.Context) (io.ReadCloser, error) {
	req := &s3.GetObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.name),
	}

	rep, err := o.client.GetObjectWithContext(ctx, req)
	if err != nil {
		return nil, err
	}

	return rep.Body, nil
}

func (o *Object) Delete(ctx context.Context) error {
	req := &s3.DeleteObjectInput{
		Bucket: aws.String(o.bucket),
		Key:    aws.String(o.name),
	}

	_, err := o.client.DeleteObjectWithContext(ctx, req)
	return err
}

func (o *Object) Move(ctx context.Context, name string) (storage.Object, error) {
	req := &s3.CopyObjectInput{
		Bucket:     aws.String(o.bucket),
		Key:        aws.String(name),
		CopySource: aws.String(fmt.Sprintf("%s/%s", o.bucket, o.name)),
	}
	if _, err := o.client.CopyObjectWithContext(ctx, req); err != nil {
		return nil, err
	}

	if err := o.Delete(ctx); err != nil {
		return nil, err
	}

	return &Object{
		bucket: o.bucket,
		name:   name,
		cfg:    o.cfg,
		client: o.client,
	}, nil
}

func (o *Object) URL() storage.URL {
	return &URL{
		object: o,
		client: o.client,
	}
}

type Bucket struct {
	name   string
	cfg    *Config
	client *s3.S3
}

func (b *Bucket) Name() string {
	return b.name
}

func (b *Bucket) Create(ctx context.Context) error {
	req := &s3.CreateBucketInput{
		Bucket: aws.String(b.name),
	}

	_, err := b.client.CreateBucketWithContext(ctx, req)
	return err
}

func (b *Bucket) Delete(ctx context.Context) error {
	req := &s3.DeleteBucketInput{
		Bucket: aws.String(b.name),
	}

	_, err := b.client.DeleteBucket(req)
	return err
}

func (b *Bucket) Object(name string) storage.Object {
	return &Object{
		name:   name,
		bucket: b.name,
		cfg:    b.cfg,
		client: b.client,
	}
}

type Storage struct {
	cfg    *Config
	client *s3.S3
}

func (s *Storage) Name() string {
	return "aws-s3"
}

func (s *Storage) Close() error {
	// S3 client does not need to be closed.
	return nil
}

func (s *Storage) Bucket(name string) storage.Bucket {
	return &Bucket{
		name:   name,
		cfg:    s.cfg,
		client: s.client,
	}
}

func NewStorage(cfg Config) (*Storage, error) {
	if cfg.Credentials == nil {
		return nil, fmt.Errorf("credentials required")
	}

	if cfg.Region == "" {
		return nil, fmt.Errorf("region is required")
	}

	if _, err := cfg.Credentials.Get(); err != nil {
		return nil, fmt.Errorf("aws credentials error: %s", err)
	}

	awscfg := aws.NewConfig().
		WithRegion(cfg.Region).
		WithCredentials(cfg.Credentials)

	sess := session.New()

	return &Storage{
		cfg:    &cfg,
		client: s3.New(sess, awscfg),
	}, nil
}
