package storage

import (
	"context"
	"io"
	"time"
)

// URL provides methods for performing various operations on an object over HTTP.
type URL interface {
	// Get returns a "signed" URL for fetching underlying object. The duration
	// determines how long the URL is valid for.
	Get(time.Duration) (string, error)

	// Put returns a "signed" URL for uploading the underlying object. The passed
	// duration determines how long the URL is valid for.
	Put(time.Duration) (string, error)
}

// Object is an interface to accessing an object from a storage system.
type Object interface {
	// Name returns the name of the object.
	Name() string

	// Bucket returns the name of the bucket this object is in.
	Bucket() string

	// Writer returns an io.WriteCloser for writing the object contents.
	// It replaces the contents if the object alread exists.
	Writer(ctx context.Context) (io.WriteCloser, error)

	// Reader returns an io.ReadCloser to reading the object contents.
	Reader(ctx context.Context) (io.ReadCloser, error)

	// Exists returns true if the object exists in storage.
	Exists(ctx context.Context) (bool, error)

	// Delete deletes the object.
	Delete(ctx context.Context) error

	// Move renames the object.
	Move(ctx context.Context, name string) (Object, error)

	// URL returns the object's URL interface.
	URL() URL
}

// Bucket is an interface for representing a container of objects and other buckets.
type Bucket interface {
	// Name returns the name of the bucket.
	Name() string

	// Create creates the bucket.
	Create(ctx context.Context) error

	// Delete deletes the bucket.
	Delete(ctx context.Context) error

	// Object returns a reference to an object within the bucket.
	Object(name string) Object
}

// Storage is an interface that represents an object storage system.
type Storage interface {
	// Name of the storage provider.
	Name() string

	// Bucket returns a references to a bucket.
	Bucket(name string) Bucket

	// Close closes the storage system.
	Close() error
}
