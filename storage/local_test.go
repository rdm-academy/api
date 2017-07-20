package storage

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// Create a bucket, create and read an object, delete the bucket.
func TestLocal(t *testing.T) {
	baseDir := filepath.Join(os.TempDir(), "test")

	if err := os.Mkdir(baseDir, 0755); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(baseDir); err != nil {
			t.Logf("error removing base dir: %s", err)
		}
	}()

	ctx := context.Background()

	c, err := New(ctx, Config{
		Base: baseDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	bkt := c.Bucket("test-9b3dsd8")
	obj := bkt.Object("dir/test.csv")

	if err := bkt.Create(ctx); err != nil {
		t.Fatalf("bucket.create: %s", err)
	}

	text := "hello world!\n"
	data := bytes.NewBufferString(text)

	wc, err := obj.Writer(ctx)
	if err != nil {
		t.Errorf("object.writer: %s", err)
	} else {
		// Write the data.
		if _, err := io.Copy(wc, data); err != nil {
			t.Errorf("objectwriter.write: %s", err)
		}

		// Close the writer to store it.
		if err := wc.Close(); err != nil {
			t.Errorf("objectwriter.close: %s", err)
		}
	}

	// Move the object to a new path.
	obj, err = obj.Move(ctx, "foo.csv")
	if err != nil {
		t.Errorf("object.move: %s", err)
	}

	// Reset to read the data back.
	data.Reset()

	rc, err := obj.Reader(ctx)
	if err != nil {
		t.Errorf("object.reader: %s", err)
	} else {
		if _, err := io.Copy(data, rc); err != nil {
			t.Errorf("objectreader.read: %s", err)
		}

		// Compare the original text and read text.
		out := data.String()
		if out != text {
			t.Errorf("expected %s, got %s", text, out)
		}
	}

	// Delete the object.
	if err := obj.Delete(ctx); err != nil {
		t.Errorf("object.delete: %s", err)
	}

	// Delete the bucket.
	if err := bkt.Delete(ctx); err != nil {
		t.Errorf("bucket.delete: %s", err)
	}
}
