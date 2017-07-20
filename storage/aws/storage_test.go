package aws

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

// Create a bucket, create and read an object, delete the bucket.
func TestStorage(t *testing.T) {
	profile := os.Getenv("AWS_TEST_PROFILE")
	region := os.Getenv("AWS_TEST_REGION")

	if profile == "" || region == "" {
		t.Skip("AWS_TEST_PROFILE and AWS_TEST_REGION required")
	}

	ctx := context.Background()

	c, err := NewStorage(Config{
		Region:      region,
		Credentials: credentials.NewSharedCredentials("", profile),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	bkt := c.Bucket("rdm-academy-test-3923uwe2")
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

	data.Reset()

	// Try presigned URL.
	url, err := obj.URL().Get(time.Second * 10)

	if err != nil {
		t.Errorf("object.url.get: %s", err)
	} else {
		resp, err := http.Get(url)
		if err != nil {
			t.Errorf("get url: %s", err)
		} else {
			if _, err := io.Copy(data, resp.Body); err != nil {
				t.Errorf("http body read: %s", err)
			}

			// Compare the original text and read text.
			out := data.String()
			if out != text {
				t.Errorf("expected %s, got %s", text, out)
			}
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
