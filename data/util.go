package data

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"
)

// readSizer wraps a reader and counts the number of bytes read.
type readSizer struct {
	r    io.Reader
	size int64
}

func (s *readSizer) Size() int64 {
	return s.size
}

func (s *readSizer) Read(b []byte) (int, error) {
	n, err := s.r.Read(b)
	s.size += int64(n)
	return n, err
}

// Closure to simplify error handling.
func upload(ctx context.Context, url string, mediatype string, body io.Reader) (*fileMeta, error) {
	// Compute the hash of the body.
	hsh := sha256.New()

	rs := &readSizer{r: body}

	// Stream the body to both the hash function and
	// a second reader for the put request.
	tr := io.TeeReader(rs, hsh)

	req, err := http.NewRequest(http.MethodPut, url, tr)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %s", err)
	}

	req = req.WithContext(ctx)

	if mediatype != "" {
		req.Header.Set("content-type", mediatype)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request error: %s", err)
	}

	if resp.StatusCode >= 400 {
		b, _ := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("http error: %s\n%s", resp.Status, string(b))
	}

	hash := fmt.Sprintf("sha256:%x", hsh.Sum(nil))

	return &fileMeta{
		Mediatype: mediatype,
		Hash:      hash,
		Size:      rs.Size(),
	}, nil
}

func Upload(ctx context.Context, svc ServiceClient, mediatype string, body io.Reader) (string, error) {
	rep, err := svc.Upload(ctx, &UploadRequest{})
	if err != nil {
		return "", err
	}

	id := rep.Id
	url := rep.SignedUrl

	// Set to in-progress.
	_, err = svc.Update(ctx, &UpdateRequest{
		Id:         id,
		State:      State_INPROGRESS,
		ImportTime: time.Now().Unix(),
	})
	if err != nil {
		return "", fmt.Errorf("failed to set to in progress: %s", err)
	}

	// Perform the upload.
	meta, err := upload(ctx, url, mediatype, body)
	if err != nil {
		_, err2 := svc.Update(ctx, &UpdateRequest{
			Id:    id,
			State: State_ERROR,
			Error: err.Error(),
		})

		if err2 != nil {
			return "", fmt.Errorf("failed to set ERROR state: %s\n%s\noriginal error: %s", id, err2, err)
		}
		return "", err
	}

	// Update state.
	_, err = svc.Update(ctx, &UpdateRequest{
		Id:        id,
		State:     State_DONE,
		Mediatype: meta.Mediatype,
		PutTime:   time.Now().Unix(),
		Size:      meta.Size,
		Hash:      meta.Hash,
	})

	// Log so this can be manually set if need be.
	if err != nil {
		return "", fmt.Errorf("failed to set DONE state: %s\n%s", id, err)
	}

	return id, nil
}
