package operation

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/auth/qbox"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/kodo"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/kodocli"
)

type singleClusterUploader struct {
	bucket        string
	upHosts       []string
	credentials   *qbox.Mac
	partSize      int64
	upConcurrency int
	queryer       IQueryer
	httpTransport http.RoundTripper
	upTimeout     time.Duration
}

func newSingleClusterUploader(c *Config) *singleClusterUploader {
	mac := qbox.NewMac(c.Ak, c.Sk)
	part := c.PartSize * 1024 * 1024
	if part < 4*1024*1024 {
		part = 4 * 1024 * 1024
	}
	var queryer IQueryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	return &singleClusterUploader{
		bucket:        c.Bucket,
		upHosts:       dupStrings(c.UpHosts),
		credentials:   mac,
		partSize:      part,
		upConcurrency: c.UpConcurrency,
		queryer:       queryer,
		httpTransport: getHttpClientTransport(c),
		upTimeout:     buildDurationByMs(c.UpTimeoutMs, DefaultConfigUpTimeoutMs),
	}
}

func (p *singleClusterUploader) getUploader() kodocli.Uploader {
	upHosts := p.upHosts
	if p.queryer != nil {
		if hosts := p.queryer.QueryUpHosts(false); len(hosts) > 0 {
			upHosts = hosts
		}
	}

	var uploader = kodocli.NewUploader(1, &kodocli.UploadConfig{
		UpHosts:        upHosts,
		UploadPartSize: p.partSize,
		Concurrency:    p.upConcurrency,
		Transport:      p.httpTransport,
		UpTimeout:      p.upTimeout,
	})
	return uploader
}

func (p *singleClusterUploader) makeUptoken(policy *kodo.PutPolicy, expires int) string {
	var rr = *policy
	if expires <= 0 {
		expires = 3600
	}
	rr.Deadline = uint32(time.Now().Unix()) + uint32(expires)
	b, _ := json.Marshal(&rr)
	return p.credentials.SignWithData(b)
}

func (p *singleClusterUploader) uploadData(data []byte, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Infof("up time: key=%s, elapsed=%v", key, time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{Scope: p.bucket + ":" + key}
	upToken := p.makeUptoken(&policy, 3600*24)
	uploader := p.getUploader()
	for i := 0; i < 3; i++ {
		err = uploader.Put2(context.Background(), nil, upToken, key, bytes.NewReader(data), int64(len(data)), nil)
		if err == nil {
			break
		}
		elog.Infof("small upload retry: retried=%d, err=%s", i, err)
	}
	return
}

func (p *singleClusterUploader) uploadDataReader(data io.ReaderAt, size int, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Infof("up time: key=%s, elapsed=%v", key, time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope: p.bucket + ":" + key,
	}

	upToken := p.makeUptoken(&policy, 3600*24)

	uploader := p.getUploader()

	for i := 0; i < 3; i++ {
		err = uploader.Put2(context.Background(), nil, upToken, key, newReaderAtNopCloser(data), int64(size), nil)
		if err == nil {
			break
		}
		elog.Infof("small upload retry: retried=%d, err=%s", i, err)
	}
	return
}

func (p *singleClusterUploader) upload(file string, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Infof("up time: key=%s, elapsed=%v", key, time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope: p.bucket + ":" + key,
	}
	upToken := p.makeUptoken(&policy, 3600*24)

	f, err := os.Open(file)
	if err != nil {
		elog.Info("open file failed: file=%s, err=%s", file, err)
		return err
	}
	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		elog.Info("get file stat failed: err=%s", err)
		return err
	}

	uploader := p.getUploader()

	if fInfo.Size() <= p.partSize {
		for i := 0; i < 3; i++ {
			err = uploader.Put2(context.Background(), nil, upToken, key, newReaderAtNopCloser(f), fInfo.Size(), nil)
			if err == nil {
				break
			}
			elog.Infof("small upload retry: retried=%d, err=%s", i, err)
		}
		return
	}

	for i := 0; i < 3; i++ {
		err = uploader.Upload(context.Background(), nil, upToken, key, newReaderAtNopCloser(f), fInfo.Size(), nil,
			func(partIdx int, etag string) {
				elog.Infof("callback: partNum=%d, etag=%s", partIdx, etag)
			})
		if err == nil {
			break
		}
		elog.Infof("part upload retry: retried=%d, err=%s", i, err)
	}
	return
}

func (p *singleClusterUploader) uploadReader(reader io.Reader, key string) (err error) {
	t := time.Now()
	defer func() {
		elog.Infof("up time: key=%s, elapsed=%v", key, time.Since(t))
	}()
	key = strings.TrimPrefix(key, "/")
	policy := kodo.PutPolicy{
		Scope: p.bucket + ":" + key,
	}
	upToken := p.makeUptoken(&policy, 3600*24)

	uploader := p.getUploader()

	bufReader := bufio.NewReader(reader)
	firstPart, err := io.ReadAll(io.LimitReader(bufReader, p.partSize))
	if err != nil {
		return
	}

	smallUpload := false
	if len(firstPart) < int(p.partSize) {
		smallUpload = true
	} else if _, err = bufReader.Peek(1); err != nil {
		if err == io.EOF {
			smallUpload = true
		} else {
			return err
		}
	}

	if smallUpload {
		for i := 0; i < 3; i++ {
			err = uploader.Put2(context.Background(), nil, upToken, key, bytes.NewReader(firstPart), int64(len(firstPart)), nil)
			if err == nil {
				break
			}
			elog.Infof("small upload retry: retried=%d, err=%s", i, err)
		}
		return
	}

	err = uploader.StreamUpload(context.Background(), nil, upToken, key, io.MultiReader(bytes.NewReader(firstPart), bufReader),
		func(partIdx int, etag string) {
			elog.Infof("callback: partNum=%d, etag=%s", partIdx, etag)
		})
	return err
}

type readerAtCloser interface {
	io.ReaderAt
	io.Closer
}

type readerAtNopCloser struct {
	io.ReaderAt
}

func (readerAtNopCloser) Close() error { return nil }

// newReaderAtNopCloser returns a readerAtCloser with a no-op Close method wrapping
// the provided ReaderAt r.
func newReaderAtNopCloser(r io.ReaderAt) readerAtCloser {
	return readerAtNopCloser{r}
}
