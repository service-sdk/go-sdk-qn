package operation

import (
	"context"
	"errors"
	"fmt"
	"github.com/service-sdk/go-sdk-qn/api.v7/auth/qbox"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"github.com/service-sdk/go-sdk-qn/x/rpc.v7"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type singleClusterDownloader struct {
	bucket      string
	ioHosts     []string
	credentials *qbox.Mac
	queryer     *Queryer
}

func newSingleClusterDownloader(c *Config) *singleClusterDownloader {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	downloader := singleClusterDownloader{
		bucket:      c.Bucket,
		ioHosts:     dupStrings(c.IoHosts),
		credentials: mac,
		queryer:     queryer,
	}
	shuffleHosts(downloader.ioHosts)
	return &downloader
}

func (d *singleClusterDownloader) downloadRaw(key string, headers http.Header) (resp *http.Response, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		resp, _, err = d.downloadRawInner(key, headers, failedIoHosts)
		if err == nil {
			return
		}
	}
	return
}

func (d *singleClusterDownloader) downloadFile(key, path string) (f *os.File, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		f, err = d.downloadFileInner(key, path, failedIoHosts)
		if err == nil {
			return
		}
	}
	return
}

func (d *singleClusterDownloader) downloadBytes(key string) (data []byte, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		data, err = d.downloadBytesInner(key, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		l, data, err = d.downloadRangeBytesInner(key, offset, size, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	failedIoHosts := make(map[string]struct{})
	for i := 0; i < 3; i++ {
		l, reader, err = d.downloadRangeReaderInner(key, offset, size, failedIoHosts)
		if err == nil {
			break
		}
	}
	return
}

func (d *singleClusterDownloader) downloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error) {
	concurrency := runtime.NumCPU()
	var (
		length                  = len(keys)
		stats                   = make([]*FileStat, length)
		index             int32 = 0
		failedIoHosts           = make(map[string]struct{})
		failedIoHostsLock sync.RWMutex
		pool              = goroutine_pool.NewGoroutinePool(concurrency)
	)

	for i := 0; i < concurrency; i++ {
		pool.Go(func(ctx context.Context) error {
			for {
				pos := int(atomic.AddInt32(&index, 1) - 1)
				if pos >= length {
					return nil
				}
				for j := 0; j < 3; j++ {
					failedIoHostsLock.RLock()
					host := d.nextHost(failedIoHosts)
					failedIoHostsLock.RUnlock()
					k := keys[pos]
					stat, err := d.downloadCheckInner(k, host)
					stats[pos] = stat
					if err == nil {
						succeedHostName(host)
						break
					}
					if stat.code == PathError {
						elog.Info("path stat error ", k, err)
						break
					}

					if stat.code == HostError {
						elog.Info("stat retry ", k, j, host, err)
						failedIoHostsLock.RLock()
						failedIoHosts[host] = struct{}{}
						failedIoHostsLock.RUnlock()
						failHostName(host)
						continue
					}
				}
			}
		})
	}
	_ = pool.Wait(ctx)
	return stats, nil
}

var curIoHostIndex uint32 = 0

func (d *singleClusterDownloader) nextHost(failedHosts map[string]struct{}) string {
	ioHosts := d.ioHosts
	if d.queryer != nil {
		if hosts := d.queryer.QueryIoHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			ioHosts = hosts
		}
	}
	switch len(ioHosts) {
	case 0:
		panic("No Io hosts is configured")
	case 1:
		return ioHosts[0]
	default:
		var ioHost string
		for i := 0; i <= len(ioHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curIoHostIndex, 1) - 1)
			ioHost = ioHosts[index%len(ioHosts)]
			if _, isFailedBefore := failedHosts[ioHost]; !isFailedBefore && isHostNameValid(ioHost) {
				break
			}
		}
		return ioHost
	}
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

func (d *singleClusterDownloader) downloadRawInner(key string, headers http.Header, failedIoHosts map[string]struct{}) (*http.Response, string, error) {
	key = strings.TrimPrefix(key, "/")
	host := d.nextHost(failedIoHosts)
	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.GetAccessKey(), d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, host, err
	}
	for headerName, headerValue := range headers {
		req.Header[headerName] = headerValue
	}
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, host, err
	}
	return response, host, nil
}

func (d *singleClusterDownloader) downloadFileInner(key, path string, failedIoHosts map[string]struct{}) (*os.File, error) {
	var length int64 = 0
	var f *os.File
	var err error
	f, err = os.OpenFile(path, os.O_RDWR, 0644)
	if err == nil {
		length, err = f.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
	} else if !os.IsNotExist(err) {
		elog.Warn("open file error", err)
		return nil, err
	}

	headers := make(http.Header)
	headers.Set("Accept-Encoding", "")
	if length != 0 {
		r := fmt.Sprintf("bytes=%d-", length)
		headers.Set("Range", r)
		elog.Info("continue download")
	}

	response, host, err := d.downloadRawInner(key, headers, failedIoHosts)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		succeedHostName(host)
		return f, nil
	}
	if response.StatusCode != http.StatusOK && response.StatusCode != http.StatusPartialContent {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	ctLength := response.ContentLength
	if f == nil {
		f, err = os.OpenFile(path, os.O_CREATE, 0644)
		if err != nil {
			return nil, err
		}
	}

	n, err := io.Copy(f, response.Body)
	if err != nil {
		return nil, err
	}
	if ctLength != n {
		elog.Warn("download length not equal", ctLength, n)
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	return f, nil
}

func (d *singleClusterDownloader) downloadBytesInner(key string, failedIoHosts map[string]struct{}) ([]byte, error) {
	response, host, err := d.downloadRawInner(key, make(http.Header), failedIoHosts)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		return nil, errors.New(response.Status)
	}
	succeedHostName(host)
	return io.ReadAll(response.Body)
}

func generateRange(offset, size int64) string {
	if offset == -1 {
		return fmt.Sprintf("bytes=-%d", size)
	}
	return fmt.Sprintf("bytes=%d-%d", offset, offset+size)
}

const PathError = -1
const HostError = -2
const FileError = -3

func (d *singleClusterDownloader) downloadCheckInner(key, host string) (f *FileStat, err error) {
	key = strings.TrimPrefix(key, "/")

	url := fmt.Sprintf("%s/getfile/%s/%s/%s", host, d.credentials.GetAccessKey(), d.bucket, key)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: -1,
			code: PathError,
		}, err
	}

	req.Header.Set("Range", generateRange(-1, 4))
	req.Header.Set("User-Agent", rpc.UserAgent)
	response, err := downloadClient.Do(req)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: -1,
			code: HostError,
		}, err
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New(response.Status)
	}

	if response.StatusCode == http.StatusNotFound {
		return &FileStat{
			Name: key,
			Size: -1,
			code: FileError,
		}, os.ErrNotExist
	}

	if response.StatusCode != http.StatusPartialContent {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: 0,
			code: HostError,
		}, err
	}
	_, err = io.ReadAll(response.Body)
	if err != nil {
		return &FileStat{
			Name: key,
			Size: l,
			code: HostError,
		}, err
	}
	return &FileStat{
		Name: key,
		Size: l,
		code: 0,
	}, nil
}

func (d *singleClusterDownloader) downloadRangeBytesInner(key string, offset, size int64, failedIoHosts map[string]struct{}) (int64, []byte, error) {
	l, r, err := d.downloadRangeReaderInner(key, offset, size, failedIoHosts)
	if err != nil {
		return l, nil, err
	}
	b, err := io.ReadAll(r)
	r.Close()
	return l, b, err
}

type wrapper struct {
	s    io.ReadCloser
	host string
}

func (w *wrapper) Read(p []byte) (n int, err error) {
	n, err = w.s.Read(p)
	if err != nil && err != io.EOF {
		elog.Info("read interrupt", w.host, err)
	}
	return
}

func (w *wrapper) Close() error {
	return w.s.Close()
}

func (d *singleClusterDownloader) downloadRangeReaderInner(key string, offset, size int64, failedIoHosts map[string]struct{}) (int64, io.ReadCloser, error) {
	headers := make(http.Header)
	headers.Set("Range", generateRange(offset, size))
	response, host, err := d.downloadRawInner(key, headers, failedIoHosts)
	if err != nil {
		return -1, nil, err
	}

	if response.StatusCode != http.StatusPartialContent {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, errors.New(response.Status)
	}

	rangeResponse := response.Header.Get("Content-Range")
	if rangeResponse == "" {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, errors.New("no content range")
	}

	l, err := getTotalLength(rangeResponse)
	if err != nil {
		failedIoHosts[host] = struct{}{}
		failHostName(host)
		response.Body.Close()
		return -1, nil, err
	}
	w := wrapper{
		s:    response.Body,
		host: host,
	}
	return l, &w, err
}

// 获取响应头 content-range 中的 / 后面的数字, 即文件总长度
func getTotalLength(contentRange string) (int64, error) {
	cr := strings.Split(contentRange, "/")
	if len(cr) != 2 {
		return -1, errors.New("wrong range " + contentRange)
	}

	return strconv.ParseInt(cr[1], 10, 64)
}
