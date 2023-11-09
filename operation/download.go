package operation

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"
)

type clusterDownloader interface {
	downloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error)
	downloadRaw(key string, headers http.Header) (*http.Response, error)
	downloadFile(key, path string) (f *os.File, err error)
	downloadBytes(key string) (data []byte, err error)
	downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error)
	downloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error)
}

// Downloader 下载器
type Downloader struct {
	clusterDownloader
}

// NewDownloader 根据配置创建下载器
func NewDownloader(c *Config) *Downloader {
	return &Downloader{newSingleClusterDownloader(c)}
}

// NewDownloaderV2 根据环境变量创建下载器
func NewDownloaderV2() *Downloader {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	} else if singleClusterConfig, ok := c.(*Config); ok {
		return NewDownloader(singleClusterConfig)
	} else {
		var (
			concurrency = 1
			err         error
		)
		if concurrencyStr := os.Getenv("QINIU_MULTI_CLUSTERS_CONCURRENCY"); concurrencyStr != "" {
			if concurrency, err = strconv.Atoi(concurrencyStr); err != nil {
				elog.Warnf("Invalid QINIU_MULTI_CLUSTERS_CONCURRENCY: err=%s", err)
			}
		}
		return &Downloader{newMultiClusterDownloader(c, concurrency)}
	}
}

// DownloadCheck 检查文件
func (d *Downloader) DownloadCheck(key string) (l int64, err error) {
	l, _, err = d.DownloadRangeBytes(key, -1, 4)
	return
}

// DownloadCheckList 检查多个文件
func (d *Downloader) DownloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error) {
	return d.downloadCheckList(ctx, keys)
}

// DownloadRaw 使用给定的 HTTP Header 请求下载接口，并直接获得 http.Response 响应
func (d *Downloader) DownloadRaw(key string, headers http.Header) (*http.Response, error) {
	return d.downloadRaw(key, headers)
}

// DownloadFile 下载指定对象到文件里
func (d *Downloader) DownloadFile(key, path string) (f *os.File, err error) {
	return d.downloadFile(key, path)
}

// DownloadBytes 下载指定对象到内存中
func (d *Downloader) DownloadBytes(key string) (data []byte, err error) {
	return d.downloadBytes(key)
}

// DownloadRangeBytes 下载指定对象的指定范围到内存中
func (d *Downloader) DownloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	return d.downloadRangeBytes(key, offset, size)
}

// DownloadRangeReader 下载指定对象的指定范围为Reader
func (d *Downloader) DownloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	return d.downloadRangeReader(key, offset, size)
}
