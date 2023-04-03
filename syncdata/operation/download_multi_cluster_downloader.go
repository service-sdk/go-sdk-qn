package operation

import (
	"context"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"io"
	"net/http"
	"os"
)

type multiClusterDownloader struct {
	config                   Configurable
	multiClustersConcurrency int
}

func newMultiClusterDownloader(config Configurable, multiClustersConcurrency int) *multiClusterDownloader {
	return &multiClusterDownloader{config: config, multiClustersConcurrency: multiClustersConcurrency}
}

func (d *multiClusterDownloader) downloadCheckList(ctx context.Context, keys []string) ([]*FileStat, error) {

	type KeysWithIndex struct {
		IndexMap []int
		Keys     []string
	}

	clusterPathsMap := make(map[*Config]*KeysWithIndex)
	for i, key := range keys {
		config, exists := d.config.forKey(key)
		if !exists {
			return nil, ErrUndefinedConfig
		}
		if keysWithIndex := clusterPathsMap[config]; keysWithIndex != nil {
			keysWithIndex.IndexMap = append(keysWithIndex.IndexMap, i)
			keysWithIndex.Keys = append(keysWithIndex.Keys, key)
		} else {
			keysWithIndex = &KeysWithIndex{Keys: make([]string, 0, 1), IndexMap: make([]int, 0, 1)}
			keysWithIndex.IndexMap = append(keysWithIndex.IndexMap, i)
			keysWithIndex.Keys = append(keysWithIndex.Keys, key)
			clusterPathsMap[config] = keysWithIndex
		}
	}

	pool := goroutine_pool.NewGoroutinePool(d.multiClustersConcurrency)
	allStats := make([]*FileStat, len(keys))
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				ds := newSingleClusterDownloader(config)
				if stats, err := ds.downloadCheckList(ctx, keys); err != nil {
					return err
				} else {
					for i := range stats {
						allStats[indexMap[i]] = stats[i]
					}
					return nil
				}
			})
		}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
	}
	err := pool.Wait(ctx)
	return allStats, err
}

func (d *multiClusterDownloader) downloadRaw(key string, headers http.Header) (*http.Response, error) {
	if config, exists := d.config.forKey(key); !exists {
		return nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadRaw(key, headers)
	}
}

func (d *multiClusterDownloader) downloadFile(key, path string) (f *os.File, err error) {
	if config, exists := d.config.forKey(key); !exists {
		return nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadFile(key, path)
	}
}

func (d *multiClusterDownloader) downloadBytes(key string) (data []byte, err error) {
	if config, exists := d.config.forKey(key); !exists {
		return nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadBytes(key)
	}
}

func (d *multiClusterDownloader) downloadRangeBytes(key string, offset, size int64) (l int64, data []byte, err error) {
	if config, exists := d.config.forKey(key); !exists {
		return 0, nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadRangeBytes(key, offset, size)
	}
}

func (d *multiClusterDownloader) downloadRangeReader(key string, offset, size int64) (l int64, reader io.ReadCloser, err error) {
	if config, exists := d.config.forKey(key); !exists {
		return 0, nil, ErrUndefinedConfig
	} else {
		return newSingleClusterDownloader(config).downloadRangeReader(key, offset, size)
	}

}
