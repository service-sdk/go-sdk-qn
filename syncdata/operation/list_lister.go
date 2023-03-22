package operation

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"github.com/service-sdk/go-sdk-qn/x/httputil.v1"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
)

var (
	ErrUndefinedConfig                        = errors.New("undefined config")
	ErrCannotTransferBetweenDifferentClusters = errors.New("cannot transfer between different clusters")
)

// Lister 列举器
type Lister struct {
	config                   Configurable
	singleClusterLister      *singleClusterLister
	multiClustersConcurrency int
}

// NewLister 根据配置创建列举器
func NewLister(c *Config) *Lister {
	return &Lister{config: c, singleClusterLister: newSingleClusterLister(c)}
}

// NewListerV2 根据环境变量创建列举器
func NewListerV2() *Lister {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	} else if singleClusterConfig, ok := c.(*Config); ok {
		return NewLister(singleClusterConfig)
	} else {
		var (
			concurrency = 1
			err         error
		)
		if concurrencyStr := os.Getenv("QINIU_MULTI_CLUSTERS_CONCURRENCY"); concurrencyStr != "" {
			if concurrency, err = strconv.Atoi(concurrencyStr); err != nil {
				elog.Warn("Invalid QINIU_MULTI_CLUSTERS_CONCURRENCY: ", err)
			}
		}
		return &Lister{config: c, multiClustersConcurrency: concurrency}
	}
}

// FileStat 文件元信息
type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	code int
}

// Rename 重命名对象
func (l *Lister) Rename(fromKey, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.rename(fromKey, toKey)
}

// MoveTo 移动对象到指定存储空间的指定对象中
func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.moveTo(fromKey, toBucket, toKey)
}

// Copy 复制对象到当前存储空间的指定对象中
func (l *Lister) Copy(fromKey, toKey string) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, err := l.canTransfer(fromKey, toKey)
		if err != nil {
			return err
		}
		scl = newSingleClusterLister(c)
	}
	return scl.copy(fromKey, toKey)
}

func (l *Lister) canTransfer(fromKey, toKey string) (*Config, error) {
	configOfFromKey, exists := l.config.forKey(fromKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}
	configOfToKey, exists := l.config.forKey(toKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}
	if configOfFromKey != configOfToKey {
		return nil, ErrCannotTransferBetweenDifferentClusters
	}
	return configOfFromKey, nil
}

// Delete 删除指定对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) Delete(key string) error {
	return l.delete(key, false)
}

// ForceDelete 强制删除指定对象，无论是否配置回收站，该 API 都会直接删除文件
func (l *Lister) ForceDelete(key string) error {
	return l.delete(key, true)
}

func (l *Lister) delete(key string, isForce bool) error {
	var scl *singleClusterLister
	if l.singleClusterLister != nil {
		scl = l.singleClusterLister
	} else {
		c, exists := l.config.forKey(key)
		if !exists {
			return ErrUndefinedConfig
		}
		scl = newSingleClusterLister(c)
	}
	return scl.delete(key, isForce)
}

// ListStat 获取指定对象列表的元信息
func (l *Lister) ListStat(keys []string) []*FileStat {
	if fileStats, err := l.listStat(context.Background(), keys); err != nil {
		return []*FileStat{}
	} else {
		return fileStats
	}
}

func (l *Lister) listStat(ctx context.Context, keys []string) ([]*FileStat, error) {
	if l.singleClusterLister != nil {
		return l.singleClusterLister.listStat(ctx, keys)
	}

	type KeysWithIndex struct {
		IndexMap []int
		Keys     []string
	}

	clusterPathsMap := make(map[*Config]*KeysWithIndex)
	for i, key := range keys {
		config, exists := l.config.forKey(key)
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

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	allStats := make([]*FileStat, len(keys))
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				if stats, err := l.listStatForConfig(ctx, config, keys); err != nil {
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

func (l *Lister) listStatForConfig(ctx context.Context, config *Config, keys []string) ([]*FileStat, error) {
	return newSingleClusterLister(config).listStat(ctx, keys)
}

// ListPrefix 根据前缀列举存储空间
func (l *Lister) ListPrefix(prefix string) []string {
	if keys, err := l.listPrefix(context.Background(), prefix); err != nil {
		return []string{}
	} else {
		return keys
	}
}

func (l *Lister) listPrefix(ctx context.Context, prefix string) ([]string, error) {
	if l.singleClusterLister != nil {
		return l.singleClusterLister.listPrefix(ctx, prefix)
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	allKeys := make([]string, 0)
	var allKeysMutex sync.Mutex
	l.config.forEachClusterConfig(func(_ string, config *Config) error {
		pool.Go(func(ctx context.Context) error {
			if keys, err := l.listPrefixForConfig(ctx, config, prefix); err != nil {
				return err
			} else {
				allKeysMutex.Lock()
				allKeys = append(allKeys, keys...)
				allKeysMutex.Unlock()
				return nil
			}
		})
		return nil
	})
	err := pool.Wait(ctx)
	sort.Strings(allKeys) // 对所有 key 排序，模拟从一个集群的效果
	return allKeys, err
}

func (l *Lister) listPrefixForConfig(ctx context.Context, config *Config, prefix string) ([]string, error) {
	return newSingleClusterLister(config).listPrefix(ctx, prefix)
}

// DeleteKeys 删除多个对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) DeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys, false)
}

// ForceDeleteKeys 强制删除多个对象，无论是否配置回收站，该 API 都会直接删除文件
func (l *Lister) ForceDeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys, true)
}

func (l *Lister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	if l.singleClusterLister != nil {
		return l.singleClusterLister.deleteKeys(ctx, keys, isForce)
	}

	type KeysWithIndex struct {
		IndexMap []int
		Keys     []string
	}

	clusterPathsMap := make(map[*Config]*KeysWithIndex)
	for i, key := range keys {
		config, exists := l.config.forKey(key)
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

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	allErrors := make([]*DeleteKeysError, len(keys))
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				if errors, err := l.deleteKeysForConfig(ctx, config, keys, isForce); err != nil {
					return err
				} else {
					for i := range errors {
						allErrors[indexMap[i]] = errors[i]
					}
					return nil
				}
			})
		}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
	}
	err := pool.Wait(ctx)
	return allErrors, err
}

func (l *Lister) deleteKeysForConfig(ctx context.Context, config *Config, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	return newSingleClusterLister(config).deleteKeys(ctx, keys, isForce)
}

// RenameDirectory 目录级别的Rename操作
func (l *Lister) RenameDirectory(srcDir, destDir string) error {
	// TODO
	return nil
}

// MoveDirectoryTo 目录级别的Move操作
func (l *Lister) MoveDirectoryTo(srcDir, destDir string) error {
	// TODO
	return nil
}

// CopyDirectory 目录级别的Copy操作
func (l *Lister) CopyDirectory(srcDir, destDir string) error {
	// TODO
	return nil
}

// DeleteDirectory 目录级别的Delete操作
func (l *Lister) DeleteDirectory(dir string) error {
	// TODO
	return nil
}

// ForceDeleteDirectory 目录级别的强制Delete操作
func (l *Lister) ForceDeleteDirectory(dir string) error {
	// TODO
	return nil
}

func (l *Lister) batchStab(r io.Reader) []*FileStat {
	j := json.NewDecoder(r)
	var fl []string
	err := j.Decode(&fl)
	if err != nil {
		elog.Error(err)
		return nil
	}
	return l.ListStat(fl)
}

func isServerError(err error) bool {
	if err != nil {
		code := httputil.DetectCode(err)
		return code/100 == 5
	}
	return false
}
