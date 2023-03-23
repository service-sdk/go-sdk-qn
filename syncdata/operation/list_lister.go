package operation

import (
	"context"
	"encoding/json"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"github.com/service-sdk/go-sdk-qn/x/httputil.v1"
	"io"
	"os"
	"strconv"
	"sync"
)

type clusterLister interface {
	listStat(ctx context.Context, keys []string) ([]*FileStat, error)
	listPrefix(ctx context.Context, prefix string) ([]string, error)
	listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error
	delete(key string, isForce bool) error
	copy(fromKey, toKey string) error
	moveTo(fromKey string, toBucket string, toKey string) error
	rename(fromKey, toKey string) error
	deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error)
}

// Lister 列举器
type Lister struct {
	clusterLister
}

// NewLister 根据配置创建列举器
func NewLister(c *Config) *Lister {
	return &Lister{newSingleClusterLister(c)}
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
		return &Lister{newMultiClusterLister(c, concurrency)}
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
	return l.rename(fromKey, toKey)
}

// MoveTo 移动对象到指定存储空间的指定对象中
func (l *Lister) MoveTo(fromKey, toBucket, toKey string) error {
	return l.moveTo(fromKey, toBucket, toKey)
}

// Copy 复制对象到当前存储空间的指定对象中
func (l *Lister) Copy(fromKey, toKey string) error {
	return l.copy(fromKey, toKey)
}

// Delete 删除指定对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) Delete(key string) error {
	return l.delete(key, false)
}

// ForceDelete 强制删除指定对象，无论是否配置回收站，该 API 都会直接删除文件
func (l *Lister) ForceDelete(key string) error {
	return l.delete(key, true)
}

// ListStat 获取指定对象列表的元信息
func (l *Lister) ListStat(keys []string) []*FileStat {
	if fileStats, err := l.listStat(context.Background(), keys); err != nil {
		return []*FileStat{}
	} else {
		return fileStats
	}
}

// ListPrefix 根据前缀列举存储空间
func (l *Lister) ListPrefix(prefix string) []string {
	keys, err := l.listPrefix(context.Background(), prefix)
	if err != nil {
		return []string{}
	}
	return keys
}

// DeleteKeys 删除多个对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) DeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys, false)
}

// ForceDeleteKeys 强制删除多个对象，无论是否配置回收站，该 API 都会直接删除文件
func (l *Lister) ForceDeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys, true)
}

type RenameDirectoryError struct {
	srcKey  string
	destKey string
	err     error
}

// RenameDirectory 目录级别的Rename操作
func (l *Lister) RenameDirectory(srcDir, destDir string) (renameErrors []RenameDirectoryError) {
	var renameErrorsMutex sync.Mutex

	ch := make(chan string, 100)

	for i := 0; i < 10; i++ {
		go func() {
			for key := range ch {
				destKey := destDir + key[len(srcDir):]
				err := l.Rename(key, destKey)

				// 没有错误，继续
				if err == nil {
					continue
				}

				// 有错误，记录下来
				func() {
					renameErrorsMutex.Lock()
					defer renameErrorsMutex.Unlock()
					renameErrors = append(renameErrors, RenameDirectoryError{
						srcKey:  key,
						destKey: destKey,
						err:     err,
					})
				}()
			}
		}()
	}

	for _, key := range l.ListPrefix(srcDir) {
		ch <- key
	}
	close(ch)
	return nil
}

// MoveDirectoryTo 目录级别的Move操作
func (l *Lister) MoveDirectoryTo(srcDir, destDir string) error {
	// TODO
	return nil
}

// CopyDirectory 目录级别的Copy操作
func (l *Lister) CopyDirectory(srcDir, destDir string) error {
	ch := make(chan string, 100)
	pool := goroutine_pool.NewGoroutinePool(10)
	pool.Go(func(ctx context.Context) error {
		// 积攒够100个key，就批量copy
		//var keys []string
		//for key := range ch {
		//	keys = append(keys, key)
		//	if len(keys) >= 100 {
		//		l.CopyKeys(keys, destDir)
		//		keys = nil
		//	}
		//}
		return nil
	})
	l.listPrefixToChannel(context.Background(), srcDir, ch)

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
