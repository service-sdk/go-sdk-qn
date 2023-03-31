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

// CopyKeys 批量复制
func (l *Lister) CopyKeys(input []CopyKeyInput) ([]*CopyKeysError, error) {
	return l.copyKeys(context.Background(), input)
}

// MoveKeys 批量移动
func (l *Lister) MoveKeys(input []MoveKeyInput) ([]*MoveKeysError, error) {
	return l.moveKeys(context.Background(), input)
}

// RenameKeys 批量重命名
func (l *Lister) RenameKeys(input []RenameKeyInput) ([]*RenameKeysError, error) {
	return l.renameKeys(context.Background(), input)
}

// RenameDirectory 目录级别的Rename操作
func (l *Lister) RenameDirectory(srcDir, destDir string) (renameErrors []RenameKeysError, err error) {
	return l.renameDirectory(context.Background(), 20, srcDir, destDir)
}

// MoveDirectoryTo 目录级别的Move操作
func (l *Lister) MoveDirectoryTo(srcDir, destDir, toBucket string) (moveErrors []MoveKeysError, err error) {
	return l.moveDirectoryTo(context.Background(), 20, srcDir, destDir, toBucket)
}

// CopyDirectory 目录级别的Copy操作
func (l *Lister) CopyDirectory(srcDir, destDir string) (copyErrors []CopyKeysError, err error) {
	return l.copyDirectory(context.Background(), 20, srcDir, destDir)
}

// DeleteDirectory 目录级别的Delete操作
func (l *Lister) DeleteDirectory(dir string) (deleteErrors []DeleteKeysError, err error) {
	return l.deleteDirectory(context.Background(), 20, dir, false)
}

// ForceDeleteDirectory 目录级别的强制Delete操作
func (l *Lister) ForceDeleteDirectory(dir string) (deleteErrors []DeleteKeysError, err error) {
	return l.deleteDirectory(context.Background(), 20, dir, true)
}

// RenameDirectory 目录级别的Rename操作
func (l *Lister) renameDirectory(ctx context.Context, consumerCount int, srcDir, destDir string) (renameErrors []RenameKeysError, err error) {
	srcDirKey := makeSureKeyAsDir(srcDir)
	destDirKey := makeSureKeyAsDir(destDir)
	pool := goroutine_pool.NewGoroutinePoolWithoutLimit()

	// 处理错误
	resultErrors := make(chan RenameKeysError, 100)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range resultErrors {
			renameErrors = append(renameErrors, e)
		}
	}()

	// 列举所有的文件
	listPrefixChan := make(chan string, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(listPrefixChan)
		return l.listPrefixToChannel(ctx, srcDirKey, listPrefixChan)
	})

	// 生成重命名所有的文件的input
	renameKeyInputChan := make(chan RenameKeyInput, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(renameKeyInputChan)
		for key := range listPrefixChan {
			// replaceFirst
			destKey := destDirKey + key[len(srcDirKey):]
			renameKeyInputChan <- RenameKeyInput{
				FromKey: key,
				ToKey:   destKey,
			}
		}
		return nil
	})

	// 创建多个协程，同时执行Rename操作
	for i := 0; i < consumerCount; i++ {
		pool.Go(func(ctx context.Context) error {
			err := l.renameKeysFromChannel(ctx, renameKeyInputChan, resultErrors)
			if err != nil {
				return err
			}
			return nil
		})
	}
	// 等待所有的rename消费者结束
	err = pool.Wait(ctx)
	// 通知resultErrors的消费者结束
	close(resultErrors)
	// 等待resultErrors的消费者结束
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return renameErrors, nil
}

func (l *Lister) moveDirectoryTo(ctx context.Context, consumerCount int, srcDir, destDir, toBucket string) (moveErrors []MoveKeysError, err error) {
	srcDirKey := makeSureKeyAsDir(srcDir)
	destDirKey := makeSureKeyAsDir(destDir)
	pool := goroutine_pool.NewGoroutinePoolWithoutLimit()

	// 处理错误
	resultErrors := make(chan MoveKeysError, 100)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range resultErrors {
			moveErrors = append(moveErrors, e)
		}
	}()

	// 列举所有的文件
	listPrefixChan := make(chan string, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(listPrefixChan)
		return l.listPrefixToChannel(ctx, srcDirKey, listPrefixChan)
	})

	// 生成重命名所有的文件的input
	moveKeyInputChan := make(chan MoveKeyInput, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(moveKeyInputChan)
		for key := range listPrefixChan {
			// replaceFirst
			destKey := destDirKey + key[len(srcDirKey):]
			moveKeyInputChan <- MoveKeyInput{
				FromToKey: FromToKey{
					FromKey: key,
					ToKey:   destKey,
				},
				ToBucket: toBucket,
			}
		}
		return nil
	})

	// 创建多个协程作为消费者，同时执行操作
	for i := 0; i < consumerCount; i++ {
		pool.Go(func(ctx context.Context) error {
			err := l.moveKeysFromChannel(ctx, moveKeyInputChan, resultErrors)
			if err != nil {
				return err
			}
			return nil
		})
	}
	// 等待所有的rename消费者结束
	err = pool.Wait(ctx)
	// 通知resultErrors的消费者结束
	close(resultErrors)
	// 等待resultErrors的消费者结束
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return moveErrors, nil
}

func (l *Lister) copyDirectory(ctx context.Context, consumerCount int, srcDir, destDir string) (copyErrors []CopyKeysError, err error) {
	srcDirKey := makeSureKeyAsDir(srcDir)
	destDirKey := makeSureKeyAsDir(destDir)
	pool := goroutine_pool.NewGoroutinePoolWithoutLimit()

	// 处理错误
	resultErrors := make(chan CopyKeysError, 100)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range resultErrors {
			copyErrors = append(copyErrors, e)
		}
	}()

	// 列举所有的文件
	listPrefixChan := make(chan string, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(listPrefixChan)
		return l.listPrefixToChannel(ctx, srcDirKey, listPrefixChan)
	})

	// 生成重命名所有的文件的input
	copyKeyInputChan := make(chan CopyKeyInput, 100)
	pool.Go(func(ctx context.Context) error {
		defer close(copyKeyInputChan)
		for key := range listPrefixChan {
			// replaceFirst
			destKey := destDirKey + key[len(srcDirKey):]
			copyKeyInputChan <- CopyKeyInput{
				FromKey: key,
				ToKey:   destKey,
			}
		}
		return nil
	})

	// 创建多个协程作为消费者，同时执行操作
	for i := 0; i < consumerCount; i++ {
		pool.Go(func(ctx context.Context) error {
			err := l.copyKeysFromChannel(ctx, copyKeyInputChan, resultErrors)
			if err != nil {
				return err
			}
			return nil
		})
	}
	// 等待所有的消费者结束
	err = pool.Wait(ctx)
	// 通知resultErrors的消费者结束
	close(resultErrors)
	// 等待resultErrors的消费者结束
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return copyErrors, nil
}

// deleteDirectory 目录级别的强制Delete操作
func (l *Lister) deleteDirectory(ctx context.Context, consumerCount int, dir string, isForce bool) (deleteErrors []DeleteKeysError, err error) {
	dirKey := makeSureKeyAsDir(dir)
	pool := goroutine_pool.NewGoroutinePoolWithoutLimit()

	deleteKeyErrorChan := make(chan DeleteKeysError, 1000)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for e := range deleteKeyErrorChan {
			deleteErrors = append(deleteErrors, e)
		}
	}()

	keyChan := make(chan string, 1000)
	// deleter consumer
	pool.Go(func(ctx context.Context) error {
		defer close(deleteKeyErrorChan)
		return l.deleteKeysFromChannel(ctx, keyChan, isForce, deleteKeyErrorChan)
	})
	// lister producer
	pool.Go(func(ctx context.Context) error {
		defer close(keyChan)
		return l.listPrefixToChannel(ctx, dirKey, keyChan)
	})

	err = pool.Wait(ctx)
	wg.Wait()
	if err != nil {
		return nil, err
	}
	return deleteErrors, nil
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
