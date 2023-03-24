package operation

import (
	"context"
	"errors"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"sort"
	"sync"
)

var (
	ErrUndefinedConfig                        = errors.New("undefined config")
	ErrCannotTransferBetweenDifferentClusters = errors.New("cannot transfer between different clusters")
)

type multiClusterLister struct {
	config                   Configurable
	multiClustersConcurrency int
}

func newMultiClusterLister(config Configurable, multiClustersConcurrency int) clusterLister {
	return &multiClusterLister{
		config:                   config,
		multiClustersConcurrency: multiClustersConcurrency,
	}
}

// 根据key判定两个对象是否可以进行转移操作
func (l *multiClusterLister) canTransfer(fromKey, toKey string) (*Config, error) {
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

func (l *multiClusterLister) listStatForConfig(ctx context.Context, config *Config, keys []string) ([]*FileStat, error) {
	return newSingleClusterLister(config).listStat(ctx, keys)
}
func (l *multiClusterLister) listStat(ctx context.Context, keys []string) ([]*FileStat, error) {
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
				stats, err := l.listStatForConfig(ctx, config, keys)
				if err != nil {
					return err
				}
				for i := range stats {
					allStats[indexMap[i]] = stats[i]
				}
				return nil
			})
		}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
	}
	err := pool.Wait(ctx)
	return allStats, err
}

func (l *multiClusterLister) listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	l.config.forEachClusterConfig(func(_ string, config *Config) error {
		pool.Go(func(ctx context.Context) error {
			err := newSingleClusterLister(config).listPrefixToChannel(ctx, prefix, ch)
			if err != nil {
				return err
			}
			return nil
		})
		return nil
	})
	return pool.Wait(ctx)
}

func (l *multiClusterLister) listPrefix(ctx context.Context, prefix string) ([]string, error) {
	allKeys := make([]string, 0)
	ch := make(chan string, 100)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for key := range ch {
			allKeys = append(allKeys, key)
		}
		wg.Done()
	}()

	err := l.listPrefixToChannel(ctx, prefix, ch)
	close(ch)
	wg.Wait()

	if err != nil {
		return nil, err
	}
	sort.Strings(allKeys) // 对所有 key 排序，模拟从一个集群的效果
	return allKeys, err
}

func (l *multiClusterLister) delete(key string, isForce bool) error {
	c, exists := l.config.forKey(key)
	if !exists {
		return ErrUndefinedConfig
	}
	return newSingleClusterLister(c).delete(key, isForce)
}

func (l *multiClusterLister) copy(fromKey, toKey string) error {
	c, err := l.canTransfer(fromKey, toKey)
	if err != nil {
		return err
	}
	return newSingleClusterLister(c).copy(fromKey, toKey)
}
func (l *multiClusterLister) moveTo(fromKey string, toBucket string, toKey string) error {
	c, err := l.canTransfer(fromKey, toKey)
	if err != nil {
		return err
	}
	return newSingleClusterLister(c).moveTo(fromKey, toBucket, toKey)
}

func (l *multiClusterLister) rename(fromKey, toKey string) error {
	c, err := l.canTransfer(fromKey, toKey)
	if err != nil {
		return err
	}
	return newSingleClusterLister(c).rename(fromKey, toKey)
}

func (l *multiClusterLister) deleteKeysForConfig(ctx context.Context, config *Config, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	return newSingleClusterLister(config).deleteKeys(ctx, keys, isForce)
}
func (l *multiClusterLister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {

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
				deleteErrors, err := l.deleteKeysForConfig(ctx, config, keys, isForce)
				if err != nil {
					return err
				}
				for i, deleteError := range deleteErrors {
					allErrors[indexMap[i]] = deleteError
				}
				return nil
			})
		}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
	}
	err := pool.Wait(ctx)
	return allErrors, err
}

func (l *multiClusterLister) copyKeys(ctx context.Context, input []CopyKeyInput) ([]*CopyKeysError, error) {
	// TODO
	return nil, nil
}
func (l *multiClusterLister) moveKeys(ctx context.Context, input []MoveKeyInput) ([]*MoveKeysError, error) {
	// TODO
	return nil, nil
}
func (l *multiClusterLister) renameKeys(ctx context.Context, input []RenameKeyInput) ([]*RenameKeysError, error) {
	// TODO
	return nil, nil
}
