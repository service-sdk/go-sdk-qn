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

func newMultiClusterLister(config Configurable, multiClustersConcurrency int) *multiClusterLister {
	return &multiClusterLister{
		config:                   config,
		multiClustersConcurrency: multiClustersConcurrency,
	}
}

type valuesWithIndices[V any] struct {
	IndexMap []int
	Values   []V
}

// 根据key判定两个对象是否可以进行集群转移操作
// 只有当fromKey和toKey都能找到相应配置文件且两个配置文件相同时即同一集群节点才可以进行转移
func (l *multiClusterLister) canTransfer(fromKey, toKey string) (*Config, error) {
	configOfFromKey, exists := l.config.forKey(fromKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}
	configOfToKey, exists := l.config.forKey(toKey)
	if !exists {
		return nil, ErrUndefinedConfig
	}

	// 两个不同的集群之间不能进行互相转移
	if configOfFromKey != configOfToKey {
		return nil, ErrCannotTransferBetweenDifferentClusters
	}
	return configOfFromKey, nil
}

func (l *multiClusterLister) groupBy(keys []string) (clusterPathsMap map[*Config]*valuesWithIndices[string], err error) {
	// 将keys按照集群进行分组
	clusterPathsMap = make(map[*Config]*valuesWithIndices[string])
	for i, key := range keys {
		config, exists := l.config.forKey(key)
		if !exists {
			return nil, ErrUndefinedConfig
		}

		// 不包含则创建
		if e, contains := clusterPathsMap[config]; !contains {
			e = &valuesWithIndices[string]{Values: make([]string, 0, 1), IndexMap: make([]int, 0, 1)}
			clusterPathsMap[config] = e
		}

		e := clusterPathsMap[config]

		// 将key添加到对应的集群中
		e.IndexMap = append(e.IndexMap, i)
		e.Values = append(e.Values, key)
	}
	return clusterPathsMap, nil
}

func (l *multiClusterLister) listStat(ctx context.Context, keys []string) ([]*FileStat, error) {
	result := make([]*FileStat, len(keys))

	clusterPathsMap, err := l.groupBy(keys)
	if err != nil {
		return nil, err
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				stats, err := newSingleClusterLister(config).listStat(ctx, keys)
				if err != nil {
					return err
				}
				for i := range stats {
					result[indexMap[i]] = stats[i]
				}
				return nil
			})
		}(config, keysWithIndex.Values, keysWithIndex.IndexMap)
	}
	err = pool.Wait(ctx)
	return result, err
}

func (l *multiClusterLister) listPrefixToChannel(ctx context.Context, prefix string, output chan<- string) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	_ = l.config.forEachClusterConfig(func(_ string, config *Config) error {
		// 为每个集群创建一个 goroutine 进行 list 到 channel 中
		pool.Go(func(ctx context.Context) error {
			err := newSingleClusterLister(config).listPrefixToChannel(ctx, prefix, output)
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

func (l *multiClusterLister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	result := make([]*DeleteKeysError, len(keys))

	clusterPathsMap, err := l.groupBy(keys)
	if err != nil {
		return nil, err
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, keys []string, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				deleteErrors, err := newSingleClusterLister(config).deleteKeys(ctx, keys, isForce)
				if err != nil {
					return err
				}
				for i, deleteError := range deleteErrors {
					result[indexMap[i]] = deleteError
				}
				return nil
			})
		}(config, keysWithIndex.Values, keysWithIndex.IndexMap)
	}
	err = pool.Wait(ctx)
	return result, err
}

func (l *multiClusterLister) copyKeys(ctx context.Context, inputs []CopyKeyInput) ([]*CopyKeysError, error) {
	result := make([]*CopyKeysError, len(inputs))

	clusterPathsMap := make(map[*Config]*valuesWithIndices[CopyKeyInput])

	// 将所有的canTransfer不满足的key提前赋值
	for i, input := range inputs {
		if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
			result[i] = &CopyKeysError{
				Code:    -1,
				Error:   err.Error(),
				FromKey: input.FromKey,
				ToKey:   input.ToKey,
			}
		} else {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return nil, ErrUndefinedConfig
			}
			// 都是canTransfer的, 不包含则创建
			if e, contains := clusterPathsMap[config]; !contains {
				e = &valuesWithIndices[CopyKeyInput]{
					Values:   make([]CopyKeyInput, 0, 1),
					IndexMap: make([]int, 0, 1),
				}
				clusterPathsMap[config] = e
			}

			e := clusterPathsMap[config]

			// 将key添加到对应的集群中
			e.IndexMap = append(e.IndexMap, i)
			e.Values = append(e.Values, input)
		}
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, inputs []CopyKeyInput, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				copyErrors, err := newSingleClusterLister(config).copyKeys(ctx, inputs)
				if err != nil {
					return err
				}
				for i, copyError := range copyErrors {
					result[indexMap[i]] = copyError
				}
				return nil
			})
		}(config, keysWithIndex.Values, keysWithIndex.IndexMap)
	}

	err := pool.Wait(ctx)
	return result, err
}

func (l *multiClusterLister) moveKeys(ctx context.Context, input []MoveKeyInput) ([]*MoveKeysError, error) {
	result := make([]*MoveKeysError, len(input))

	clusterPathsMap := make(map[*Config]*valuesWithIndices[MoveKeyInput])

	// 将所有的canTransfer不满足的key提前赋值
	for i, input := range input {
		if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
			result[i] = &MoveKeysError{
				FromToKeyError: FromToKeyError{
					Code:    -1,
					Error:   err.Error(),
					FromKey: input.FromKey,
					ToKey:   input.ToKey,
				},
				ToBucket: input.ToBucket,
			}
		} else {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return nil, ErrUndefinedConfig
			}
			// 都是canTransfer的, 不包含则创建
			if e, contains := clusterPathsMap[config]; !contains {
				e = &valuesWithIndices[MoveKeyInput]{
					Values:   make([]MoveKeyInput, 0, 1),
					IndexMap: make([]int, 0, 1),
				}
				clusterPathsMap[config] = e
			}

			e := clusterPathsMap[config]

			// 将key添加到对应的集群中
			e.IndexMap = append(e.IndexMap, i)
			e.Values = append(e.Values, input)
		}
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, inputs []MoveKeyInput, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				moveErrors, err := newSingleClusterLister(config).moveKeys(ctx, inputs)
				if err != nil {
					return err
				}
				for i, moveError := range moveErrors {
					result[indexMap[i]] = moveError
				}
				return nil
			})
		}(config, keysWithIndex.Values, keysWithIndex.IndexMap)
	}

	err := pool.Wait(ctx)
	return result, err
}

func (l *multiClusterLister) renameKeys(ctx context.Context, input []RenameKeyInput) ([]*RenameKeysError, error) {
	result := make([]*RenameKeysError, len(input))

	clusterPathsMap := make(map[*Config]*valuesWithIndices[RenameKeyInput])

	// 将所有的canTransfer不满足的key提前赋值
	for i, input := range input {
		if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
			result[i] = &RenameKeysError{
				Code:    -1,
				Error:   err.Error(),
				FromKey: input.FromKey,
				ToKey:   input.ToKey,
			}
		} else {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return nil, ErrUndefinedConfig
			}
			// 都是canTransfer的, 不包含则创建
			if e, contains := clusterPathsMap[config]; !contains {
				e = &valuesWithIndices[RenameKeyInput]{
					Values:   make([]RenameKeyInput, 0, 1),
					IndexMap: make([]int, 0, 1),
				}
				clusterPathsMap[config] = e
			}

			e := clusterPathsMap[config]

			// 将key添加到对应的集群中
			e.IndexMap = append(e.IndexMap, i)
			e.Values = append(e.Values, input)
		}
	}

	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	for config, keysWithIndex := range clusterPathsMap {
		func(config *Config, inputs []RenameKeyInput, indexMap []int) {
			pool.Go(func(ctx context.Context) error {
				renameErrors, err := newSingleClusterLister(config).renameKeys(ctx, inputs)
				if err != nil {
					return err
				}
				for i, renameError := range renameErrors {
					result[indexMap[i]] = renameError
				}
				return nil
			})
		}(config, keysWithIndex.Values, keysWithIndex.IndexMap)
	}

	err := pool.Wait(ctx)
	return result, err
}

func (l *multiClusterLister) deleteKeysFromChannel(ctx context.Context, keysChan <-chan string, isForce bool, errorsChan chan<- DeleteKeysError) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	clusterPathsMap := make(map[*Config][]string)

	type configAndKeys struct {
		config *Config
		keys   []string
	}

	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, keys []string) {
				pool.Go(func(ctx context.Context) error {
					deleteErrors, err := newSingleClusterLister(config).deleteKeys(ctx, keys, isForce)
					if err != nil {
						return err
					}
					for _, deleteError := range deleteErrors {
						if deleteError != nil {
							errorsChan <- *deleteError
						}
					}
					return nil
				})
			}(configAndKeys.config, configAndKeys.keys)
		}
	}()

	pool.Go(func(ctx context.Context) error {
		for key := range keysChan {
			config, exists := l.config.forKey(key)
			if !exists {
				return ErrUndefinedConfig
			}
			clusterPathsMap[config] = append(clusterPathsMap[config], key)

			// 当某个集群中的key数量达到100时, 将其放入对应的集群任务中
			if len(clusterPathsMap[config]) > 100 {
				configAndKeysChan <- configAndKeys{
					config: config,
					keys:   clusterPathsMap[config],
				}
				clusterPathsMap[config] = make([]string, 0)
			}
		}
		for config, keys := range clusterPathsMap {
			if len(keys) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					keys:   keys,
				}
			}
		}
		close(configAndKeysChan)
		return nil
	})

	return pool.Wait(ctx)
}

func (l *multiClusterLister) copyKeysFromChannel(ctx context.Context, input <-chan CopyKeyInput, errorsChan chan<- CopyKeysError) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	clusterPathsMap := make(map[*Config][]CopyKeyInput)

	type configAndKeys struct {
		config *Config
		inputs []CopyKeyInput
	}

	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []CopyKeyInput) {
				pool.Go(func(ctx context.Context) error {
					copyErrors, err := newSingleClusterLister(config).copyKeys(ctx, inputs)
					if err != nil {
						return err
					}
					for _, copyError := range copyErrors {
						if copyError != nil {
							errorsChan <- *copyError
						}
					}
					return nil
				})
			}(configAndKeys.config, configAndKeys.inputs)
		}
	}()

	pool.Go(func(ctx context.Context) error {
		for input := range input {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return ErrUndefinedConfig
			}
			clusterPathsMap[config] = append(clusterPathsMap[config], input)

			// 当某个集群中的key数量达到100时, 将其放入对应的集群任务中
			if len(clusterPathsMap[config]) > 100 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: clusterPathsMap[config],
				}
				clusterPathsMap[config] = make([]CopyKeyInput, 0)
			}
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		close(configAndKeysChan)
		return nil
	})

	return pool.Wait(ctx)
}

func (l *multiClusterLister) moveKeysFromChannel(ctx context.Context, input <-chan MoveKeyInput, errorsChan chan<- MoveKeysError) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	clusterPathsMap := make(map[*Config][]MoveKeyInput)

	type configAndKeys struct {
		config *Config
		inputs []MoveKeyInput
	}

	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []MoveKeyInput) {
				pool.Go(func(ctx context.Context) error {
					moveErrors, err := newSingleClusterLister(config).moveKeys(ctx, inputs)
					if err != nil {
						return err
					}
					for _, moveError := range moveErrors {
						if moveError != nil {
							errorsChan <- *moveError
						}
					}
					return nil
				})
			}(configAndKeys.config, configAndKeys.inputs)
		}
	}()

	pool.Go(func(ctx context.Context) error {
		for input := range input {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return ErrUndefinedConfig
			}
			clusterPathsMap[config] = append(clusterPathsMap[config], input)

			// 当某个集群中的key数量达到100时, 将其放入对应的集群任务中
			if len(clusterPathsMap[config]) > 100 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: clusterPathsMap[config],
				}
				clusterPathsMap[config] = make([]MoveKeyInput, 0)
			}
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		close(configAndKeysChan)
		return nil
	})

	return pool.Wait(ctx)
}

func (l *multiClusterLister) renameKeysFromChannel(ctx context.Context, input <-chan RenameKeyInput, errorsChan chan<- RenameKeysError) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	clusterPathsMap := make(map[*Config][]RenameKeyInput)

	type configAndKeys struct {
		config *Config
		inputs []RenameKeyInput
	}

	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []RenameKeyInput) {
				pool.Go(func(ctx context.Context) error {
					renameErrors, err := newSingleClusterLister(config).renameKeys(ctx, inputs)
					if err != nil {
						return err
					}
					for _, renameError := range renameErrors {
						if renameError != nil {
							errorsChan <- *renameError
						}
					}
					return nil
				})
			}(configAndKeys.config, configAndKeys.inputs)
		}
	}()

	pool.Go(func(ctx context.Context) error {
		for input := range input {
			config, exists := l.config.forKey(input.FromKey)
			if !exists {
				return ErrUndefinedConfig
			}
			clusterPathsMap[config] = append(clusterPathsMap[config], input)

			// 当某个集群中的key数量达到100时, 将其放入对应的集群任务中
			if len(clusterPathsMap[config]) > 100 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: clusterPathsMap[config],
				}
				clusterPathsMap[config] = make([]RenameKeyInput, 0)
			}
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		close(configAndKeysChan)
		return nil
	})

	return pool.Wait(ctx)
}
