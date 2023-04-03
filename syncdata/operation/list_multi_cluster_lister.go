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

// 根据key判定两个对象是否可以进行集群传输操作
// 只有当fromKey和toKey都能找到相应配置文件且两个配置文件相同时即同一集群节点才可以进行转移
func (l *multiClusterLister) canTransfer(fromKey, toKey string) (*Config, error) {
	if configOfFromKey, exists := l.config.forKey(fromKey); !exists {
		// fromKey的配置文件不存在
		return nil, ErrUndefinedConfig
	} else if configOfToKey, exists := l.config.forKey(toKey); !exists {
		// toKey的配置文件不存在
		return nil, ErrUndefinedConfig
	} else if configOfFromKey != configOfToKey {
		// 两个不同的集群之间不能进行互相传输
		return nil, ErrCannotTransferBetweenDifferentClusters
	} else {
		// 两个配置文件相同，即同一集群节点，可以进行传输
		return configOfFromKey, nil
	}
}

type keysWithIndices struct {
	Keys     []string
	IndexMap []int
}

func (l *multiClusterLister) groupBy(keys []string) (clusterPathsMap map[*Config]*keysWithIndices, err error) {
	// 将keys按照集群进行分组
	clusterPathsMap = make(map[*Config]*keysWithIndices)

	for i, key := range keys {
		if config, exists := l.config.forKey(key); !exists {
			return nil, ErrUndefinedConfig
		} else {
			// 不包含则创建
			if e, contains := clusterPathsMap[config]; !contains {
				e = &keysWithIndices{Keys: make([]string, 0, 1), IndexMap: make([]int, 0, 1)}
				clusterPathsMap[config] = e
			}

			e := clusterPathsMap[config]

			// 将key添加到对应的集群中
			e.IndexMap = append(e.IndexMap, i)
			e.Keys = append(e.Keys, key)
		}
	}
	return clusterPathsMap, nil
}

// 并发列举每个
func (l *multiClusterLister) listStat(ctx context.Context, keys []string) ([]*FileStat, error) {
	if clusterPathsMap, err := l.groupBy(keys); err != nil {
		return nil, err
	} else {
		result := make([]*FileStat, len(keys))
		pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
		for config, keysWithIndex := range clusterPathsMap {
			func(config *Config, keys []string, indexMap []int) {
				pool.Go(func(ctx context.Context) error {
					if stats, err := newSingleClusterLister(config).listStat(ctx, keys); err != nil {
						return err
					} else {
						for i := range stats {
							result[indexMap[i]] = stats[i]
						}
						return nil
					}
				})
			}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
		}
		return result, pool.Wait(ctx)
	}
}

func (l *multiClusterLister) listPrefixToChannel(ctx context.Context, prefix string, output chan<- string) error {
	pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	_ = l.config.forEachClusterConfig(func(_ string, config *Config) error {
		// 为每个集群创建一个 goroutine 进行 list 到 channel 中
		pool.Go(func(ctx context.Context) error {
			if err := newSingleClusterLister(config).listPrefixToChannel(ctx, prefix, output); err != nil {
				return err
			} else {
				return nil
			}
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
		defer wg.Done()
		for key := range ch {
			allKeys = append(allKeys, key)
		}
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
	if c, exists := l.config.forKey(key); !exists {
		return ErrUndefinedConfig
	} else {
		return newSingleClusterLister(c).delete(key, isForce)
	}
}

func (l *multiClusterLister) copy(fromKey, toKey string) error {
	if c, err := l.canTransfer(fromKey, toKey); err != nil {
		return err
	} else {
		return newSingleClusterLister(c).copy(fromKey, toKey)
	}
}
func (l *multiClusterLister) moveTo(fromKey string, toBucket string, toKey string) error {
	if c, err := l.canTransfer(fromKey, toKey); err != nil {
		return err
	} else {
		return newSingleClusterLister(c).moveTo(fromKey, toBucket, toKey)
	}
}

func (l *multiClusterLister) rename(fromKey, toKey string) error {
	if c, err := l.canTransfer(fromKey, toKey); err != nil {
		return err
	} else {
		return newSingleClusterLister(c).rename(fromKey, toKey)
	}
}

func (l *multiClusterLister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	if clusterPathsMap, err := l.groupBy(keys); err != nil {
		// 只要一个key找不到对应的集群，就直接返回错误
		return nil, err
	} else {
		result := make([]*DeleteKeysError, len(keys))
		pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
		for config, keysWithIndex := range clusterPathsMap {
			func(config *Config, keys []string, indexMap []int) {
				pool.Go(func(ctx context.Context) error {
					if deleteErrors, err := newSingleClusterLister(config).deleteKeys(ctx, keys, isForce); err != nil {
						// 只要有一个集群删除失败了，就停止所有的删除，并返回错误
						return err
					} else {
						for i, deleteError := range deleteErrors {
							result[indexMap[i]] = deleteError
						}
						return nil
					}
				})
			}(config, keysWithIndex.Keys, keysWithIndex.IndexMap)
		}
		return result, pool.Wait(ctx)
	}
}

func (l *multiClusterLister) copyKeys(ctx context.Context, inputs []CopyKeyInput) ([]*CopyKeysError, error) {
	type copyKeyInputsWithIndices struct {
		Inputs   []CopyKeyInput
		IndexMap []int
	}

	// 将所有的input按照fromKey所在集群进行分组
	groupByInputs := func() (map[*Config]*copyKeyInputsWithIndices, error) {
		clusterPathsMap := make(map[*Config]*copyKeyInputsWithIndices)

		// 将所有的canTransfer不满足的key提前赋值
		for i, input := range inputs {
			if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
				return nil, err
			} else {
				config, _ := l.config.forKey(input.FromKey)

				e, contains := clusterPathsMap[config]
				if !contains {
					// 都是canTransfer的, 不包含则创建
					clusterPathsMap[config] = &copyKeyInputsWithIndices{
						Inputs:   make([]CopyKeyInput, 0, 1),
						IndexMap: make([]int, 0, 1),
					}
					e = clusterPathsMap[config]
				}

				// 将key添加到对应的集群中
				e.IndexMap = append(e.IndexMap, i)
				e.Inputs = append(e.Inputs, input)
			}
		}

		return clusterPathsMap, nil
	}

	if clusterPathsMap, err := groupByInputs(); err != nil {
		return nil, err
	} else {
		result := make([]*CopyKeysError, len(inputs))
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
			}(config, keysWithIndex.Inputs, keysWithIndex.IndexMap)
		}
		return result, pool.Wait(ctx)
	}
}

func (l *multiClusterLister) moveKeys(ctx context.Context, input []MoveKeyInput) ([]*MoveKeysError, error) {
	type moveKeyInputsWithIndices struct {
		Inputs   []MoveKeyInput
		IndexMap []int
	}

	groupByInputs := func() (map[*Config]*moveKeyInputsWithIndices, error) {
		clusterPathsMap := make(map[*Config]*moveKeyInputsWithIndices)
		// 将所有的canTransfer不满足的key提前赋值
		for i, input := range input {
			if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
				return nil, err
			} else {
				config, _ := l.config.forKey(input.FromKey)
				// 都是canTransfer的, 不包含则创建
				if e, contains := clusterPathsMap[config]; !contains {
					e = &moveKeyInputsWithIndices{
						Inputs:   make([]MoveKeyInput, 0, 1),
						IndexMap: make([]int, 0, 1),
					}
					clusterPathsMap[config] = e
				}

				e := clusterPathsMap[config]

				// 将key添加到对应的集群中
				e.IndexMap = append(e.IndexMap, i)
				e.Inputs = append(e.Inputs, input)
			}
		}
		return clusterPathsMap, nil
	}

	if clusterPathsMap, err := groupByInputs(); err != nil {
		return nil, err
	} else {
		result := make([]*MoveKeysError, len(input))
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
			}(config, keysWithIndex.Inputs, keysWithIndex.IndexMap)
		}
		return result, pool.Wait(ctx)
	}
}

func (l *multiClusterLister) renameKeys(ctx context.Context, input []RenameKeyInput) ([]*RenameKeysError, error) {
	result := make([]*RenameKeysError, len(input))

	type renameKeyInputsWithIndices struct {
		Inputs   []RenameKeyInput
		IndexMap []int
	}

	groupByInputs := func() (map[*Config]*renameKeyInputsWithIndices, error) {
		clusterPathsMap := make(map[*Config]*renameKeyInputsWithIndices)
		// 将所有的canTransfer不满足的key提前赋值
		for i, input := range input {
			if _, err := l.canTransfer(input.FromKey, input.ToKey); err != nil {
				return nil, err
			} else {
				config, _ := l.config.forKey(input.FromKey)

				// 都是canTransfer的, 不包含则创建
				e, contains := clusterPathsMap[config]
				if !contains {
					clusterPathsMap[config] = &renameKeyInputsWithIndices{
						Inputs:   make([]RenameKeyInput, 0, 1),
						IndexMap: make([]int, 0, 1),
					}
					e = clusterPathsMap[config]
				}

				// 将key添加到对应的集群中
				e.IndexMap = append(e.IndexMap, i)
				e.Inputs = append(e.Inputs, input)
			}
		}
		return clusterPathsMap, nil
	}

	if clusterPathsMap, err := groupByInputs(); err != nil {
		return nil, err
	} else {
		pool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
		for config, keysWithIndex := range clusterPathsMap {
			func(config *Config, inputs []RenameKeyInput, indexMap []int) {
				pool.Go(func(ctx context.Context) error {
					if renameErrors, err := newSingleClusterLister(config).renameKeys(ctx, inputs); err != nil {
						return err
					} else {
						for i, renameError := range renameErrors {
							result[indexMap[i]] = renameError
						}
						return nil
					}
				})
			}(config, keysWithIndex.Inputs, keysWithIndex.IndexMap)
		}
		return result, pool.Wait(ctx)
	}
}

func (l *multiClusterLister) deleteKeysFromChannel(ctx context.Context, keysChan <-chan string, isForce bool, errorsChan chan<- DeleteKeysError) error {
	type configAndKeys struct {
		config *Config
		keys   []string
	}

	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	consumerPool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, keys []string) {
				consumerPool.Go(func(ctx context.Context) error {
					if deleteErrors, err := newSingleClusterLister(config).deleteKeys(ctx, keys, isForce); err != nil {
						return err
					} else {
						for _, deleteError := range deleteErrors {
							if deleteError != nil {
								errorsChan <- *deleteError
							}
						}
						return nil
					}
				})
			}(configAndKeys.config, configAndKeys.keys)
		}
	}()

	clusterPathsMap := make(map[*Config][]string)

	// 生产者pool
	producerPool := goroutine_pool.NewGoroutinePool(1)
	producerPool.Go(func(ctx context.Context) error {
		defer close(configAndKeysChan)
		for key := range keysChan {
			if config, exists := l.config.forKey(key); !exists {
				return ErrUndefinedConfig
			} else {
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
		}
		// 将剩余的key放入集群任务channel中
		for config, keys := range clusterPathsMap {
			if len(keys) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					keys:   keys,
				}
			}
		}
		return nil
	})
	if err := producerPool.Wait(ctx); err != nil {
		return err
	}
	wg.Wait()
	return consumerPool.Wait(ctx)
}

func (l *multiClusterLister) copyKeysFromChannel(ctx context.Context, input <-chan CopyKeyInput, errorsChan chan<- CopyKeysError) error {
	type configAndKeys struct {
		config *Config
		inputs []CopyKeyInput
	}

	consumerPool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	var wg sync.WaitGroup
	wg.Add(1)
	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		defer wg.Done()
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []CopyKeyInput) {
				consumerPool.Go(func(ctx context.Context) error {
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

	clusterPathsMap := make(map[*Config][]CopyKeyInput)
	producerPool := goroutine_pool.NewGoroutinePool(1)
	producerPool.Go(func(ctx context.Context) error {
		defer close(configAndKeysChan)
		for input := range input {
			if config, exists := l.config.forKey(input.FromKey); !exists {
				return ErrUndefinedConfig
			} else {
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
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		return nil
	})

	if err := producerPool.Wait(ctx); err != nil {
		return err
	}
	wg.Wait()
	return consumerPool.Wait(ctx)
}

func (l *multiClusterLister) moveKeysFromChannel(ctx context.Context, input <-chan MoveKeyInput, errorsChan chan<- MoveKeysError) error {
	type configAndKeys struct {
		config *Config
		inputs []MoveKeyInput
	}

	consumerPool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	var wg sync.WaitGroup
	wg.Add(1)
	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		defer wg.Done()
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []MoveKeyInput) {
				consumerPool.Go(func(ctx context.Context) error {
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

	clusterPathsMap := make(map[*Config][]MoveKeyInput)
	producerPool := goroutine_pool.NewGoroutinePool(1)
	producerPool.Go(func(ctx context.Context) error {
		defer close(configAndKeysChan)
		for input := range input {
			if config, exists := l.config.forKey(input.FromKey); !exists {
				return ErrUndefinedConfig
			} else {
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
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		return nil
	})

	if err := producerPool.Wait(ctx); err != nil {
		return err
	}
	wg.Wait()
	return consumerPool.Wait(ctx)
}

func (l *multiClusterLister) renameKeysFromChannel(ctx context.Context, input <-chan RenameKeyInput, errorsChan chan<- RenameKeysError) error {
	type configAndKeys struct {
		config *Config
		inputs []RenameKeyInput
	}

	consumerPool := goroutine_pool.NewGoroutinePool(l.multiClustersConcurrency)
	var wg sync.WaitGroup
	wg.Add(1)
	// 将所有的key按照集群配置分组的channel
	configAndKeysChan := make(chan configAndKeys, 100)
	go func() {
		defer wg.Done()
		// 每当有新的任务进来, 就将其放入对应的集群中
		for configAndKeys := range configAndKeysChan {
			func(config *Config, inputs []RenameKeyInput) {
				consumerPool.Go(func(ctx context.Context) error {
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

	clusterPathsMap := make(map[*Config][]RenameKeyInput)
	producerPool := goroutine_pool.NewGoroutinePool(1)
	producerPool.Go(func(ctx context.Context) error {
		defer close(configAndKeysChan)
		for input := range input {
			if config, exists := l.config.forKey(input.FromKey); !exists {
				return ErrUndefinedConfig
			} else {
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
		}
		for config, inputs := range clusterPathsMap {
			if len(inputs) > 0 {
				configAndKeysChan <- configAndKeys{
					config: config,
					inputs: inputs,
				}
			}
		}
		return nil
	})

	if err := producerPool.Wait(ctx); err != nil {
		return err
	}
	wg.Wait()
	return consumerPool.Wait(ctx)
}
