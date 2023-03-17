package operation

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
	"github.com/qiniupd/qiniu-go-sdk/x/httputil.v1"
	"github.com/qiniupd/qiniu-go-sdk/x/rpc.v7"
)

var (
	ErrUndefinedConfig                        = errors.New("undefined config")
	ErrCannotTransferBetweenDifferentClusters = errors.New("cannot transfer between different clusters")
)

// 列举器
type Lister struct {
	config                   Configurable
	singleClusterLister      *singleClusterLister
	multiClustersConcurrency int
}

// 根据配置创建列举器
func NewLister(c *Config) *Lister {
	return &Lister{config: c, singleClusterLister: newSingleClusterLister(c)}
}

// 根据环境变量创建列举器
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

// 文件元信息
type FileStat struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	code int    `json:"-"`
}

// 重命名对象
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

// 移动对象到指定存储空间的指定对象中
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

// 复制对象到当前存储空间的指定对象中
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

// 删除指定对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) Delete(key string) error {
	return l.delete(key, false)
}

// 强制删除指定对象，无论是否配置回收站，该 API 都会直接删除文件
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

// 获取指定对象列表的元信息
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

	pool := newGoroutinePool(l.multiClustersConcurrency)
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

// 根据前缀列举存储空间
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

	pool := newGoroutinePool(l.multiClustersConcurrency)
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

// 删除多个对象，如果配置了回收站，该 API 将会将文件移动到回收站中，而不做实际的删除
func (l *Lister) DeleteKeys(keys []string) ([]*DeleteKeysError, error) {
	return l.deleteKeys(context.Background(), keys, false)
}

// 强制删除多个对象，无论是否配置回收站，该 API 都会直接删除文件
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

	pool := newGoroutinePool(l.multiClustersConcurrency)
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

func newSingleClusterLister(c *Config) *singleClusterLister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	lister := singleClusterLister{
		bucket:           c.Bucket,
		rsHosts:          dupStrings(c.RsHosts),
		upHosts:          dupStrings(c.UpHosts),
		rsfHosts:         dupStrings(c.RsfHosts),
		apiServerHosts:   dupStrings(c.ApiServerHosts),
		credentials:      mac,
		queryer:          queryer,
		batchConcurrency: c.BatchConcurrency,
		batchSize:        c.BatchSize,
		recycleBin:       c.RecycleBin,
	}
	if lister.batchConcurrency <= 0 {
		lister.batchConcurrency = 20
	}
	if lister.batchSize <= 0 {
		lister.batchSize = 100
	}
	shuffleHosts(lister.rsHosts)
	shuffleHosts(lister.rsfHosts)
	shuffleHosts(lister.upHosts)
	shuffleHosts(lister.apiServerHosts)
	return &lister
}

type singleClusterLister struct {
	bucket           string
	rsHosts          []string
	upHosts          []string
	rsfHosts         []string
	apiServerHosts   []string
	credentials      *qbox.Mac
	queryer          *Queryer
	batchSize        int
	batchConcurrency int
	recycleBin       string
}

var curRsHostIndex uint32 = 0

func (l *singleClusterLister) nextRsHost(failedHosts map[string]struct{}) string {
	rsHosts := l.rsHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsHosts = hosts
		}
	}
	switch len(rsHosts) {
	case 0:
		panic("No Rs hosts is configured")
	case 1:
		return rsHosts[0]
	default:
		var rsHost string
		for i := 0; i <= len(rsHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsHostIndex, 1) - 1)
			rsHost = rsHosts[index%len(rsHosts)]
			if _, isFailedBefore := failedHosts[rsHost]; !isFailedBefore && isHostNameValid(rsHost) {
				break
			}
		}
		return rsHost
	}
}

var curRsfHostIndex uint32 = 0

func (l *singleClusterLister) nextRsfHost(failedHosts map[string]struct{}) string {
	rsfHosts := l.rsfHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryRsfHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			rsfHosts = hosts
		}
	}
	switch len(rsfHosts) {
	case 0:
		panic("No Rsf hosts is configured")
	case 1:
		return rsfHosts[0]
	default:
		var rsfHost string
		for i := 0; i <= len(rsfHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curRsfHostIndex, 1) - 1)
			rsfHost = rsfHosts[index%len(rsfHosts)]
			if _, isFailedBefore := failedHosts[rsfHost]; !isFailedBefore && isHostNameValid(rsfHost) {
				break
			}
		}
		return rsfHost
	}
}

func (l *singleClusterLister) nextApiServerHost(failedHosts map[string]struct{}) string {
	apiServerHosts := l.apiServerHosts
	if l.queryer != nil {
		if hosts := l.queryer.QueryApiServerHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			apiServerHosts = hosts
		}
	}
	switch len(apiServerHosts) {
	case 0:
		panic("No ApiServer hosts is configured")
	case 1:
		return apiServerHosts[0]
	default:
		var apiServerHost string
		for i := 0; i <= len(apiServerHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curApiServerHostIndex, 1) - 1)
			apiServerHost = apiServerHosts[index%len(apiServerHosts)]
			if _, isFailedBefore := failedHosts[apiServerHost]; !isFailedBefore && isHostNameValid(apiServerHost) {
				break
			}
		}
		return apiServerHost
	}
}

func (l *singleClusterLister) renameByCallingMoveAPI(fromKey, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "", "")
	err := bucket.Move(nil, fromKey, toKey)
	if !isServerError(err) {
		succeedHostName(host)
		return err
	} else {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("move retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "", "")
		err = bucket.Move(nil, fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		} else {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("move retry 1", host, err)
			return err
		}
	}
}

func (l *singleClusterLister) renameByCallingRenameAPI(ctx context.Context, fromKey, toKey string) error {
	failedApiServerHosts := make(map[string]struct{})
	host := l.nextApiServerHost(failedApiServerHosts)
	bucket := l.newBucket("", "", host)
	err := bucket.Rename(ctx, fromKey, toKey)
	if !isServerError(err) {
		succeedHostName(host)
		return err
	} else {
		failedApiServerHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("rename retry 0", host, err)
		host = l.nextApiServerHost(failedApiServerHosts)
		bucket = l.newBucket("", "", host)
		err = bucket.Rename(ctx, fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		} else {
			failedApiServerHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("rename retry 1", host, err)
			return err
		}
	}
}

func (l *singleClusterLister) rename(fromKey, toKey string) error {
	if l.recycleBin != "" { // 启用回收站功能表示 RENAME API 可用
		return l.renameByCallingRenameAPI(context.Background(), fromKey, toKey)
	} else {
		return l.renameByCallingMoveAPI(fromKey, toKey)
	}
}

func (l *singleClusterLister) moveTo(fromKey, toBucket, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "", "")
	err := bucket.MoveEx(nil, fromKey, toBucket, toKey)
	if !isServerError(err) {
		succeedHostName(host)
		return err
	} else {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("move retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "", "")
		err = bucket.MoveEx(nil, fromKey, toBucket, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		} else {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("move retry 1", host, err)
			return err
		}
	}
}

func (l *singleClusterLister) copy(fromKey, toKey string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "", "")
	err := bucket.Copy(nil, fromKey, toKey)
	if !isServerError(err) {
		succeedHostName(host)
		return err
	} else {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("copy retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "", "")
		err = bucket.Copy(nil, fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		} else {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("copy retry 1", host, err)
			return err
		}
	}
}

func (l *singleClusterLister) deleteByCallingDeleteAPI(ctx context.Context, key string) error {
	failedRsHosts := make(map[string]struct{})
	host := l.nextRsHost(failedRsHosts)
	bucket := l.newBucket(host, "", "")
	err := bucket.Delete(ctx, key)
	if !isServerError(err) {
		succeedHostName(host)
		return err
	} else {
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("delete retry 0", host, err)
		host = l.nextRsHost(failedRsHosts)
		bucket = l.newBucket(host, "", "")
		err = bucket.Delete(ctx, key)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		} else {
			failedRsHosts[host] = struct{}{}
			failHostName(host)
			elog.Info("delete retry 1", host, err)
			return err
		}
	}
}

func (l *singleClusterLister) putInRecycleBin(ctx context.Context, key string, recycleBin string) error {
	keyAfterRename := recycleBin
	if !strings.HasSuffix(keyAfterRename, "/") {
		keyAfterRename += "/"
	}
	keyAfterRename += key
	l.deleteByCallingDeleteAPI(ctx, keyAfterRename)
	return l.renameByCallingRenameAPI(ctx, key, keyAfterRename)
}

func (l *singleClusterLister) delete(key string, isForce bool) error {
	if !isForce && l.recycleBin != "" { // 启用回收站功能
		return l.putInRecycleBin(context.Background(), key, l.recycleBin)
	} else {
		return l.deleteByCallingDeleteAPI(context.Background(), key)
	}
}

func (l *singleClusterLister) listStat(ctx context.Context, paths []string) ([]*FileStat, error) {
	return l.listStatWithRetries(ctx, paths, 10, 0)
}

func (l *singleClusterLister) listStatWithRetries(ctx context.Context, paths []string, retries, retried uint) ([]*FileStat, error) {
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		stats              = make([]*FileStat, len(paths))
		failedPath         []string
		failedPathIndexMap []int
		failedRsHosts      = make(map[string]struct{})
		failedRsHostsLock  sync.RWMutex
		pool               = newGoroutinePool(concurrency)
	)

	for i := 0; i < len(paths); i += l.batchSize {
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				failedRsHostsLock.RLock()
				host := l.nextRsHost(failedRsHosts)
				failedRsHostsLock.RUnlock()
				bucket := l.newBucket(host, "", "")
				r, err := bucket.BatchStat(ctx, paths...)
				if err != nil {
					failedRsHostsLock.Lock()
					failedRsHosts[host] = struct{}{}
					failedRsHostsLock.Unlock()
					failHostName(host)
					elog.Info("batchStat retry 0", host, err)
					failedRsHostsLock.RLock()
					host = l.nextRsHost(failedRsHosts)
					failedRsHostsLock.RUnlock()
					bucket = l.newBucket(host, "", "")
					r, err = bucket.BatchStat(ctx, paths...)
					if err != nil {
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)
						elog.Info("batchStat retry 1", host, err)
						return err
					} else {
						succeedHostName(host)
					}
				} else {
					succeedHostName(host)
				}
				for j, v := range r {
					if v.Code != 200 {
						stats[index+j] = &FileStat{Name: paths[j], Size: -1, code: v.Code}
						elog.Warn("stat bad file:", paths[j], "with code:", v.Code)
					} else {
						stats[index+j] = &FileStat{Name: paths[j], Size: v.Data.Fsize, code: v.Code}
					}
				}
				return nil
			})
		}(paths[i:i+size], i)
	}
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries > 0 {
		for i, stat := range stats {
			if stat.code/100 == 5 {
				failedPathIndexMap = append(failedPathIndexMap, i)
				failedPath = append(failedPath, stat.Name)
				elog.Warn("restat bad file:", stat.Name, "with code:", stat.code)
			}
		}
		if len(failedPath) > 0 {
			elog.Warn("restat ", len(failedPath), " bad files, retried:", retried)
			retriedStats, err := l.listStatWithRetries(ctx, failedPath, retries-1, retried+1)
			if err != nil {
				return stats, err
			}
			for i, retriedStat := range retriedStats {
				stats[failedPathIndexMap[i]] = retriedStat
			}
		}
	}

	return stats, nil
}

func (l *singleClusterLister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	if !isForce && l.recycleBin != "" {
		return l.renameAsDeleteKeys(ctx, keys, l.recycleBin)
	} else {
		return l.deleteAsDeleteKeysWithRetries(ctx, keys, 10, 0)
	}
}

func (l *singleClusterLister) renameAsDeleteKeys(ctx context.Context, paths []string, recycleBin string) ([]*DeleteKeysError, error) {
	var (
		errors     = make([]*DeleteKeysError, len(paths))
		errorsLock sync.Mutex
		pool       = newGoroutinePool(l.batchConcurrency)
	)
	for i := 0; i < len(paths); i += 1 {
		func(index int) {
			pool.Go(func(ctx context.Context) error {
				err := l.putInRecycleBin(ctx, paths[index], recycleBin)
				if err != nil {
					deleteKeysErr := DeleteKeysError{Name: paths[index]}
					if errorInfo, ok := err.(*rpc.ErrorInfo); ok {
						deleteKeysErr.Code = errorInfo.HttpCode()
						deleteKeysErr.Error = errorInfo.Error()
					} else {
						deleteKeysErr.Code = httputil.DetectCode(err)
						deleteKeysErr.Error = err.Error()
					}
					errorsLock.Lock()
					errors[index] = &deleteKeysErr
					errorsLock.Unlock()
				}
				return nil
			})
		}(i)
	}
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}
	return errors, nil
}

func (l *singleClusterLister) deleteAsDeleteKeysWithRetries(ctx context.Context, paths []string, retries, retried uint) ([]*DeleteKeysError, error) {
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		errors             = make([]*DeleteKeysError, len(paths))
		failedPath         []string
		failedPathIndexMap []int
		failedRsHosts      = make(map[string]struct{})
		failedRsHostsLock  sync.RWMutex
		pool               = newGoroutinePool(concurrency)
	)

	for i := 0; i < len(paths); i += l.batchSize {
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				failedRsHostsLock.RLock()
				host := l.nextRsHost(failedRsHosts)
				failedRsHostsLock.RUnlock()
				bucket := l.newBucket(host, "", "")
				r, err := bucket.BatchDelete(ctx, paths...)
				if err != nil {
					failedRsHostsLock.Lock()
					failedRsHosts[host] = struct{}{}
					failedRsHostsLock.Unlock()
					failHostName(host)
					elog.Info("batchDelete retry 0", host, err)
					failedRsHostsLock.RLock()
					host = l.nextRsHost(failedRsHosts)
					failedRsHostsLock.RUnlock()
					bucket = l.newBucket(host, "", "")
					r, err = bucket.BatchDelete(ctx, paths...)
					if err != nil {
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)
						elog.Info("batchDelete retry 1", host, err)
						return err
					} else {
						succeedHostName(host)
					}
				} else {
					succeedHostName(host)
				}
				for j, v := range r {
					if v.Code != 200 {
						errors[index+j] = &DeleteKeysError{Name: paths[j], Error: v.Error, Code: v.Code}
						elog.Warn("delete bad file:", paths[j], "with code:", v.Code)
					} else {
						errors[index+j] = nil
					}
				}
				return nil
			})
		}(paths[i:i+size], i)
	}
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries > 0 {
		for i, e := range errors {
			if e != nil && e.Code/100 == 5 {
				failedPathIndexMap = append(failedPathIndexMap, i)
				failedPath = append(failedPath, e.Name)
				elog.Warn("redelete bad file:", e.Name, "with code:", e.Code)
			}
		}
		if len(failedPath) > 0 {
			elog.Warn("redelete ", len(failedPath), " bad files, retried:", retried)
			retriedErrors, err := l.deleteAsDeleteKeysWithRetries(ctx, failedPath, retries-1, retried+1)
			if err != nil {
				return errors, err
			}
			for i, retriedError := range retriedErrors {
				errors[failedPathIndexMap[i]] = retriedError
			}
		}
	}

	return errors, nil
}

type DeleteKeysError struct {
	Error string
	Code  int
	Name  string
}

func (l *singleClusterLister) listPrefix(ctx context.Context, prefix string) ([]string, error) {
	failedHosts := make(map[string]struct{})
	rsfHost := l.nextRsfHost(failedHosts)
	bucket := l.newBucket("", rsfHost, "")
	var files []string
	marker := ""
	for {
		r, _, out, err := bucket.List(ctx, prefix, "", marker, 1000)
		if err != nil && err != io.EOF {
			failedHosts[rsfHost] = struct{}{}
			failHostName(rsfHost)
			elog.Info("ListPrefix retry 0", rsfHost, err)
			rsfHost = l.nextRsfHost(failedHosts)
			bucket = l.newBucket("", rsfHost, "")
			r, _, out, err = bucket.List(ctx, prefix, "", marker, 1000)
			if err != nil && err != io.EOF {
				failedHosts[rsfHost] = struct{}{}
				failHostName(rsfHost)
				elog.Info("ListPrefix retry 1", rsfHost, err)
				return nil, err
			} else {
				succeedHostName(rsfHost)
			}
		} else {
			succeedHostName(rsfHost)
		}
		elog.Info("list len", marker, len(r))
		for _, v := range r {
			files = append(files, v.Key)
		}

		if out == "" {
			break
		}
		marker = out
	}
	return files, nil
}

func (l *singleClusterLister) newBucket(host, rsfHost, apiHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.AccessKey,
		SecretKey: string(l.credentials.SecretKey),
		RSHost:    host,
		RSFHost:   rsfHost,
		APIHost:   apiHost,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewWithoutZone(&cfg)
	return client.Bucket(l.bucket)
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
