package operation

import (
	"context"
	"github.com/service-sdk/go-sdk-qn/api.v7/auth/qbox"
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"github.com/service-sdk/go-sdk-qn/x/httputil.v1"
	"github.com/service-sdk/go-sdk-qn/x/rpc.v7"
	"io"
	"strings"
	"sync"
	"sync/atomic"
)

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
	// 确保回收站路径是以 / 结尾的目录文件
	keyAfterRename := recycleBin
	if !strings.HasSuffix(keyAfterRename, "/") {
		keyAfterRename += "/"
	}
	// 将文件名拼接到回收站路径后面
	keyAfterRename += key
	// 删除已经存在的回收站文件
	l.deleteByCallingDeleteAPI(ctx, keyAfterRename)
	// 重命名文件到回收站
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
		pool               = goroutine_pool.NewGoroutinePool(concurrency)
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
		pool       = goroutine_pool.NewGoroutinePool(l.batchConcurrency)
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
		pool               = goroutine_pool.NewGoroutinePool(concurrency)
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
		AccessKey: l.credentials.GetAccessKey(),
		SecretKey: string(l.credentials.GetSecretKey()),
		RSHost:    host,
		RSFHost:   rsfHost,
		APIHost:   apiHost,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewClient(&cfg)
	return *kodo.NewBucket(client, l.bucket)
}
