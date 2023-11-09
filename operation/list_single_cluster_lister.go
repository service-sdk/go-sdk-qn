package operation

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/auth/qbox"
	kodo2 "github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/kodo"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/goroutine_pool.v7"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/httputil.v1"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/rpc.v7"
)

type singleClusterLister struct {
	bucket         string
	rsHosts        []string
	upHosts        []string
	rsfHosts       []string
	apiServerHosts []string

	credentials      *qbox.Mac
	queryer          IQueryer
	batchSize        int
	batchConcurrency int
	recycleBin       string

	rsTimeout  time.Duration
	rsfTimeout time.Duration
	upTimeout  time.Duration
	apiTimeout time.Duration

	httpTransport http.RoundTripper
}

func newSingleClusterLister(c *Config) *singleClusterLister {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer IQueryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	lister := singleClusterLister{
		bucket:         c.Bucket,
		rsHosts:        dupStrings(c.RsHosts),
		upHosts:        dupStrings(c.UpHosts),
		rsfHosts:       dupStrings(c.RsfHosts),
		apiServerHosts: dupStrings(c.ApiServerHosts),

		rsTimeout:  buildDurationByMs(c.RsTimeoutMs, DefaultConfigRsTimeoutMs),
		rsfTimeout: buildDurationByMs(c.RsfTimeoutMs, DefaultConfigRsfTimeoutMs),
		upTimeout:  buildDurationByMs(c.UpTimeoutMs, DefaultConfigUpTimeoutMs),
		apiTimeout: buildDurationByMs(c.ApiTimeoutMs, DefaultConfigApiTimeoutMs),

		credentials:      mac,
		queryer:          queryer,
		batchConcurrency: c.BatchConcurrency,
		batchSize:        c.BatchSize,
		recycleBin:       c.RecycleBin,
		httpTransport:    getHttpClientTransport(c),
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

func (l *singleClusterLister) enableRecycleBin() bool {
	return l.recycleBin != ""
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

// 通过调用 rename API 来重命名
func (l *singleClusterLister) renameByCallingRenameAPI(ctx context.Context, fromKey, toKey string) (err error) {
	failedApiServerHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		host := l.nextApiServerHost(failedApiServerHosts)
		bucket := l.newBucket("", "", host)
		err = bucket.Rename(ctx, fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedApiServerHosts[host] = struct{}{}
		failHostName(host)
		elog.Infof("rename retry: retried=%d, host=%d, err=%s", i, host, err)
	}
	return err
}

func (l *singleClusterLister) rename(fromKey, toKey string) error {
	if l.enableRecycleBin() { // 启用回收站功能表示 RENAME API 可用
		return l.renameByCallingRenameAPI(context.Background(), fromKey, toKey)
	}
	return l.moveTo(fromKey, l.bucket, toKey)
}

func (l *singleClusterLister) moveTo(fromKey, toBucket, toKey string) (err error) {
	failedRsHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		host := l.nextRsHost(failedRsHosts)
		bucket := l.newBucket(host, "", "")
		err = bucket.MoveEx(context.Background(), fromKey, toBucket, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Infof("move retry: retried=%d, host=%d, err=%s", i, host, err)
	}
	return err
}

func (l *singleClusterLister) copy(fromKey, toKey string) (err error) {
	failedRsHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		host := l.nextRsHost(failedRsHosts)
		bucket := l.newBucket(host, "", "")
		err = bucket.Copy(context.Background(), fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Infof("copy retry: retried=%d, host=%d, err=%s", i, host, err)
	}
	return err
}

func (l *singleClusterLister) deleteByCallingDeleteAPI(ctx context.Context, key string) (err error) {
	failedRsHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		host := l.nextRsHost(failedRsHosts)
		bucket := l.newBucket(host, "", "")
		err = bucket.Delete(ctx, key)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Infof("delete retry: retried=%d, host=%s, err=%s", i, host, err)
	}
	return err
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
	_ = l.deleteByCallingDeleteAPI(ctx, keyAfterRename)
	// 重命名文件到回收站
	return l.renameByCallingRenameAPI(ctx, key, keyAfterRename)
}

func (l *singleClusterLister) delete(key string, isForce bool) error {
	if !isForce && l.enableRecycleBin() { // 启用回收站功能
		return l.putInRecycleBin(context.Background(), key, l.recycleBin)
	} else {
		return l.deleteByCallingDeleteAPI(context.Background(), key)
	}
}

func (l *singleClusterLister) listStat(ctx context.Context, paths []string) ([]*FileStat, error) {
	return l.listStatWithRetries(ctx, paths, 10, 0)
}

func (l *singleClusterLister) listStatWithRetries(ctx context.Context, paths []string, retries, retried uint) ([]*FileStat, error) {
	// 并发数计算
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

	// 分批处理
	for i := 0; i < len(paths); i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}

		// paths 是这批要删除的文件
		// index 是这批文件的起始位置
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，如果出错了最多重试两次，返回成功删除的结果
				res, _ := func() ([]kodo2.BatchStatItemRet, error) {
					var err error
					var r []kodo2.BatchStatItemRet
					for i := 0; i < 2; i++ {
						// 获取一个 rs host
						failedRsHostsLock.RLock()
						host := l.nextRsHost(failedRsHosts)
						failedRsHostsLock.RUnlock()

						// 根据拿到的rs域名构造一个 bucket
						bucket := l.newBucket(host, "", "")
						r, err = bucket.BatchStat(ctx, paths...)

						// 成功退出
						if err == nil {
							succeedHostName(host)
							return r, nil
						}
						// 出错了，获取写锁，记录错误的 rs host
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)

						elog.Infof("batchDelete retry: retried=%d, host=%s, err=%s", i, host, err)
					}
					// 重试2次都失败了，返回错误
					return nil, err
				}()

				// 批量删除的结果，过滤掉成功的，记录失败的到 stats 中
				for j, v := range res {
					if v.Code == 200 {
						stats[index+j] = &FileStat{
							Name: paths[j],
							Size: v.Data.Fsize,
							code: v.Code,
						}
					} else {
						stats[index+j] = &FileStat{
							Name: paths[j],
							Size: -1,
							code: v.Code,
						}
						elog.Warn("stat bad file:", paths[j], "with code:", v.Code)
					}
				}
				return nil
			})
		}(paths[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries <= 0 {
		return stats, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range stats {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil && e.code/100 == 5 {
			failedPathIndexMap = append(failedPathIndexMap, i)
			failedPath = append(failedPath, e.Name)
			elog.Warn("restat bad file:", e.Name, "with code:", e.code)
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(failedPath) > 0 {
		elog.Warn("restat ", len(failedPath), " bad files, retried:", retried)

		// 将失败的文件进行重试
		retriedErrors, err := l.listStatWithRetries(ctx, failedPath, retries-1, retried+1)

		// 如果重试出错了，直接返回错误
		if err != nil {
			return stats, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			stats[failedPathIndexMap[i]] = retriedError
		}
	}

	// 返回最终的结果
	return stats, nil
}

func (l *singleClusterLister) deleteKeysFromChannel(
	ctx context.Context,
	keysChan <-chan string,
	isForce bool,
	errorsChan chan<- DeleteKeysError,
) error {
	if !isForce && l.enableRecycleBin() {
		// 非强制删除且启用回收站功能
		return l.renameAsDeleteKeysFromChannel(ctx, keysChan, l.recycleBin, errorsChan)
	} else {
		return l.deleteAsDeleteKeysFromChannelWithRetries(ctx, keysChan, 10, errorsChan)
	}
}

func (l *singleClusterLister) renameAsDeleteKeysFromChannel(
	ctx context.Context,
	keysChan <-chan string,
	recycleBin string,
	errorsChan chan<- DeleteKeysError,
) error {

	batchKeys := make([]string, 0, l.batchSize)

	// doOneBatch 用于执行一次批量删除操作
	doOneBatch := func() error {
		errors, err := l.renameAsDeleteKeys(ctx, batchKeys, recycleBin)
		if err != nil {
			return err
		}
		for _, e := range errors {
			if e != nil {
				errorsChan <- *e
			}
		}
		return nil
	}

	for key := range keysChan {
		batchKeys = append(batchKeys, key)
		if len(batchKeys) >= l.batchSize {
			if err := doOneBatch(); err != nil {
				return err
			}
			batchKeys = batchKeys[:0]
		}
	}
	if len(batchKeys) > 0 {
		if err := doOneBatch(); err != nil {
			return err
		}
	}

	return nil
}

func (l *singleClusterLister) deleteAsDeleteKeysFromChannelWithRetries(
	ctx context.Context,
	keysChan <-chan string,
	retries uint,
	errorsChan chan<- DeleteKeysError,
) error {
	batchKeys := make([]string, 0, l.batchSize)

	// doOneBatch 用于执行一次批量删除操作
	doOneBatch := func() error {
		errors, err := l.deleteAsDeleteKeysWithRetries(ctx, batchKeys, retries, 0)
		if err != nil {
			return err
		}
		for _, e := range errors {
			if e != nil {
				errorsChan <- *e
			}
		}
		return nil
	}

	for key := range keysChan {
		batchKeys = append(batchKeys, key)
		if len(batchKeys) >= l.batchSize {
			if err := doOneBatch(); err != nil {
				return err
			}
			batchKeys = batchKeys[:0]
		}
	}
	if len(batchKeys) > 0 {
		if err := doOneBatch(); err != nil {
			return err
		}
	}

	return nil
}

func (l *singleClusterLister) deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error) {
	if !isForce && l.enableRecycleBin() {
		// 非强制删除且启用回收站功能
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

// 带重试逻辑的批量删除
func (l *singleClusterLister) deleteAsDeleteKeysWithRetries(ctx context.Context, paths []string, retries, retried uint) ([]*DeleteKeysError, error) {
	// 并发数计算
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

	// 分批处理
	for i := 0; i < len(paths); i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > len(paths)-i {
			size = len(paths) - i
		}

		// paths 是这批要删除的文件
		// index 是这批文件的起始位置
		func(paths []string, index int) {
			pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，如果出错了最多重试两次，返回成功删除的结果
				res, _ := func() ([]kodo2.BatchItemRet, error) {
					var err error
					var r []kodo2.BatchItemRet
					for i := 0; i < 2; i++ {
						// 获取一个 rs host
						failedRsHostsLock.RLock()
						host := l.nextRsHost(failedRsHosts)
						failedRsHostsLock.RUnlock()

						// 根据拿到的rs域名构造一个 bucket
						bucket := l.newBucket(host, "", "")
						r, err = bucket.BatchDelete(ctx, paths...)

						// 成功退出
						if err == nil {
							succeedHostName(host)
							return r, nil
						}
						// 出错了，获取写锁，记录错误的 rs host
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)

						elog.Infof("batchDelete retry: retried=%d, host=%s, err=%s", i, host, err)
					}
					// 重试2次都失败了，返回错误
					return nil, err
				}()

				// 批量删除的结果，过滤掉成功的，记录失败的到 errors 中
				for j, v := range res {
					if v.Code == 200 || v.Code == 612 {
						errors[index+j] = nil
						continue
					}
					errors[index+j] = &DeleteKeysError{
						Name:  paths[j],
						Error: v.Error,
						Code:  v.Code,
					}
					elog.Warn("delete bad file:", paths[j], "with code:", v.Code)
				}
				return nil
			})
		}(paths[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries <= 0 {
		return errors, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range errors {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil && e.Code/100 == 5 {
			failedPathIndexMap = append(failedPathIndexMap, i)
			failedPath = append(failedPath, e.Name)
			elog.Warn("redelete bad file:", e.Name, "with code:", e.Code)
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(failedPath) > 0 {
		elog.Warn("redelete ", len(failedPath), " bad files, retried:", retried)

		// 将失败的文件进行重试
		retriedErrors, err := l.deleteAsDeleteKeysWithRetries(ctx, failedPath, retries-1, retried+1)

		// 如果重试出错了，直接返回错误
		if err != nil {
			return errors, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			errors[failedPathIndexMap[i]] = retriedError
		}
	}

	// 返回最终的结果
	return errors, nil
}

// 列举指定前缀的文件到channel中
func (l *singleClusterLister) listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error {
	marker := ""
	for {
		res, markerOut, err := func() (res []kodo2.ListItem, markerOut string, err error) {
			failedHosts := make(map[string]struct{})
			for i := 0; i < 2; i++ {
				rsfHost := l.nextRsfHost(failedHosts)
				bucket := l.newBucket("", rsfHost, "")
				res, _, markerOut, err = bucket.List(ctx, prefix, "", marker, 1000)
				if err != nil && err != io.EOF {
					failedHosts[rsfHost] = struct{}{}
					failHostName(rsfHost)
					elog.Infof("ListPrefix retry: retried=%d, host=%s, err=%s", i, rsfHost, err)
					continue
				}
				succeedHostName(rsfHost)
				return res, markerOut, nil
			}
			return nil, "", err
		}()

		if err != nil {
			return err
		}

		for _, item := range res {
			ch <- item.Key
		}

		if markerOut == "" {
			break
		}
		marker = markerOut
	}
	return nil
}

// 列举指定前缀的所有文件
func (l *singleClusterLister) listPrefix(ctx context.Context, prefix string) (files []string, err error) {
	ch := make(chan string, 1000)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for c := range ch {
			files = append(files, c)
		}
	}()

	err = l.listPrefixToChannel(ctx, prefix, ch)
	close(ch)
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return files, nil
}

func (l *singleClusterLister) newBucket(host, rsfHost, apiHost string) kodo2.Bucket {
	cfg := kodo2.Config{
		AccessKey: l.credentials.GetAccessKey(),
		SecretKey: l.credentials.GetSecretKey(),

		RSHost:  host,
		RSFHost: rsfHost,
		APIHost: apiHost,
		UpHosts: l.upHosts,

		RsTimeout:  l.rsTimeout,
		RsfTimeout: l.rsfTimeout,
		ApiTimeout: l.apiTimeout,
		UpTimeout:  l.upTimeout,
	}
	client := kodo2.NewClient(&cfg, l.httpTransport)
	return *kodo2.NewBucket(client, l.bucket)
}

func (l *singleClusterLister) copyKeysWithRetries(ctx context.Context, input []CopyKeyInput, retries, retried uint) ([]*CopyKeysError, error) {
	num := len(input)
	// 并发数计算
	concurrency := (num + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		errors              = make([]*CopyKeysError, num)
		failedInput         []CopyKeyInput
		failedInputIndexMap []int
		failedRsHosts       = make(map[string]struct{})
		failedRsHostsLock   sync.RWMutex
		pool                = goroutine_pool.NewGoroutinePool(concurrency)
	)

	// 分批处理
	for i := 0; i < num; i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > num-i {
			size = num - i
		}

		// paths 是这批要删除的文件
		// index 是这批文件的起始位置
		func(paths []CopyKeyInput, index int) {
			pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，如果出错了最多重试两次，返回成功删除的结果
				res, _ := func() ([]kodo2.BatchItemRet, error) {
					var err error
					var r []kodo2.BatchItemRet
					for i := 0; i < 2; i++ {
						// 获取一个 rs host
						failedRsHostsLock.RLock()
						host := l.nextRsHost(failedRsHosts)
						failedRsHostsLock.RUnlock()

						// 根据拿到的rs域名构造一个 bucket
						bucket := l.newBucket(host, "", "")

						var pairs []kodo2.KeyPair
						for _, v := range paths {
							pairs = append(pairs, kodo2.KeyPair{
								SrcKey:  v.FromKey,
								DestKey: v.ToKey,
							})
						}
						r, err = bucket.BatchCopy(ctx, pairs...)

						// 成功退出
						if err == nil {
							succeedHostName(host)
							return r, nil
						}
						// 出错了，获取写锁，记录错误的 rs host
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)

						elog.Infof("batchDelete retry: retried=%d, host=%s, err=%s", i, host, err)
					}
					// 重试2次都失败了，返回错误
					return nil, err
				}()

				// 批量删除的结果，过滤掉成功的，记录失败的到 errors 中
				for j, v := range res {
					if v.Code == 200 || v.Code == 612 {
						errors[index+j] = nil
						continue
					}
					errors[index+j] = &CopyKeysError{
						Error:   v.Error,
						Code:    v.Code,
						FromKey: paths[j].FromKey,
						ToKey:   paths[j].ToKey,
					}
					elog.Warn("copy bad file:", paths[j], "with code:", v.Code)
				}
				return nil
			})
		}(input[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries <= 0 {
		return errors, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range errors {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil && e.Code/100 == 5 {
			failedInputIndexMap = append(failedInputIndexMap, i)
			failedInput = append(failedInput, CopyKeyInput{
				FromKey: e.FromKey,
				ToKey:   e.ToKey,
			})
			elog.Warn("recopy bad file:", e.FromKey, " -> ", e.ToKey, " with code:", e.Code)
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(failedInput) > 0 {
		elog.Warn("redelete ", len(failedInput), " bad files, retried:", retried)

		// 将失败的文件进行重试
		retriedErrors, err := l.copyKeysWithRetries(ctx, failedInput, retries-1, retried+1)

		// 如果重试出错了，直接返回错误
		if err != nil {
			return errors, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			errors[failedInputIndexMap[i]] = retriedError
		}
	}

	// 返回最终的结果
	return errors, nil
}

func (l *singleClusterLister) copyKeys(ctx context.Context, input []CopyKeyInput) ([]*CopyKeysError, error) {
	return l.copyKeysWithRetries(ctx, input, 10, 0)
}

// 从channel中读取数据并批量复制
func (l *singleClusterLister) copyKeysFromChannel(ctx context.Context, input <-chan CopyKeyInput, errorsChan chan<- CopyKeysError) error {
	var batch []CopyKeyInput
	for in := range input {
		batch = append(batch, in)
		if len(batch) >= l.batchSize {
			res, err := l.copyKeys(ctx, batch)
			if err != nil {
				return err
			}
			for _, r := range res {
				if r != nil {
					errorsChan <- *r
				}
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		_, err := l.copyKeys(ctx, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *singleClusterLister) moveKeys(ctx context.Context, input []MoveKeyInput) ([]*MoveKeysError, error) {
	return l.moveKeysWithRetries(ctx, input, 10, 0)
}
func (l *singleClusterLister) moveKeysWithRetries(ctx context.Context, input []MoveKeyInput, retries, retried uint) ([]*MoveKeysError, error) {
	num := len(input)
	// 并发数计算
	concurrency := (num + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		errors              = make([]*MoveKeysError, num)
		failedInput         []MoveKeyInput
		failedInputIndexMap []int
		failedRsHosts       = make(map[string]struct{})
		failedRsHostsLock   sync.RWMutex
		pool                = goroutine_pool.NewGoroutinePool(concurrency)
	)

	// 分批处理
	for i := 0; i < num; i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > num-i {
			size = num - i
		}

		func(paths []MoveKeyInput, index int) {
			pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，如果出错了最多重试两次，返回成功删除的结果
				res, _ := func() ([]kodo2.BatchItemRet, error) {
					var err error
					var r []kodo2.BatchItemRet
					for i := 0; i < 2; i++ {
						// 获取一个 rs host
						failedRsHostsLock.RLock()
						host := l.nextRsHost(failedRsHosts)
						failedRsHostsLock.RUnlock()

						// 根据拿到的rs域名构造一个 bucket
						bucket := l.newBucket(host, "", "")

						var pairs []kodo2.KeyPairEx
						for _, v := range paths {
							pairs = append(pairs, kodo2.KeyPairEx{
								SrcKey:     v.FromKey,
								DestKey:    v.ToKey,
								DestBucket: v.ToBucket,
							})
						}
						r, err = bucket.BatchMove(ctx, pairs...)

						// 成功退出
						if err == nil {
							succeedHostName(host)
							return r, nil
						}
						// 出错了，获取写锁，记录错误的 rs host
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)

						elog.Infof("batchMove retry: retried=%d, host=%s, err=%s", i, host, err)
					}
					// 重试2次都失败了，返回错误
					return nil, err
				}()

				for j, v := range res {
					if v.Code == 200 {
						errors[index+j] = nil
						continue
					}
					errors[index+j] = &MoveKeysError{
						FromToKeyError: FromToKeyError{
							Error:   v.Error,
							Code:    v.Code,
							FromKey: paths[j].FromKey,
							ToKey:   paths[j].ToKey,
						},
						ToBucket: paths[j].ToBucket,
					}
					elog.Warn("move bad file:", paths[j], "with code:", v.Code)
				}
				return nil
			})
		}(input[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries <= 0 {
		return errors, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range errors {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil && e.Code/100 == 5 {
			failedInputIndexMap = append(failedInputIndexMap, i)
			failedInput = append(failedInput, MoveKeyInput{
				FromToKey: FromToKey{
					FromKey: e.FromKey,
					ToKey:   e.ToKey,
				},
				ToBucket: e.ToBucket,
			})
			elog.Warn("re move bad file:", e.FromKey, " -> ", e.ToKey, " toBucket: ", e.ToBucket, " with code:", e.Code)
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(failedInput) > 0 {
		elog.Warn("re move ", len(failedInput), " bad files, retried:", retried)

		// 将失败的文件进行重试
		retriedErrors, err := l.moveKeysWithRetries(ctx, failedInput, retries-1, retried+1)

		// 如果重试出错了，直接返回错误
		if err != nil {
			return errors, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			errors[failedInputIndexMap[i]] = retriedError
		}
	}

	// 返回最终的结果
	return errors, nil
}

func (l *singleClusterLister) moveKeysFromChannel(ctx context.Context, input <-chan MoveKeyInput, errorsChan chan<- MoveKeysError) error {
	var batch []MoveKeyInput
	for in := range input {
		batch = append(batch, in)
		if len(batch) >= l.batchSize {
			res, err := l.moveKeys(ctx, batch)
			if err != nil {
				return err
			}
			for _, r := range res {
				if r != nil {
					errorsChan <- *r
				}
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		_, err := l.moveKeys(ctx, batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *singleClusterLister) renameKeysWithRetries(ctx context.Context, input []RenameKeyInput, retries, retried uint) ([]*RenameKeysError, error) {
	num := len(input)
	// 并发数计算
	concurrency := (num + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	var (
		errors              = make([]*RenameKeysError, num)
		failedInput         []RenameKeyInput
		failedInputIndexMap []int
		failedRsHosts       = make(map[string]struct{})
		failedRsHostsLock   sync.RWMutex
		pool                = goroutine_pool.NewGoroutinePool(concurrency)
	)

	// 分批处理
	for i := 0; i < num; i += l.batchSize {
		// 计算本次批量处理的数量
		size := l.batchSize
		if size > num-i {
			size = num - i
		}

		func(paths []RenameKeyInput, index int) {
			pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，如果出错了最多重试两次，返回成功删除的结果
				res, _ := func() ([]kodo2.BatchItemRet, error) {
					var err error
					var r []kodo2.BatchItemRet
					for i := 0; i < 2; i++ {
						// 获取一个 rs host
						failedRsHostsLock.RLock()
						host := l.nextRsHost(failedRsHosts)
						failedRsHostsLock.RUnlock()

						// 根据拿到的rs域名构造一个 bucket
						bucket := l.newBucket(host, "", "")

						var pairs []kodo2.KeyPair
						for _, v := range paths {
							pairs = append(pairs, kodo2.KeyPair{
								SrcKey:  v.FromKey,
								DestKey: v.ToKey,
							})
						}
						r, err = bucket.BatchRename(ctx, pairs...)

						// 成功退出
						if err == nil {
							succeedHostName(host)
							return r, nil
						}
						// 出错了，获取写锁，记录错误的 rs host
						failedRsHostsLock.Lock()
						failedRsHosts[host] = struct{}{}
						failedRsHostsLock.Unlock()
						failHostName(host)

						elog.Infof("batchRename retry: retried=%d, host=%s, err=%s", i, host, err)
					}
					// 重试2次都失败了，返回错误
					return nil, err
				}()

				for j, v := range res {
					if v.Code == 200 {
						errors[index+j] = nil
						continue
					}
					errors[index+j] = &RenameKeysError{
						Error:   v.Error,
						Code:    v.Code,
						FromKey: paths[j].FromKey,
						ToKey:   paths[j].ToKey,
					}
					elog.Warn("rename bad file:", paths[j], "with code:", v.Code)
				}
				return nil
			})
		}(input[i:i+size], i)
	}

	// 等待所有的批量删除任务完成，如果出错了，直接结束返回错误
	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	if retries <= 0 {
		return errors, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range errors {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil && e.Code/100 == 5 {
			failedInputIndexMap = append(failedInputIndexMap, i)
			failedInput = append(failedInput, RenameKeyInput{
				FromKey: e.FromKey,
				ToKey:   e.ToKey,
			})
			elog.Warn("rename bad file:", e.FromKey, " -> ", e.ToKey, " with code:", e.Code)
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(failedInput) > 0 {
		elog.Warn("rename ", len(failedInput), " bad files, retried:", retried)

		// 将失败的文件进行重试
		retriedErrors, err := l.renameKeysWithRetries(ctx, failedInput, retries-1, retried+1)

		// 如果重试出错了，直接返回错误
		if err != nil {
			return errors, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			errors[failedInputIndexMap[i]] = retriedError
		}
	}

	// 返回最终的结果
	return errors, nil
}

func (l *singleClusterLister) renameKeys(ctx context.Context, input []RenameKeyInput) ([]*RenameKeysError, error) {
	// 未开启回收站，就直接调用move接口替代
	if !l.enableRecycleBin() {
		var moveKeys []MoveKeyInput
		for _, e := range input {
			moveKeys = append(moveKeys, MoveKeyInput{
				FromToKey: FromToKey{
					FromKey: e.FromKey,
					ToKey:   e.ToKey,
				},
				ToBucket: l.bucket,
			})
		}
		moveKeysErrors, err := l.moveKeys(ctx, moveKeys)
		if err != nil {
			return nil, err
		}
		var renameKeysErrors []*RenameKeysError
		for _, e := range moveKeysErrors {
			if e == nil {
				continue
			}
			renameKeysErrors = append(renameKeysErrors, &RenameKeysError{
				Error:   e.Error,
				Code:    e.Code,
				FromKey: e.FromKey,
				ToKey:   e.ToKey,
			})
		}
		return renameKeysErrors, nil
	}
	// 开启回收站，就调用rename接口
	return l.renameKeysWithRetries(ctx, input, 10, 0)
}

func (l *singleClusterLister) renameKeysFromChannel(ctx context.Context, input <-chan RenameKeyInput, errorsChan chan<- RenameKeysError) error {
	var batch []RenameKeyInput
	for in := range input {
		batch = append(batch, in)
		if len(batch) >= l.batchSize {
			res, err := l.renameKeys(ctx, batch)
			if err != nil {
				return err
			}
			for _, r := range res {
				if r != nil {
					errorsChan <- *r
				}
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		_, err := l.renameKeys(ctx, batch)
		if err != nil {
			return err
		}
	}
	return nil
}
