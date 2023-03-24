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
		elog.Info("rename retry", i, host, err)
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
		err = bucket.MoveEx(nil, fromKey, toBucket, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("move retry", i, host, err)
	}
	return err
}

func (l *singleClusterLister) copy(fromKey, toKey string) (err error) {
	failedRsHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		host := l.nextRsHost(failedRsHosts)
		bucket := l.newBucket(host, "", "")
		err = bucket.Copy(nil, fromKey, toKey)
		if !isServerError(err) {
			succeedHostName(host)
			return err
		}
		failedRsHosts[host] = struct{}{}
		failHostName(host)
		elog.Info("copy retry", i, host, err)
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
		elog.Info("delete retry", i, host, err)
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
	return l.listStatWithRetries(ctx, paths, 10)
}

func (l *singleClusterLister) listStatWithRetries(ctx context.Context, paths []string, retries uint) ([]*FileStat, error) {
	return newBatchKeysWithRetries(
		/*context*/ ctx, l, paths, retries,
		"stat",
		/*action*/ func(bucket kodo.Bucket, paths []string) ([]kodo.BatchStatItemRet, error) {
			return bucket.BatchStat(ctx, paths...)
		},
		2, l.batchSize, l.batchConcurrency,
		/*resultBuilder*/ func(path string, r kodo.BatchStatItemRet) *FileStat {
			if r.Code != 200 {
				return &FileStat{Name: path, code: r.Code, Size: -1}
			}
			return &FileStat{Name: path, Size: r.Data.Fsize, code: r.Code}
		},
		/*resultParser*/ func(err *FileStat) (code int, path string) {
			return err.code, err.Name
		},
	).doAndRetryAction()
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
		errors, err := l.deleteAsDeleteKeysWithRetries(ctx, batchKeys, retries)
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
		return l.deleteAsDeleteKeysWithRetries(ctx, keys, 10)
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
func (l *singleClusterLister) deleteAsDeleteKeysWithRetries(ctx context.Context, paths []string, retries uint) ([]*DeleteKeysError, error) {
	return newBatchKeysWithRetries(
		/*context*/ ctx, l, paths, retries,
		"delete",
		/*action*/ func(bucket kodo.Bucket, paths []string) ([]kodo.BatchItemRet, error) {
			return bucket.BatchDelete(ctx, paths...)
		},
		2, l.batchSize, l.batchConcurrency,
		func(path string, r kodo.BatchItemRet) *DeleteKeysError {
			if code := r.Code; code != 200 {
				return &DeleteKeysError{Code: r.Code, Name: path, Error: r.Error}
			}
			return nil
		},
		func(err *DeleteKeysError) (code int, path string) {
			return err.Code, err.Name
		},
	).doAndRetryAction()
}

// 列举指定前缀的文件到channel中
func (l *singleClusterLister) listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error {
	marker := ""
	for {
		res, markerOut, err := func() (res []kodo.ListItem, markerOut string, err error) {
			failedHosts := make(map[string]struct{})
			for i := 0; i < 2; i++ {
				rsfHost := l.nextRsfHost(failedHosts)
				bucket := l.newBucket("", rsfHost, "")
				res, _, markerOut, err = bucket.List(ctx, prefix, "", marker, 1000)
				if err != nil && err != io.EOF {
					failedHosts[rsfHost] = struct{}{}
					failHostName(rsfHost)
					elog.Info("ListPrefix retry ", i, rsfHost, err)
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
		// 从channel读数据
		for c := range ch {
			files = append(files, c)
		}
		wg.Done()
	}()

	err = l.listPrefixToChannel(ctx, prefix, ch)
	close(ch)
	wg.Wait()

	if err != nil {
		return nil, err
	}
	return files, nil
}

func (l *singleClusterLister) newBucket(host, rsfHost, apiHost string) kodo.Bucket {
	cfg := kodo.Config{
		AccessKey: l.credentials.GetAccessKey(),
		SecretKey: l.credentials.GetSecretKey(),
		RSHost:    host,
		RSFHost:   rsfHost,
		APIHost:   apiHost,
		UpHosts:   l.upHosts,
	}
	client := kodo.NewClient(&cfg)
	return *kodo.NewBucket(client, l.bucket)
}

func (l *singleClusterLister) copyKeys(ctx context.Context, input []CopyKeyInput) ([]*CopyKeysError, error) {
	return newBatchKeysWithRetries(
		/*context*/ ctx, l, input, 10,
		"copy",
		/*action*/ func(bucket kodo.Bucket, input []CopyKeyInput) ([]kodo.BatchItemRet, error) {
			var entries []kodo.KeyPair
			for _, v := range input {
				entries = append(entries, kodo.KeyPair{SrcKey: v.FromKey, DestKey: v.ToKey})
			}
			return bucket.BatchCopy(ctx, entries...)
		},
		2, l.batchSize, l.batchConcurrency,
		func(fromToKey CopyKeyInput, r kodo.BatchItemRet) *CopyKeysError {
			if code := r.Code; code != 200 {
				return &CopyKeysError{
					Code:    r.Code,
					FromKey: fromToKey.FromKey,
					ToKey:   fromToKey.ToKey,
					Error:   r.Error,
				}
			}
			return nil
		},
		func(err *CopyKeysError) (code int, fromToKey CopyKeyInput) {
			return err.Code, CopyKeyInput{
				FromKey: err.FromKey,
				ToKey:   err.ToKey,
			}
		},
	).doAndRetryAction()
}

// 从channel中读取数据并批量复制
func (l *singleClusterLister) copyKeysFromChannel(
	ctx context.Context,
	input <-chan CopyKeyInput,
	errorsChan chan<- CopyKeysError,
) error {
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
	return newBatchKeysWithRetries(
		/*context*/ ctx, l, input, 10,
		"move",
		/*action*/ func(bucket kodo.Bucket, fromToKeys []MoveKeyInput) ([]kodo.BatchItemRet, error) {
			var entries []kodo.KeyPairEx
			for _, e := range fromToKeys {
				entries = append(entries, kodo.KeyPairEx{
					SrcKey:     e.FromKey,
					DestKey:    e.ToKey,
					DestBucket: e.toBucket,
				})
			}
			return bucket.BatchMove(ctx, entries...)
		},
		2, l.batchSize, l.batchConcurrency,
		func(input MoveKeyInput, r kodo.BatchItemRet) *MoveKeysError {
			if code := r.Code; code == 200 {
				return nil
			}
			return &MoveKeysError{
				FromToKeyError: FromToKeyError{
					Code:    r.Code,
					FromKey: input.FromKey,
					ToKey:   input.ToKey,
					Error:   r.Error,
				},
				ToBucket: input.toBucket,
			}
		},
		func(err *MoveKeysError) (code int, input MoveKeyInput) {
			return err.Code, MoveKeyInput{
				FromToKey: FromToKey{
					FromKey: err.FromKey,
					ToKey:   err.ToKey,
				},
				toBucket: err.ToBucket,
			}
		},
	).doAndRetryAction()
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
				toBucket: l.bucket,
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
	return newBatchKeysWithRetries(
		/*context*/ ctx, l, input, 10,
		"rename",
		/*action*/ func(bucket kodo.Bucket, input []RenameKeyInput) ([]kodo.BatchItemRet, error) {
			var entries []kodo.KeyPair
			for _, e := range input {
				entries = append(entries, kodo.KeyPair{
					SrcKey:  e.FromKey,
					DestKey: e.ToKey,
				})
			}
			return bucket.BatchRename(ctx, entries...)
		},
		2, l.batchSize, l.batchConcurrency,
		func(fromToKey RenameKeyInput, r kodo.BatchItemRet) *RenameKeysError {
			return &RenameKeysError{
				Code:    r.Code,
				FromKey: fromToKey.FromKey,
				ToKey:   fromToKey.ToKey,
				Error:   r.Error,
			}
		},
		func(err *RenameKeysError) (code int, fromToKey RenameKeyInput) {
			return err.Code, RenameKeyInput{
				FromKey: err.FromKey,
				ToKey:   err.ToKey,
			}
		},
	).doAndRetryAction()
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
