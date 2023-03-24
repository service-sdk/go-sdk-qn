package operation

import (
	"context"
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"sync"
)

type batchKodoResult interface {
	kodo.BatchItemRet | kodo.BatchStatItemRet
}
type batchResult interface{ DeleteKeysError | FileStat }

// batchAction 批量操作的动作
type batchAction[R batchKodoResult] func(bucket kodo.Bucket, paths []string) ([]R, error)

// 批处理结果代码解析
type batchKodoResultCodeGetter[R batchKodoResult] func(result R) (code int)

// batchResultBuilder 错误构造器
type batchResultBuilder[R batchKodoResult, E batchResult] func(code int, path string, r R) *E

// batchResultMessageParser 错误信息解析器
type batchResultMessageParser[E batchResult] func(err *E) (code int, path string)

type batchKeysWithRetries[R batchKodoResult, E batchResult] struct {
	l                  *singleClusterLister
	pool               *goroutine_pool.GoroutinePool
	paths              []string
	retries            uint
	retried            uint
	failedPath         []string
	failedPathIndexMap []int
	failedRsHosts      map[string]struct{}
	failedRsHostsLock  sync.RWMutex
	ctx                context.Context
	errors             []*E

	// 动作名称
	actionName string

	// 动作与动作最大重试次数
	action           batchAction[R]
	actionMaxRetries uint

	// 分批处理的大小与并发数
	batchSize        int
	batchConcurrency int

	resultBuilder        batchResultBuilder[R, E]
	resultMessageParser  batchResultMessageParser[E]
	kodoResultCodeGetter batchKodoResultCodeGetter[R]
}

func newBatchKeysWithRetries[R batchKodoResult, E batchResult](
	ctx context.Context,
	l *singleClusterLister,
	paths []string,
	retries uint,

	actionName string,
	action func(bucket kodo.Bucket, paths []string) ([]R, error),
	actionMaxRetries uint,

	batchSize int, // 分批处理的大小与并发数
	batchConcurrency int,

	resultBuilder batchResultBuilder[R, E],
	resultMessageParser batchResultMessageParser[E],
	kodoResultCodeGetter batchKodoResultCodeGetter[R],
) *batchKeysWithRetries[R, E] {
	// 并发数计算
	concurrency := (len(paths) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	return &batchKeysWithRetries[R, E]{
		ctx:                  ctx,
		l:                    l,
		pool:                 goroutine_pool.NewGoroutinePool(concurrency),
		paths:                paths,
		retries:              retries,
		failedRsHosts:        make(map[string]struct{}),
		errors:               make([]*E, len(paths)),
		actionName:           actionName,
		action:               action,
		actionMaxRetries:     actionMaxRetries,
		batchSize:            batchSize,
		batchConcurrency:     batchConcurrency,
		resultBuilder:        resultBuilder,
		resultMessageParser:  resultMessageParser,
		kodoResultCodeGetter: kodoResultCodeGetter,
	}
}

// 执行一次动作
func (d *batchKeysWithRetries[R, E]) doActionOnce(paths []string) ([]R, error) {
	// 获取一个 rs host
	d.failedRsHostsLock.RLock()
	defer d.failedRsHostsLock.RUnlock()
	host := d.l.nextRsHost(d.failedRsHosts)

	// 根据拿到的rs域名构造一个 bucket
	bucket := d.l.newBucket(host, "", "")

	// 执行相应批处理操作
	r, err := d.action(bucket, paths)

	// 成功退出
	if err == nil {
		succeedHostName(host)
		return r, nil
	}
	// 出错了，获取写锁，记录错误的 rs host
	d.failedRsHostsLock.Lock()
	defer d.failedRsHostsLock.Unlock()
	d.failedRsHosts[host] = struct{}{}

	failHostName(host)
	return nil, err
}

func (d *batchKeysWithRetries[R, E]) doAction(paths []string) (r []R, err error) {
	for i := uint(0); i < d.actionMaxRetries; i++ {
		r, err = d.doActionOnce(paths)
		// 没有错误，直接返回
		if err == nil {
			return r, nil
		}
	}
	return nil, err
}

func (d *batchKeysWithRetries[R, E]) pushAllTaskToPool() {
	// 将任务进行分批处理
	for i := 0; i < len(d.paths); i += d.l.batchSize {
		// 计算本次批量处理的数量
		size := d.l.batchSize
		if size > len(d.paths)-i {
			size = len(d.paths) - i
		}

		// paths 是这批要处理的文件
		// index 是这批文件的起始位置
		func(paths []string, index int) {
			d.pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，返回成功删除的结果
				res, _ := d.doAction(paths)

				// 批量删除的结果，过滤掉成功的，记录失败的到 errors 中
				for j, v := range res {
					if code := d.kodoResultCodeGetter(v); code == 200 {
						d.errors[index+j] = nil
						continue
					} else {
						err := d.resultBuilder(code, paths[j], v)
						d.errors[index+j] = d.resultBuilder(code, paths[j], v)
						elog.Warn(d.actionName, " bad file:", paths[j], "with error:", err)
					}
				}
				return nil
			})
		}(d.paths[i:i+size], i)
	}
}

func (d *batchKeysWithRetries[R, E]) waitAllTask() error {
	// 等待所有的批量任务完成，如果出错了，直接结束返回错误
	return d.pool.Wait(d.ctx)
}

func (d *batchKeysWithRetries[R, E]) doAndRetryAction() ([]*E, error) {
	// 把所有的批量任务放到 goroutine pool 中
	d.pushAllTaskToPool()

	// 等待所有的批量任务完成
	if err := d.waitAllTask(); err != nil {
		return nil, err
	}

	// 剩余重试次数为0，直接返回结果
	if d.retries <= 0 {
		return d.errors, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range d.errors {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil {
			errorCode, errorPath := d.resultMessageParser(e)
			if errorCode/100 == 5 {
				d.failedPathIndexMap = append(d.failedPathIndexMap, i)
				d.failedPath = append(d.failedPath, errorPath)
				elog.Warn("retry", d.actionName, " bad file:", errorPath, "with code:", errorCode)
			}
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(d.failedPath) > 0 {
		elog.Warn(d.actionName, " ", len(d.failedPath), " bad files, retried:", d.retried)

		// 将失败的文件进行重试
		d.retries--
		d.retried++
		retriedErrors, err := d.doAndRetryAction()

		// 如果重试出错了，直接返回错误
		if err != nil {
			return d.errors, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedError := range retriedErrors {
			d.errors[d.failedPathIndexMap[i]] = retriedError
		}
	}
	// 返回所有的错误
	return d.errors, nil
}
