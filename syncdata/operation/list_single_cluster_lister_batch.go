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
type batchOutput interface {
	DeleteKeysError | FileStat | FromToKeyError | CopyKeysError | MoveKeysError | RenameKeysError
}

type batchInput interface {
	string | FromToKey | CopyKeyInput | RenameKeyInput | MoveKeyInput
}

// batchAction 批量操作的动作
type batchAction[R batchKodoResult, I batchInput] func(bucket kodo.Bucket, paths []I) ([]R, error)

// 批处理结果代码解析
type batchKodoResultCodeGetter[R batchKodoResult] func(result R) (code int)

// batchResultBuilder 错误构造器
type batchResultBuilder[R batchKodoResult, O batchOutput, I batchInput] func(path I, r R) *O

// batchResultMessageParser 错误信息解析器
type batchResultMessageParser[O batchOutput, I batchInput] func(err *O) (code int, path I)

type batchKeysWithRetries[R batchKodoResult, O batchOutput, I batchInput] struct {
	l                    *singleClusterLister
	pool                 *goroutine_pool.GoroutinePool
	inputs               []I
	retries              uint
	retried              uint
	failedInputs         []I
	failedInputsIndexMap []int
	failedRsHosts        map[string]struct{}
	failedRsHostsLock    sync.RWMutex
	ctx                  context.Context
	outputs              []*O

	// 动作名称
	actionName string

	// 动作与动作最大重试次数
	action           batchAction[R, I]
	actionMaxRetries uint

	// 分批处理的大小与并发数
	batchSize        int
	batchConcurrency int

	resultBuilder        batchResultBuilder[R, O, I]
	resultMessageParser  batchResultMessageParser[O, I]
	kodoResultCodeGetter batchKodoResultCodeGetter[R]
}

func newBatchKeysWithRetries[R batchKodoResult, O batchOutput, I batchInput](
	ctx context.Context,
	l *singleClusterLister,
	inputs []I,
	retries uint,

	actionName string,
	action batchAction[R, I],
	actionMaxRetries uint,

	batchSize int, // 分批处理的大小与并发数
	batchConcurrency int,

	resultBuilder batchResultBuilder[R, O, I],
	resultMessageParser batchResultMessageParser[O, I],
	kodoResultCodeGetter batchKodoResultCodeGetter[R],
) *batchKeysWithRetries[R, O, I] {
	// 并发数计算
	concurrency := (len(inputs) + l.batchSize - 1) / l.batchSize
	if concurrency > l.batchConcurrency {
		concurrency = l.batchConcurrency
	}
	return &batchKeysWithRetries[R, O, I]{
		ctx:                  ctx,
		l:                    l,
		pool:                 goroutine_pool.NewGoroutinePool(concurrency),
		inputs:               inputs,
		retries:              retries,
		failedRsHosts:        make(map[string]struct{}),
		outputs:              make([]*O, len(inputs)),
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
func (d *batchKeysWithRetries[R, O, I]) doActionOnce(inputs []I) ([]R, error) {
	// 获取一个 rs host
	d.failedRsHostsLock.RLock()
	defer d.failedRsHostsLock.RUnlock()
	host := d.l.nextRsHost(d.failedRsHosts)

	// 根据拿到的rs域名构造一个 bucket
	bucket := d.l.newBucket(host, "", "")

	// 执行相应批处理操作
	r, err := d.action(bucket, inputs)

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

func (d *batchKeysWithRetries[R, O, I]) doAction(inputs []I) (r []R, err error) {
	for i := uint(0); i < d.actionMaxRetries; i++ {
		r, err = d.doActionOnce(inputs)
		// 没有错误，直接返回
		if err == nil {
			return r, nil
		}
	}
	return nil, err
}

func (d *batchKeysWithRetries[R, O, I]) pushAllTaskToPool() {
	// 将任务进行分批处理
	for i := 0; i < len(d.inputs); i += d.l.batchSize {
		// 计算本次批量处理的数量
		size := d.l.batchSize
		if size > len(d.inputs)-i {
			size = len(d.inputs) - i
		}

		// inputs 是这批要处理的文件
		// index 是这批文件的起始位置
		func(paths []I, index int) {
			d.pool.Go(func(ctx context.Context) error {
				// 删除这一批文件，返回成功删除的结果
				res, _ := d.doAction(paths)

				// 批量删除的结果，过滤掉成功的，记录失败的到 outputs 中
				for j, v := range res {
					err := d.resultBuilder(paths[j], v)
					d.outputs[index+j] = err
					if err != nil {
						elog.Warn(d.actionName, " bad file:", paths[j], "with error:", err)
					}
				}
				return nil
			})
		}(d.inputs[i:i+size], i)
	}
}

func (d *batchKeysWithRetries[R, O, I]) waitAllTask() error {
	// 等待所有的批量任务完成，如果出错了，直接结束返回错误
	return d.pool.Wait(d.ctx)
}

func (d *batchKeysWithRetries[R, O, I]) doAndRetryAction() ([]*O, error) {
	// 把所有的批量任务放到 goroutine pool 中
	d.pushAllTaskToPool()

	// 等待所有的批量任务完成
	if err := d.waitAllTask(); err != nil {
		return nil, err
	}

	// 剩余重试次数为0，直接返回结果
	if d.retries <= 0 {
		return d.outputs, nil
	}

	// 如果期望重试的次数大于0，那么尝试一次重试
	for i, e := range d.outputs {
		// 对于所有的5xx错误，都记录下来，等待重试
		if e != nil {
			errorCode, errorPath := d.resultMessageParser(e)
			if errorCode/100 == 5 {
				d.failedInputsIndexMap = append(d.failedInputsIndexMap, i)
				d.failedInputs = append(d.failedInputs, errorPath)
				elog.Warn("retry", d.actionName, " bad file:", errorPath, "with code:", errorCode)
			}
		}
	}

	// 如果有需要重试的文件，那么进行重试
	if len(d.failedInputs) > 0 {
		elog.Warn(d.actionName, " ", len(d.failedInputs), " bad files, retried:", d.retried)

		// 将失败的文件进行重试
		d.retries--
		d.retried++
		retriedOrrors, err := d.doAndRetryAction()

		// 如果重试出错了，直接返回错误
		if err != nil {
			return d.outputs, err
		}

		// 如果重试成功了，那么把重试的结果合并到原来的结果中
		for i, retriedOrror := range retriedOrrors {
			d.outputs[d.failedInputsIndexMap[i]] = retriedOrror
		}
	}
	// 返回所有的错误
	return d.outputs, nil
}
