package kodo

import (
	"context"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/goroutine_pool.v7"
)

// Batch 批量操作
func (p Bucket) Batch(ctx context.Context, ret interface{}, op []string) (err error) {
	return p.Conn.CallWithForm(
		ctx,
		ret,
		"POST",
		p.Conn.RSHost+"/batch",
		map[string][]string{"op": op},
		p.Conn.RsTimeout,
	)
}

func (p Bucket) BatchStat(ctx context.Context, keys ...string) (ret []BatchStatItemRet, err error) {
	b := make([]string, len(keys))
	for i, key := range keys {
		b[i] = URIStat(p.Name, key)
	}
	err = p.Batch(ctx, &ret, b)
	return
}

func (p Bucket) BatchDelete(ctx context.Context, keys ...string) (ret []BatchItemRet, err error) {
	b := make([]string, len(keys))
	for i, key := range keys {
		b[i] = URIDelete(p.Name, key)
	}
	err = p.Batch(ctx, &ret, b)
	return
}

type KeyPair struct {
	SrcKey  string
	DestKey string
}

type KeyPairEx struct {
	SrcKey     string
	DestKey    string
	DestBucket string
}

func (p Bucket) BatchMove(ctx context.Context, entries ...KeyPairEx) (ret []BatchItemRet, err error) {
	b := make([]string, len(entries))
	for i, e := range entries {
		b[i] = URIMove(p.Name, e.SrcKey, e.DestBucket, e.DestKey)
	}
	err = p.Batch(ctx, &ret, b)
	return
}

func (p Bucket) BatchCopy(ctx context.Context, entries ...KeyPair) (ret []BatchItemRet, err error) {
	b := make([]string, len(entries))
	for i, e := range entries {
		b[i] = URICopy(p.Name, e.SrcKey, p.Name, e.DestKey)
	}
	err = p.Batch(ctx, &ret, b)
	return
}

// BatchRename 批量重命名
// 该操作没有批量api，需要goroutine模拟实现
func (p Bucket) BatchRename(ctx context.Context, entries ...KeyPair) (ret []BatchItemRet, err error) {
	pool := goroutine_pool.NewGoroutinePool(10)
	ret = make([]BatchItemRet, len(entries))
	for i, e := range entries {
		func(i int) {
			pool.Go(func(ctx context.Context) error {
				if err := p.Rename(ctx, e.SrcKey, e.DestKey); err != nil {
					ret[i].Code = -1
					ret[i].Error = err.Error()
				} else {
					ret[i].Code = 200
				}
				return nil
			})
		}(i)
	}

	if err := pool.Wait(ctx); err != nil {
		return nil, err
	}

	return ret, nil
}
