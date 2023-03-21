package kodo

import (
	"context"
)

// Batch 批量操作
func (p *Client) Batch(ctx context.Context, ret interface{}, op []string) (err error) {
	return p.CallWithForm(
		ctx,
		ret,
		"POST",
		p.RSHost+"/batch",
		map[string][]string{"op": op},
	)
}

func (p Bucket) BatchStat(ctx context.Context, keys ...string) (ret []BatchStatItemRet, err error) {
	b := make([]string, len(keys))
	for i, key := range keys {
		b[i] = URIStat(p.Name, key)
	}
	err = p.Conn.Batch(ctx, &ret, b)
	return
}

func (p Bucket) BatchDelete(ctx context.Context, keys ...string) (ret []BatchItemRet, err error) {

	b := make([]string, len(keys))
	for i, key := range keys {
		b[i] = URIDelete(p.Name, key)
	}
	err = p.Conn.Batch(ctx, &ret, b)
	return
}

type KeyPair struct {
	Src  string
	Dest string
}

func (p Bucket) BatchMove(ctx context.Context, entries ...KeyPair) (ret []BatchItemRet, err error) {
	b := make([]string, len(entries))
	for i, e := range entries {
		b[i] = URIMove(p.Name, e.Src, p.Name, e.Dest)
	}
	err = p.Conn.Batch(ctx, &ret, b)
	return
}

func (p Bucket) BatchCopy(ctx context.Context, entries ...KeyPair) (ret []BatchItemRet, err error) {
	b := make([]string, len(entries))
	for i, e := range entries {
		b[i] = URICopy(p.Name, e.Src, p.Name, e.Dest)
	}
	err = p.Conn.Batch(ctx, &ret, b)
	return
}
