package kodo

import "context"

// Stat 取文件属性
// @param ctx 是请求的上下文
// @param key 是要访问的文件的访问路径
func (p Bucket) Stat(ctx context.Context, key string) (entry Entry, err error) {
	err = p.Conn.CallWithTimeout(ctx, &entry, "POST", p.Conn.RSHost+URIStat(p.Name, key), p.Conn.RsTimeout)
	return
}

// Delete 删除一个文件
// @param ctx 是请求的上下文
// @param key 是要删除的文件的访问路径
func (p Bucket) Delete(ctx context.Context, key string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.RSHost+URIDelete(p.Name, key), p.Conn.RsTimeout)
}

// Move 移动一个文件。
// @param ctx     是请求的上下文。
// @param keySrc  是要移动的文件的旧路径。
// @param keyDest 是要移动的文件的新路径。
func (p Bucket) Move(ctx context.Context, keySrc, keyDest string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.RSHost+URIMove(p.Name, keySrc, p.Name, keyDest), p.Conn.RsTimeout)
}

// Rename 重命名一个文件。
// @param ctx     是请求的上下文。
// @param keySrc  是要移动的文件的旧路径。
// @param keyDest 是要移动的文件的新路径。
func (p Bucket) Rename(ctx context.Context, keySrc, keyDest string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.APIHost+URIRename(p.Name, keySrc, p.Name, keyDest), p.Conn.ApiTimeout)
}

// MoveEx 跨空间（bucket）移动一个文件。
// @param ctx        是请求的上下文。
// @param keySrc     是要移动的文件的旧路径。
// @param bucketDest 是文件的目标空间。
// @param keyDest    是要移动的文件的新路径。
func (p Bucket) MoveEx(ctx context.Context, keySrc, bucketDest, keyDest string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.RSHost+URIMove(p.Name, keySrc, bucketDest, keyDest), p.Conn.RsTimeout)
}

// Copy 复制一个文件。
// @param ctx     是请求的上下文。
// @param keySrc  是要复制的文件的源路径。
// @param keyDest 是要复制的文件的目标路径。
func (p Bucket) Copy(ctx context.Context, keySrc, keyDest string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.RSHost+URICopy(p.Name, keySrc, p.Name, keyDest), p.Conn.RsTimeout)
}

// ChangeMime 修改文件的MIME类型。
// @param ctx  是请求的上下文。
// @param key  是要修改的文件的访问路径。
// @param mime 是要设置的新MIME类型。
func (p Bucket) ChangeMime(ctx context.Context, key, mime string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.RSHost+URIChangeMime(p.Name, key, mime), p.Conn.RsTimeout)
}

// Fetch 从网上抓取一个资源并存储到七牛空间（bucket）中
// @param ctx 是请求的上下文
// @param key 是要存储的文件的访问路径。如果文件已经存在则覆盖
// @param url 是要抓取的资源的URL
func (p Bucket) Fetch(ctx context.Context, key string, url string) (err error) {
	return p.Conn.CallWithTimeout(ctx, nil, "POST", p.Conn.IoHost+uriFetch(p.Name, key, url), p.Conn.IoTimeout)
}
