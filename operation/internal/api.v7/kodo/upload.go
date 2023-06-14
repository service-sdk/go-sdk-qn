package kodo

import (
	. "context"
	kodocli2 "github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/kodocli"
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/rpc.v7"
	"io"
	"net/http"
)

type PutExtra kodocli2.PutExtra
type RputExtra kodocli2.RputExtra
type PutRet kodocli2.PutRet

func (p Bucket) makeUpToken(key string) string {
	policy := &PutPolicy{
		Scope:   p.Name + ":" + key,
		UpHosts: p.Conn.UpHosts,
	}
	return p.Conn.MakeUpTokenWithExpires(policy, 3600)
}

func (p Bucket) makeUpTokenWithoutKey() string {

	policy := &PutPolicy{
		Scope:   p.Name,
		UpHosts: p.Conn.UpHosts,
	}
	return p.Conn.MakeUpTokenWithExpires(policy, 3600)
}

func (p Bucket) makeUploader() kodocli2.Uploader {
	return kodocli2.Uploader{
		Conn: rpc.Client{
			Client: &http.Client{
				Transport: p.Conn.Transport,
				Timeout:   p.Conn.UpTimeout,
			},
		},
		UpHosts: p.Conn.UpHosts,
	}
}

// ----------------------------------------------------------

// Put 上传一个文件。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。返回的是 PutRet 结构。可选，可以传 nil 表示不感兴趣。
// key     是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// data    是文件内容的访问接口（io.Reader）。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。详细见 PutExtra 结构的描述。
func (p Bucket) Put(
	ctx Context, ret interface{}, key string, data io.ReaderAt, size int64, extra *PutExtra) error {

	uploader := p.makeUploader()
	uptoken := p.makeUpToken(key)
	return uploader.Put(ctx, ret, uptoken, key, data, size, (*kodocli2.PutExtra)(extra))
}

// PutWithoutKey 上传一个文件。自动以文件的 hash 作为文件的访问路径（key）。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。返回的是 PutRet 结构。可选，可以传 nil 表示不感兴趣。
// data    是文件内容的访问接口（io.Reader）。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。详细见 PutExtra 结构的描述。
func (p Bucket) PutWithoutKey(
	ctx Context, ret interface{}, data io.ReaderAt, size int64, extra *PutExtra) error {

	uploader := p.makeUploader()
	uptoken := p.makeUpTokenWithoutKey()
	return uploader.PutWithoutKey(ctx, ret, uptoken, data, size, (*kodocli2.PutExtra)(extra))
}

// PutFile 上传一个文件。
// 和 Put 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.Reader 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。返回的是 PutRet 结构。可选，可以传 nil 表示不感兴趣。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 PutExtra 结构的描述。
func (p Bucket) PutFile(
	ctx Context, ret interface{}, key, localFile string, extra *PutExtra) (err error) {

	uploader := p.makeUploader()
	uptoken := p.makeUpToken(key)
	return uploader.PutFile(ctx, ret, uptoken, key, localFile, (*kodocli2.PutExtra)(extra))
}

// PutFileWithoutKey 上传一个文件。自动以文件的 hash 作为文件的访问路径（key）。
// 和 PutWithoutKey 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.Reader 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。返回的是 PutRet 结构。可选，可以传 nil 表示不感兴趣。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 PutExtra 结构的描述。
func (p Bucket) PutFileWithoutKey(
	ctx Context, ret interface{}, localFile string, extra *PutExtra) (err error) {

	uploader := p.makeUploader()
	uptoken := p.makeUpTokenWithoutKey()
	return uploader.PutFileWithoutKey(ctx, ret, uptoken, localFile, (*kodocli2.PutExtra)(extra))
}

// ----------------------------------------------------------

// Rput 上传一个文件，支持断点续传和分块上传。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// key     是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// data    是文件内容的访问接口。考虑到需要支持分块上传和断点续传，要的是 io.ReaderAt 接口，而不是 io.Reader。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。详细见 RputExtra 结构的描述。
func (p Bucket) Rput(
	ctx Context, ret interface{}, key string, data io.ReaderAt, size int64, extra *RputExtra) error {

	uploader := p.makeUploader()
	uptoken := p.makeUpToken(key)
	return uploader.Rput(ctx, ret, uptoken, key, data, size, (*kodocli2.RputExtra)(extra))
}

// RputWithoutKey 上传一个文件，支持断点续传和分块上传。自动以文件的 hash 作为文件的访问路径（key）。
//
// ctx     是请求的上下文。
// ret     是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// data    是文件内容的访问接口。考虑到需要支持分块上传和断点续传，要的是 io.ReaderAt 接口，而不是 io.Reader。
// fsize   是要上传的文件大小。
// extra   是上传的一些可选项。详细见 RputExtra 结构的描述。
func (p Bucket) RputWithoutKey(
	ctx Context, ret interface{}, data io.ReaderAt, size int64, extra *RputExtra) error {

	uploader := p.makeUploader()
	uptoken := p.makeUpTokenWithoutKey()
	return uploader.RputWithoutKey(ctx, ret, uptoken, data, size, (*kodocli2.RputExtra)(extra))
}

// RputFile 上传一个文件，支持断点续传和分块上传。
// 和 Rput 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.ReaderAt 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// key       是要上传的文件访问路径。比如："foo/bar.jpg"。注意我们建议 key 不要以 '/' 开头。另外，key 为空字符串是合法的。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 RputExtra 结构的描述。
func (p Bucket) RputFile(
	ctx Context, ret interface{}, key, localFile string, extra *RputExtra) (err error) {

	uploader := p.makeUploader()
	uptoken := p.makeUpToken(key)
	return uploader.RputFile(ctx, ret, uptoken, key, localFile, (*kodocli2.RputExtra)(extra))
}

// RputFileWithoutKey 上传一个文件，支持断点续传和分块上传。自动以文件的 hash 作为文件的访问路径（key）。
// 和 RputWithoutKey 不同的只是一个通过提供文件路径来访问文件内容，一个通过 io.ReaderAt 来访问。
//
// ctx       是请求的上下文。
// ret       是上传成功后返回的数据。如果 uptoken 中没有设置 CallbackUrl 或 ReturnBody，那么返回的数据结构是 PutRet 结构。
// localFile 是要上传的文件的本地路径。
// extra     是上传的一些可选项。详细见 RputExtra 结构的描述。
func (p Bucket) RputFileWithoutKey(
	ctx Context, ret interface{}, localFile string, extra *RputExtra) (err error) {

	uploader := p.makeUploader()
	uptoken := p.makeUpTokenWithoutKey()
	return uploader.RputFileWithoutKey(ctx, ret, uptoken, localFile, (*kodocli2.RputExtra)(extra))
}

// ----------------------------------------------------------
