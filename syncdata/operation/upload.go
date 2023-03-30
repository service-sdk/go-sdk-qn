package operation

import (
	"io"
)

type clusterUploader interface {
	uploadReader(reader io.Reader, key string) error
	upload(file string, key string) error
	uploadDataReader(data io.ReaderAt, size int, key string) error
	uploadData(data []byte, key string) error
}

// Uploader 上传器
type Uploader struct {
	clusterUploader // 上传器
}

// NewUploader 根据配置创建上传器
func NewUploader(c *Config) *Uploader {
	return &Uploader{newSingleClusterUploader(c)}
}

// NewUploaderV2 根据环境变量创建上传器
func NewUploaderV2() *Uploader {
	c := getCurrentConfigurable()
	// 如果没有配置，返回 nil
	if c == nil {
		return nil
	}

	// 如果是单集群配置，直接返回
	if singleClusterConfig, ok := c.(*Config); ok {
		return NewUploader(singleClusterConfig)
	}

	// 如果是多集群配置，返回多集群上传器
	return &Uploader{newMultiClusterUploader(c)}
}

// UploadData 上传内存数据到指定对象中
func (p *Uploader) UploadData(data []byte, key string) (err error) {
	return p.uploadData(data, key)
}

// UploadDataReader 从 Reader 中阅读指定大小的数据并上传到指定对象中
func (p *Uploader) UploadDataReader(data io.ReaderAt, size int, key string) (err error) {
	return p.uploadDataReader(data, size, key)
}

// Upload 上传指定文件到指定对象中
func (p *Uploader) Upload(file string, key string) (err error) {
	return p.upload(file, key)
}

// UploadReader 从 Reader 中阅读全部数据并上传到指定对象中
func (p *Uploader) UploadReader(reader io.Reader, key string) (err error) {
	return p.uploadReader(reader, key)
}
