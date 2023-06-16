package kodo

type Bucket struct {
	Conn *QiniuClient
	Name string
}

// NewBucket 取七牛空间（bucket）的对象实例
// @param client 七牛kodo客户端
// @param name bucket名称
func NewBucket(client *QiniuClient, name string) *Bucket {
	return &Bucket{
		Conn: client,
		Name: name,
	}
}
