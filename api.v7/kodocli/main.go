package kodocli

import (
	"net/http"
	"time"

	"github.com/service-sdk/go-sdk-qn/api.v7/conf"
	"github.com/service-sdk/go-sdk-qn/x/rpc.v7"
	"github.com/service-sdk/go-sdk-qn/x/url.v7"
)

type UploadConfig struct {
	UpHosts   []string
	Transport http.RoundTripper

	APIHost string
	Scheme  string
}

type Uploader struct {
	Conn    rpc.Client
	UpHosts []string
}

func NewUploader(cfg *UploadConfig) (p Uploader) {
	var uc UploadConfig
	if cfg != nil {
		uc = *cfg
	}
	if uc.Scheme != "https" {
		uc.Scheme = "http"
	}

	p.UpHosts = uc.UpHosts
	p.Conn.Client = &http.Client{Transport: uc.Transport, Timeout: 10 * time.Minute}
	return
}

func NewUploaderWithoutZone(cfg *UploadConfig) (p Uploader) {
	return NewUploader(cfg)
}

// MakeBaseUrl 根据空间(Bucket)的域名，以及文件的 key，获得 baseUrl。
// 如果空间是 public 的，那么通过 baseUrl 可以直接下载文件内容。
// 如果空间是 private 的，那么需要对 baseUrl 进行私有签名得到一个临时有效的 privateUrl 进行下载。
func MakeBaseUrl(domain, key string) (baseUrl string) {
	return "http://" + domain + "/" + url.Escape(key)
}

// SetAppName 设置使用这个SDK的应用程序名。userApp 必须满足 [A-Za-z0-9_\ \-\.]*
func SetAppName(userApp string) error {
	return conf.SetAppName(userApp)
}
