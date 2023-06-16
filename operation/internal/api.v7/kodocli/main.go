package kodocli

import (
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/common"
	"net/http"
	"time"

	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/rpc.v7"
)

type FileType = common.FileType
type PutPolicy = common.PutPolicy

var ParseUpToken = common.ParseUpToken

// ----------------------------------------------------------

type zoneConfig struct {
	UpHosts []string
}

var zones = []zoneConfig{
	// z0:
	{
		UpHosts: []string{
			"http://upload.qiniu.com",
			"http://up.qiniu.com",
			"-H up.qiniu.com http://183.136.139.16",
		},
	},
	// z1:
	{
		UpHosts: []string{
			"http://upload-z1.qiniu.com",
			"http://up-z1.qiniu.com",
			"-H up-z1.qiniu.com http://106.38.227.27",
		},
	},
}

// ----------------------------------------------------------

type UploadConfig struct {
	UpHosts        []string
	Transport      http.RoundTripper
	UploadPartSize int64
	Concurrency    int
	UseBuffer      bool
	UpTimeout      time.Duration
}

type Uploader struct {
	Conn           rpc.Client
	UpHosts        []string
	UploadPartSize int64
	Concurrency    int
	UseBuffer      bool
}

func NewUploader(zone int, cfg *UploadConfig) (p Uploader) {

	var uc UploadConfig
	if cfg != nil {
		uc = *cfg
	}
	if len(uc.UpHosts) == 0 {
		if zone < 0 || zone >= len(zones) {
			panic("invalid upload config: invalid zone")
		}
		uc.UpHosts = zones[zone].UpHosts
	}

	if uc.UploadPartSize != 0 {
		p.UploadPartSize = uc.UploadPartSize
	} else {
		p.UploadPartSize = minUploadPartSize * 2
	}

	if uc.Concurrency != 0 {
		p.Concurrency = uc.Concurrency
	} else {
		p.Concurrency = 4
	}

	p.UseBuffer = uc.UseBuffer
	p.UpHosts = uc.UpHosts

	p.Conn.Client = &http.Client{
		Transport: uc.Transport,
		Timeout:   uc.UpTimeout,
	}

	p.shuffleUpHosts()
	return
}
