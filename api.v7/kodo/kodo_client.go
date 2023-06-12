package kodo

import (
	"github.com/service-sdk/go-sdk-qn/api.v7/auth/qbox"
	"time"
)
import "github.com/service-sdk/go-sdk-qn/x/rpc.v7"

type Config struct {
	AccessKey string
	SecretKey string
	RSHost    string
	RSFHost   string
	APIHost   string
	Scheme    string
	IoHost    string
	UpHosts   []string

	DialTimeout time.Duration
	RsTimeout   time.Duration
	RsfTimeout  time.Duration
	ApiTimeout  time.Duration
	IoTimeout   time.Duration
	UpTimeout   time.Duration
}

type QiniuClient struct {
	rpc.Client
	Config
	mac     *qbox.Mac
	appName string
}

func NewClient(cfg *Config) *QiniuClient {
	p := new(QiniuClient)
	if cfg != nil {
		p.Config = *cfg
	}

	mac := qbox.NewMac(p.AccessKey, p.SecretKey)
	p.Client = rpc.Client{
		Client: qbox.NewClient(mac, p.DialTimeout, 0),
	}
	p.mac = mac

	if p.Scheme != "https" {
		p.Scheme = "http"
	}
	return p
}
