package kodo

import (
	"github.com/service-sdk/go-sdk-qn/api.v7/auth/qbox"
	"net/http"
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
	Transport http.RoundTripper
}

type Client struct {
	rpc.Client
	Config
	mac     *qbox.Mac
	appName string
}

func NewClient(cfg *Config) *Client {
	p := new(Client)
	if cfg != nil {
		p.Config = *cfg
	}

	mac := qbox.NewMac(p.AccessKey, p.SecretKey)
	p.Client = rpc.Client{
		Client: qbox.NewClient(mac, p.Transport),
	}
	p.mac = mac

	if p.Scheme != "https" {
		p.Scheme = "http"
	}
	return p
}
