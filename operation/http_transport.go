package operation

import (
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	httpClientTransport *http.Transport
	once                sync.Once
)

func getHttpClientTransport(config *Config) *http.Transport {
	once.Do(func() {
		dialer := net.Dialer{
			Timeout:   time.Duration(config.DialTimeoutMs) * time.Millisecond,
			KeepAlive: 30 * time.Second,
		}
		httpClientTransport = &http.Transport{
			Proxy:                 http.ProxyFromEnvironment,
			DialContext:           dialer.DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	})
	return httpClientTransport
}
