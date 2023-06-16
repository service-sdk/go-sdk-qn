package operation

import (
	"net"
	"net/http"
	"sync"
	"time"
)

var (
	httpClientTransport      *http.Transport
	config                   *Config
	httpClientTransportMutex sync.Mutex
)

func getHttpClientTransport(cfg *Config) *http.Transport {
	httpClientTransportMutex.Lock()
	defer httpClientTransportMutex.Unlock()
	if cfg != config {
		config = cfg
		httpClientTransport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: net.Dialer{
				Timeout:   buildDurationByMs(config.DialTimeoutMs, DefaultConfigDialTimeoutMs),
				KeepAlive: 30 * time.Second,
			}.DialContext,
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}
	}
	return httpClientTransport
}
