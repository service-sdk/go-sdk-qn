package qbox

import (
	"net"
	"net/http"
	"time"
)

func NewClient(mac *Mac, dialTimeout, totalTimeout time.Duration) *http.Client {
	dialer := &net.Dialer{
		Timeout: dialTimeout,
	}
	transport := &http.Transport{
		DialContext: dialer.DialContext,
	}
	return &http.Client{
		Transport: NewTransport(mac, transport),
		Timeout:   totalTimeout,
	}
}
