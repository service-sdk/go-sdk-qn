package qbox

import (
	"net/http"
	"time"
)

func NewClient(mac *Mac, httpTransport http.RoundTripper, totalTimeout time.Duration) *http.Client {
	return &http.Client{
		Transport: NewTransport(mac, httpTransport),
		Timeout:   totalTimeout,
	}
}
