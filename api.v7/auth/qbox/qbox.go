package qbox

import (
	"net/http"
	"time"
)

func NewClient(mac *Mac, transport http.RoundTripper) *http.Client {
	t := NewTransport(mac, transport)
	return &http.Client{
		Transport: t,
		Timeout:   10 * time.Minute,
	}
}
