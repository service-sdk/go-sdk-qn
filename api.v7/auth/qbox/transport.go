package qbox

import (
	. "github.com/service-sdk/go-sdk-qn/v2/api.v7/conf"
	"net/http"
)

type Transport struct {
	mac       Mac
	Transport http.RoundTripper
}

func incBody(req *http.Request) bool {

	if req.Body == nil {
		return false
	}
	if ct, ok := req.Header["Content-Type"]; ok {
		switch ct[0] {
		case "application/x-www-form-urlencoded":
			return true
		}
	}
	return false
}

func (t *Transport) NestedObject() interface{} {

	return t.Transport
}

func (t *Transport) RoundTrip(req *http.Request) (resp *http.Response, err error) {

	token, err := t.mac.SignRequest(req, incBody(req))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "QBox "+token)
	return t.Transport.RoundTrip(req)
}

func NewTransport(mac *Mac, transport http.RoundTripper) *Transport {

	if mac == nil {
		mac = NewMac(AccessKey, SecretKey)
	}
	if transport == nil {
		transport = http.DefaultTransport
	}
	t := &Transport{mac: *mac, Transport: transport}
	return t
}
