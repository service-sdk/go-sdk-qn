package qbox

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"github.com/service-sdk/go-sdk-qn/v2/x/bytes.v7/seekable"
	"io"
	"net/http"
)

type Mac struct {
	accessKey string
	secretKey []byte
}

func NewMac(accessKey, secretKey string) (mac *Mac) {
	return &Mac{accessKey, []byte(secretKey)}
}

// GetAccessKey 获取 accessKey
func (mac *Mac) GetAccessKey() string {
	return mac.accessKey
}

// GetSecretKey 获取 secretKey
func (mac *Mac) GetSecretKey() string {
	return string(mac.secretKey)
}

func (mac *Mac) Sign(data []byte) (token string) {

	h := hmac.New(sha1.New, mac.secretKey)
	h.Write(data)

	sign := base64.URLEncoding.EncodeToString(h.Sum(nil))
	return mac.accessKey + ":" + sign[:27]
}

func (mac *Mac) SignWithData(b []byte) (token string) {

	blen := base64.URLEncoding.EncodedLen(len(b))

	key := mac.accessKey
	nkey := len(key)
	ret := make([]byte, nkey+30+blen)

	base64.URLEncoding.Encode(ret[nkey+30:], b)

	h := hmac.New(sha1.New, mac.secretKey)
	h.Write(ret[nkey+30:])
	digest := h.Sum(nil)

	copy(ret, key)
	ret[nkey] = ':'
	base64.URLEncoding.Encode(ret[nkey+1:], digest)
	ret[nkey+29] = ':'

	return string(ret)
}

func (mac *Mac) SignRequest(req *http.Request, incbody bool) (token string, err error) {

	h := hmac.New(sha1.New, mac.secretKey)

	u := req.URL
	data := u.Path
	if u.RawQuery != "" {
		data += "?" + u.RawQuery
	}
	_, _ = io.WriteString(h, data+"\n")

	if incbody {
		s2, err2 := seekable.New(req)
		if err2 != nil {
			return "", err2
		}
		h.Write(s2.Bytes())
	}

	sign := base64.URLEncoding.EncodeToString(h.Sum(nil))
	token = mac.accessKey + ":" + sign
	return
}

func (mac *Mac) VerifyCallback(req *http.Request) (bool, error) {

	auth := req.Header.Get("Authorization")
	if auth == "" {
		return false, nil
	}

	token, err := mac.SignRequest(req, true)
	if err != nil {
		return false, err
	}

	return auth == "QBox "+token, nil
}
