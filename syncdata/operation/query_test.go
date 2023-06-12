package operation

import (
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
)

const mockResponse = `
{
    "hosts": [
        {
            "region": "z0",
            "ttl": 86400,
            "io": {
                "domains": [
                    "iovip.qbox.me"
                ]
            },
            "up": {
                "domains": [
                    "upload.qiniup.com",
                    "up.qiniup.com"
                ],
                "old": [
                    "upload.qbox.me",
                    "up.qbox.me"
                ]
            },
            "uc": {
                "domains": [
                    "uc.qbox.me"
                ]
            },
            "rs": {
                "domains": [
                    "rs-z0.qbox.me"
                ]
            },
            "rsf": {
                "domains": [
                    "rsf-z0.qbox.me"
                ]
            },
            "api": {
                "domains": [
                    "api.qiniu.com"
                ]
            },
            "s3": {
                "domains": [
                    "s3-cn-east-1.qiniucs.com"
                ],
                "region_alias": "cn-east-1"
            }
        }
    ],
    "ttl": 86400
}
`

func TestQueryer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(mockResponse))
		assert.NoError(t, err)
	}))

	queryer := Queryer{
		ak:      "mock_ak",
		bucket:  "mock_bucket",
		ucHosts: []string{server.URL},
		client:  server.Client(),
	}

	a := queryer.QueryUpHosts(true)
	log.Println(a)
}
