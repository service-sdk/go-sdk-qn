package operation

import (
	"github.com/stretchr/testify/assert"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

const mockQueryResponse = `
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

func assertQueryByTimeout(t *testing.T, serverDialDuration, serverDuration, clientDialTimeout, clientTimeout time.Duration, expectErr bool) {
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println("server received request")
		time.Sleep(serverDialDuration)
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(mockQueryResponse))
		assert.NoError(t, err)
		time.Sleep(serverDuration)
	}))
	defer server.Close()

	server.Start()

	log.Println("server started url: ", server.URL)
	queryer := Queryer{
		ak:        "mock_ak",
		bucket:    "mock_bucket",
		ucHosts:   []string{server.URL},
		ucTimeout: clientTimeout,
	}
	c, err := queryer.mustQuery()
	if expectErr {
		assert.Error(t, err)
		return
	} else {
		assert.NoError(t, err)
	}
	assert.True(t, time.Now().Add(86400*time.Second).After(c.CacheExpiredAt))

	assert.Equal(t, int64(86400), c.CachedHosts.Hosts[0].Ttl)

	assert.Equal(t, "iovip.qbox.me", c.CachedHosts.Hosts[0].Io.Domains[0])
	assert.Equal(t, "upload.qiniup.com", c.CachedHosts.Hosts[0].Up.Domains[0])
	assert.Equal(t, "up.qiniup.com", c.CachedHosts.Hosts[0].Up.Domains[1])

	assert.Equal(t, "rs-z0.qbox.me", c.CachedHosts.Hosts[0].Rs.Domains[0])
	assert.Equal(t, "rsf-z0.qbox.me", c.CachedHosts.Hosts[0].Rsf.Domains[0])
	assert.Equal(t, "api.qiniu.com", c.CachedHosts.Hosts[0].ApiServer.Domains[0])
}

func TestQueryer(t *testing.T) {
	cases := []struct {
		serverDialDuration int
		serverDuration     int
		clientDialTimeout  int
		clientTimeout      int
		expectErr          bool
	}{
		// 客户端设置的超时时间小于服务端的处理时间，客户端会超时
		{100, 100, 50, 50, true},
		// 客户端设置的超时时间大于服务端的处理时间，客户端不会超时
		{50, 200, 100, 300, false},
	}

	for _, c := range cases {
		assertQueryByTimeout(
			t,
			time.Duration(c.serverDialDuration)*time.Millisecond, time.Duration(c.serverDuration)*time.Millisecond,
			time.Duration(c.clientDialTimeout)*time.Millisecond, time.Duration(c.clientTimeout)*time.Millisecond,
			c.expectErr,
		)
	}
}
