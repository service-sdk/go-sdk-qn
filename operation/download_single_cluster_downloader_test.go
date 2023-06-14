package operation

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func assertDownloadBytesTimeout(t *testing.T, serverDuration, clientTimeout int, hasError bool) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("test"))
		assert.NoError(t, err)
		time.Sleep(time.Duration(serverDuration) * time.Millisecond)
	}))
	downloader := newSingleClusterDownloader(&Config{
		Bucket:      "testBucket",
		IoHosts:     []string{server.URL},
		Ak:          "mock_ak",
		Sk:          "mock_sk",
		IoTimeoutMs: clientTimeout,
	})
	bs, err := downloader.downloadBytes("testKey")
	if hasError {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
		assert.Equal(t, []byte("test"), bs)
	}
}
func TestDownloader_DownloadBytesTimeout(t *testing.T) {
	assertDownloadBytesTimeout(t, 500, 100, true)
	assertDownloadBytesTimeout(t, 100, 500, false)
}
