package operation

import (
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSingleClusterUploader_TimeoutConfig(t *testing.T) {
	assertTimeout := func(serverDuration, clientTimeout int, expectError bool) {
		upServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			bs, err := io.ReadAll(r.Body)
			assert.NoError(t, err)
			assert.Equal(t, "testData", string(bs))
			time.Sleep(time.Duration(serverDuration) * time.Millisecond)
		}))
		uploader := newSingleClusterUploader(&Config{
			UpHosts:     []string{upServer.URL},
			UpTimeoutMs: clientTimeout,
		})
		err := uploader.uploadData([]byte("testData"), "testKey")
		if expectError {
			assert.Error(t, err)
			return
		} else {
			assert.NoError(t, err)
		}
	}
	assertTimeout(500, 100, true)
	assertTimeout(100, 500, false)
}
