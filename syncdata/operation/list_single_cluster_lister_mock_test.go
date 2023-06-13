package operation

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSingleClusterLister_RsfTimeout(t *testing.T) {
	assertTimeout := func(serverDuration, clientTimeout int, expectError bool) {
		rsfServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := w.Write([]byte(`{"items":[{"key":"testKey"}]}`))
			assert.NoError(t, err)
			time.Sleep(time.Duration(serverDuration) * time.Millisecond)
		}))
		l := newSingleClusterLister(&Config{
			RsfHosts:     []string{rsfServer.URL},
			RsfTimeoutMs: clientTimeout,
		})
		files, err := l.listPrefix(context.Background(), "")
		if expectError {
			assert.Error(t, err)
			return
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 1, len(files))
			assert.Equal(t, "testKey", files[0])
		}
	}
	assertTimeout(500, 100, true)
	assertTimeout(100, 500, false)
}

func TestSingleClusterLister_RsTimeout(t *testing.T) {
	assertTimeout := func(serverDuration, clientTimeout int, expectError bool) {
		rsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			time.Sleep(time.Duration(serverDuration) * time.Millisecond)
		}))
		l := newSingleClusterLister(&Config{
			RsHosts:     []string{rsServer.URL},
			RsTimeoutMs: clientTimeout,
		})
		err := l.copy("testKey", "testKey2")
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
