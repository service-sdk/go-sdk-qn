package operation

import (
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestApiServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/tool/scale/"):
			_, err := w.Write([]byte(`{"logical_avail_size": 12}`))
			assert.NoError(t, err)
			break
		case strings.HasPrefix(p, "/miscconfigs"):
			_, err := w.Write([]byte(`{"default_write_mode": "Mode0R3N28M4"}`))
			assert.NoError(t, err)
			break
		}
	}))
	defer server.Close()

	api := newSingleClusterApiServer(&Config{
		Ak:             "mock_ak",
		Sk:             "mock_sk",
		ApiServerHosts: []string{server.URL},
	})
	mc, err := api.miscconfigs(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, "Mode0R3N28M4", mc.DefaultWriteMode)

	m, err := api.getLogicalAvailableSizes()
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), m[""])

	s, err := api.getLogicalAvailableSize(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, uint64(12), s)
}

func TestApiServerTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"default_write_mode": "Mode0R3N28M4"}`))
		assert.NoError(t, err)
		time.Sleep(500 * time.Millisecond)
	}))
	defer server.Close()

	api := newSingleClusterApiServer(&Config{
		Ak:             "mock_ak",
		Sk:             "mock_sk",
		ApiServerHosts: []string{server.URL},
		ApiTimeoutMs:   300,
		DialTimeoutMs:  500,
	})
	_, err := api.miscconfigs(context.Background())
	assert.Error(t, err)

	_, err = api.scale(context.Background(), 1, 2)
	assert.Error(t, err)
}
