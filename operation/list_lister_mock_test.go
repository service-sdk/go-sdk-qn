package operation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLister_MockForceDeleteKeys(t *testing.T) {
	db := make(map[string][]byte)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/put/"):
			w.WriteHeader(http.StatusOK)
			var size int
			var key string
			_, err := fmt.Sscanf(p, "/put/%d/key/%s", &size, &key)
			assert.NoError(t, err)
			keyBs, err := base64.URLEncoding.DecodeString(key)
			assert.NoError(t, err)
			key = string(keyBs)
			bs := make([]byte, size)
			_, err = io.ReadAll(r.Body)
			assert.NoError(t, err)
			db[key] = bs
		case strings.HasPrefix(p, "/list"):
			w.WriteHeader(http.StatusOK)
			a := struct {
				Items []struct {
					Key string `json:"key"`
				} `json:"items"`
			}{}
			for k := range db {
				a.Items = append(a.Items, struct {
					Key string `json:"key"`
				}{Key: k})
			}
			err := json.NewEncoder(w).Encode(a)
			assert.NoError(t, err)
		case strings.HasPrefix(p, "/batch"):
			w.WriteHeader(http.StatusOK)
			db = make(map[string][]byte)
			var resp []struct {
				Code int `json:"code"`
				Data struct {
					Error string `json:"error"`
				}
			}
			for range db {
				resp = append(resp, struct {
					Code int `json:"code"`
					Data struct {
						Error string `json:"error"`
					}
				}{Code: 612})
			}
			err := json.NewEncoder(w).Encode(resp)
			assert.NoError(t, err)
		default:
			assert.Fail(t, "unknown path: "+r.URL.String())
		}
	}))
	defer server.Close()
	mockConfig := &Config{
		Ak:       "mock_ak",
		Sk:       "mock_sk",
		Bucket:   "testBucket",
		UpHosts:  []string{server.URL},
		RsfHosts: []string{server.URL},
		RsHosts:  []string{server.URL},
	}
	db = make(map[string][]byte)
	lister := &Lister{newSingleClusterLister(mockConfig)}

	uploader := NewUploader(mockConfig)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	result := lister.ListPrefix("")

	// 提取keys，并验证每个key是否存在于result中
	keys := make([]string, len(testCases))
	keys = append(keys, "no_exists_key")
	for i, tc := range testCases {
		keys[i] = tc.name
		assert.Contains(t, result, tc.name)
	}

	// 批量删除
	_, err := lister.ForceDeleteKeys(keys)
	assert.NoError(t, err)

	// 删除结束后每个key都不存在result中了
	result = lister.ListPrefix("")

	for _, key := range keys {
		assert.NotContains(t, result, key)
	}
}
