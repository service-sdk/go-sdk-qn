package operation

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type mockServer struct {
	db     map[string][]byte
	server *httptest.Server
}

func newMockServer(t *testing.T) *mockServer {
	mockServer := &mockServer{
		db: make(map[string][]byte),
	}
	mockServer.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/put/"):
			mockServer.putFile(t, w, r)
		case strings.HasPrefix(p, "/list"):
			mockServer.listFiles(t, w, r)
		case strings.HasPrefix(p, "/batch"):
			mockServer.batch(t, w, r)
		default:
			assert.Fail(t, "unknown path: "+r.URL.String())
		}
	}))
	return mockServer
}

func (m *mockServer) putFile(t *testing.T, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	var size int
	var key string
	_, err := fmt.Sscanf(r.URL.Path, "/put/%d/key/%s", &size, &key)
	assert.NoError(t, err)
	keyBs, err := base64.URLEncoding.DecodeString(key)
	assert.NoError(t, err)
	key = string(keyBs)
	bs := make([]byte, size)
	_, err = io.ReadAll(r.Body)
	assert.NoError(t, err)
	m.db[key] = bs
}

func (m *mockServer) listFiles(t *testing.T, w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	a := struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}{}
	for k := range m.db {
		a.Items = append(a.Items, struct {
			Key string `json:"key"`
		}{Key: k})
	}
	err := json.NewEncoder(w).Encode(a)
	assert.NoError(t, err)
}

func (m *mockServer) batch(t *testing.T, w http.ResponseWriter, r *http.Request) {
	log.Println("mock server batch")
	type (
		BatchResponse struct {
			Code  int    `json:"code"`
			Error string `json:"error"`
		}
	)
	w.WriteHeader(http.StatusOK)
	var resp []BatchResponse
	for range m.db {
		resp = append(resp, BatchResponse{
			Code:  200,
			Error: "",
		})
	}
	err := json.NewEncoder(w).Encode(resp)
	assert.NoError(t, err)
	m.db = make(map[string][]byte)
}

func (m *mockServer) Close() {
	m.server.Close()
}

func (m *mockServer) getConfig() *Config {
	return &Config{
		Ak:       "mock_ak",
		Sk:       "mock_sk",
		Bucket:   "testBucket",
		UpHosts:  []string{m.server.URL},
		RsfHosts: []string{m.server.URL},
		RsHosts:  []string{m.server.URL},
	}
}
