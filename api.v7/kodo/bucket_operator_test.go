package kodo

import (
	"bytes"
	"context"
	"encoding/base64"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetch(t *testing.T) {
	checkSkipTest(t)
	key := "test.png"
	url := "http://www-static.u.qiniucdn.com/public/v1645/img/css-sprite.png"
	err := bucket.Fetch(nil, key, url)
	defer bucket.Delete(nil, key)
	assert.NoError(t, err)

	stat, err := bucket.Stat(nil, key)
	assert.NoError(t, err)
	assert.Equal(t, "image/png", stat.MimeType)

}

func TestBucketOperator(t *testing.T) {
	checkSkipTest(t)
	key := "test.txt"
	content := []byte("HelloWorld1")
	fsize := int64(len(content))
	mine := "text/plain"

	err := bucket.Put(
		nil, nil,
		key,
		bytes.NewReader(content), fsize,
		nil,
	)
	assert.NoError(t, err)
	defer bucket.Delete(nil, key)

	err = bucket.ChangeMime(nil, key, mine)
	assert.NoError(t, err)

	stat, err := bucket.Stat(nil, key)
	assert.NoError(t, err)
	assert.Equal(t, fsize, stat.Fsize)
	assert.Equal(t, mine, stat.MimeType)

	copyKey := "test_copy.txt"
	err = bucket.Copy(nil, key, copyKey)
	defer bucket.Delete(nil, copyKey)
	assert.NoError(t, err)

	copyStat, err := bucket.Stat(nil, copyKey)
	assert.NoError(t, err)
	assert.Equal(t, copyStat.Hash, stat.Hash)

	moveKey := "test_move.txt"
	err = bucket.Move(nil, key, moveKey)
	defer bucket.Delete(nil, moveKey)
	assert.NoError(t, err)

	moveStat, err := bucket.Stat(nil, moveKey)
	assert.NoError(t, err)
	assert.Equal(t, moveStat.Hash, stat.Hash)

	stat, err = bucket.Stat(nil, key)
	assert.Error(t, err)
}

func TestBucket_Rename(t *testing.T) {
	checkSkipTest(t)

	ctx := context.Background()
	key := "test.txt"
	newKey := "test_new.txt"

	assert.NoError(t, bucket.Put(
		ctx, nil,
		key, bytes.NewReader(nil), 0,
		nil,
	))

	//oldApiServerHost := bucket.Conn.APIHost

	// 构造一个测试服务器，将mock api server下的rename接口代替为move接口
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		decodeURI := func(uri string) string {
			e, err := base64.URLEncoding.DecodeString(uri)
			assert.NoError(t, err)
			return string(e)
		}
		if r.Method == "POST" && strings.HasPrefix(r.URL.Path, "/rename") {
			strs := strings.Split(r.URL.Path, "/")
			e1, e2 := decodeURI(strs[2]), decodeURI(strs[3])
			_, keySrc := func() (string, string) {
				strs := strings.Split(e1, ":")
				return strs[0], strs[1]
			}()
			_, keyDest := func() (string, string) {
				strs := strings.Split(e2, ":")
				return strs[0], strs[1]
			}()
			assert.NoError(t, bucket.Move(ctx, keySrc, keyDest))
			w.WriteHeader(200)
			return
		}
	}))
	defer ts.Close()

	bucket.Conn.APIHost = ts.URL

	assert.NoError(t, bucket.Rename(ctx, key, newKey))
	_, err := bucket.Stat(ctx, key)
	assert.Error(t, err)

	bucket.Delete(ctx, newKey)
}
