package kodo

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"
)

func TestUploadAndFetchFile(t *testing.T) {
	checkSkipTest(t)

	key := "test.txt"
	content := []byte("HelloWorld1")
	err := bucket.Put(
		nil, nil,
		key,
		bytes.NewReader(content), int64(len(content)),
		nil,
	)
	defer bucket.Delete(nil, key)
	assert.NoError(t, err)
}

func TestGetPrivateUrl(t *testing.T) {
	checkSkipTest(t)

	key := "test.txt"
	content := []byte("HelloWorld1")

	_ = bucket.Put(
		context.Background(), nil,
		key,
		bytes.NewReader(content), int64(len(content)),
		nil,
	)
	defer bucket.Delete(nil, key)

	baseUrl := MakeBaseUrl(domain, key)
	privateUrl := client.MakePrivateUrl(baseUrl, nil)

	resp, err := http.Get(privateUrl)
	defer resp.Body.Close()
	assert.NoError(t, err)

	buff := bytes.NewBuffer(nil)
	_, _ = buff.ReadFrom(resp.Body)

	assert.Equal(t, content, buff.Bytes())
}