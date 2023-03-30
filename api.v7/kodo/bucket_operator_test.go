package kodo

import (
	"bytes"
	"github.com/stretchr/testify/assert"
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
