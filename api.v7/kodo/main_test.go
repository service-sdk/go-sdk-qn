package kodo

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	key        = "aa"
	keyFetch   = "afetch"
	newkey1    = "bbbb"
	newkey2    = "cccc"
	fetchURL   = "http://www-static.u.qiniucdn.com/public/v1645/img/css-sprite.png"
	bucketName string
	domain     string
	client     *Client
	bucket     = newBucket()

	QINIU_KODO_TEST string
)

func init() {

	if skipTest() {
		return
	}
	rand.Seed(time.Now().UnixNano())
	key += strconv.Itoa(rand.Int())
	keyFetch += strconv.Itoa(rand.Int())
	newkey1 += strconv.Itoa(rand.Int())
	newkey2 += strconv.Itoa(rand.Int())
	bucket.BatchDelete(nil, key, keyFetch, newkey1, newkey2)
}

func newBucket() (bucket Bucket) {

	QINIU_KODO_TEST = os.Getenv("QINIU_KODO_TEST")
	if skipTest() {
		println("[INFO] QINIU_KODO_TEST: skipping to test qiniupkg.com/api.v7")
		return
	}

	ak := os.Getenv("QINIU_ACCESS_KEY")
	sk := os.Getenv("QINIU_SECRET_KEY")
	if ak == "" || sk == "" {
		panic("require ACCESS_KEY & SECRET_KEY")
	}
	SetMac(ak, sk)

	bucketName = os.Getenv("QINIU_TEST_BUCKET")
	domain = os.Getenv("QINIU_TEST_DOMAIN")
	if bucketName == "" || domain == "" {
		panic("require test env")
	}
	client = NewWithoutZone(&Config{
		AccessKey: ak,
		SecretKey: sk,
		UpHosts:   strings.Split(os.Getenv("QINIU_TEST_UP_HOSTS"), ","),
		RSFHost:   os.Getenv("QINIU_TEST_RSF_HOST"),
		RSHost:    os.Getenv("QINIU_TEST_RS_HOST"),
		APIHost:   os.Getenv("QINIU_TEST_API_HOST"),
		IoHost:    os.Getenv("QINIU_TEST_IO_HOST"),
		Scheme:    "http",
	})

	return client.Bucket(bucketName)
}

func skipTest() bool {

	return QINIU_KODO_TEST == ""
}

func upFile(localFile, key string) error {

	return bucket.PutFile(nil, nil, key, localFile, nil)
}

func TestFetch(t *testing.T) {

	if skipTest() {
		return
	}

	err := bucket.Fetch(nil, keyFetch, fetchURL)
	assert.NoError(t, err)

	entry, err := bucket.Stat(nil, keyFetch)
	assert.NoError(t, err)
	assert.Equal(t, "image/png", entry.MimeType)
}

func TestEntry(t *testing.T) {
	if skipTest() {
		return
	}

	//上传一个文件用用于测试
	err := upFile("doc.go", key)
	assert.NoError(t, err)
	defer bucket.Delete(nil, key)

	einfo, err := bucket.Stat(nil, key)
	assert.NoError(t, err)

	mime := "text/plain"
	err = bucket.ChangeMime(nil, key, mime)
	assert.NoError(t, err)

	einfo, err = bucket.Stat(nil, key)
	assert.NoError(t, err)
	assert.Equal(t, mime, einfo.MimeType)

	err = bucket.Copy(nil, key, newkey1)
	assert.NoError(t, err)

	enewinfo, err := bucket.Stat(nil, newkey1)
	assert.NoError(t, err)
	assert.Equal(t, einfo.Hash, enewinfo.Hash)

	err = bucket.Move(nil, newkey1, newkey2)
	assert.NoError(t, err)

	enewinfo2, err := bucket.Stat(nil, newkey2)
	assert.NoError(t, err)
	assert.Equal(t, enewinfo.Hash, enewinfo2.Hash)

	err = bucket.Delete(nil, newkey2)
	assert.NoError(t, err)
}
