package kodo

import (
	"log"
	"os"
	"strings"
	"testing"
)

var (
	skipTest   = true
	bucketName string
	domain     string
	client     *QiniuClient
	bucket     *Bucket
)

func setup() {
	// 若环境变量QINIU_KODO_TEST未设置，则跳过测试
	skipTest = os.Getenv("QINIU_KODO_TEST") == ""
	if skipTest {
		os.Exit(0)
	}

	ak := os.Getenv("QINIU_ACCESS_KEY")
	sk := os.Getenv("QINIU_SECRET_KEY")

	if ak == "" || sk == "" {
		log.Fatalln("require ACCESS_KEY & SECRET_KEY")
	}

	bucketName = os.Getenv("QINIU_TEST_BUCKET")
	domain = os.Getenv("QINIU_TEST_DOMAIN")
	if bucketName == "" || domain == "" {
		log.Fatalln("require QINIU_TEST_BUCKET & QINIU_TEST_DOMAIN")
	}
	client = NewClient(&Config{
		AccessKey: ak,
		SecretKey: sk,
		UpHosts:   strings.Split(os.Getenv("QINIU_TEST_UP_HOSTS"), ","),
		RSFHost:   os.Getenv("QINIU_TEST_RSF_HOST"),
		RSHost:    os.Getenv("QINIU_TEST_RS_HOST"),
		APIHost:   os.Getenv("QINIU_TEST_API_HOST"),
		Scheme:    "http",
	})
	bucket = NewBucket(client, bucketName)
}

func TestMain(m *testing.M) {
	setup()
	os.Exit(m.Run())
}

// 检查是否应该跳过测试
func checkSkipTest(t *testing.T) {
	if skipTest {
		t.Skip("skipping test in short mode.")
	}
}
