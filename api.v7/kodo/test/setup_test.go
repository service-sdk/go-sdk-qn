package test

import (
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"log"
	"os"
	"strings"
	"testing"
)

var (
	skipTest   = true
	bucketName string
	domain     string
	client     *kodo.Client
	bucket     *kodo.Bucket
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
	client = kodo.NewClient(&kodo.Config{
		AccessKey: ak,
		SecretKey: sk,
		UpHosts:   strings.Split(os.Getenv("QINIU_TEST_UP_HOSTS"), ","),
		RSFHost:   os.Getenv("QINIU_TEST_RSF_HOST"),
		RSHost:    os.Getenv("QINIU_TEST_RS_HOST"),
		APIHost:   os.Getenv("QINIU_TEST_API_HOST"),
		IoHost:    os.Getenv("QINIU_TEST_IO_HOST"),
		Scheme:    "http",
	})
	bucket = kodo.NewBucket(client, bucketName)
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
