package operation

import (
	"os"
	"strings"
	"testing"
)

var (
	skipTest = true
	config   *Config
	lister   *Lister
	uploader *Uploader
)

func setup() {
	skipTest = os.Getenv("QINIU_KODO_TEST") == ""
	config = &Config{
		Ak:      os.Getenv("QINIU_ACCESS_KEY"),
		Sk:      os.Getenv("QINIU_SECRET_KEY"),
		UpHosts: strings.Split(os.Getenv("QINIU_TEST_UP_HOSTS"), ","),
		RsfHosts: append(
			strings.Split(os.Getenv("QINIU_TEST_RSF_HOSTS"), ","),
			os.Getenv("QINIU_TEST_RSF_HOST"),
		),
		RsHosts: append(
			strings.Split(os.Getenv("QINIU_TEST_RS_HOSTS"), ","),
			os.Getenv("QINIU_TEST_RS_HOST"),
		),
		ApiServerHosts: append(
			strings.Split(os.Getenv("QINIU_TEST_API_HOSTS"), ","),
			os.Getenv("QINIU_TEST_API_HOST"),
		),
		IoHosts: append(
			strings.Split(os.Getenv("QINIU_TEST_IO_HOSTS"), ","),
			os.Getenv("QINIU_TEST_IO_HOST"),
		),
		UcHosts: append(
			strings.Split(os.Getenv("QINIU_TEST_UC_HOSTS"), ","),
			os.Getenv("QINIU_TEST_UC_HOST"),
		),
		Bucket: os.Getenv("QINIU_TEST_BUCKET"),
		//RecycleBin: "recycle",
		BatchSize: 500,
	}
	lister = NewLister(config)
	uploader = NewUploader(config)
}

func TestMain(t *testing.M) {
	setup()
	os.Exit(t.Run())
}

// 检查是否应该跳过测试
func checkSkipTest(t *testing.T) {
	if skipTest {
		t.Skip("skipping test in short mode.")
	}
}

func bucketName() string {
	return config.Bucket
}
