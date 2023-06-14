package operation

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func getAccessKey() string {
	return os.Getenv("QINIU_ACCESS_KEY")
}

func getSecretKey() string {
	return os.Getenv("QINIU_SECRET_KEY")
}

func getUcHosts() []string {
	return append(
		strings.Split(os.Getenv("QINIU_TEST_UC_HOSTS"), ","),
		os.Getenv("QINIU_TEST_UC_HOST"),
	)
}

func getBucket() string {
	return os.Getenv("QINIU_TEST_BUCKET")
}

func getBucketWithN(n int) string {
	return fmt.Sprintf("%s-%d", getBucket(), n)
}

func getConfig1() *Config {
	return &Config{
		Ak: getAccessKey(), Sk: getSecretKey(),
		UcHosts:   getUcHosts(),
		Bucket:    getBucketWithN(1),
		BatchSize: 500,
	}
}

func getConfig2() *Config {
	return &Config{
		Ak: getAccessKey(), Sk: getSecretKey(),
		UcHosts:   getUcHosts(),
		Bucket:    getBucketWithN(2),
		BatchSize: 500,
	}
}

// 检查是否应该跳过测试
func checkSkipTest(t *testing.T) {
	if os.Getenv("QINIU_KODO_TEST") == "" {
		t.Skip("skipping test in short mode.")
	}
}
