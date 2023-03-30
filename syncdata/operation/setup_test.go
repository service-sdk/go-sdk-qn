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

func getUpHosts() []string {
	return strings.Split(os.Getenv("QINIU_TEST_UP_HOSTS"), ",")
}

func getRsfHosts() []string {
	return append(
		strings.Split(os.Getenv("QINIU_TEST_RSF_HOSTS"), ","),
		os.Getenv("QINIU_TEST_RSF_HOST"),
	)
}

func getRsHosts() []string {
	return append(
		strings.Split(os.Getenv("QINIU_TEST_RS_HOSTS"), ","),
		os.Getenv("QINIU_TEST_RS_HOST"),
	)
}

func getApiServerHosts() []string {
	return append(
		strings.Split(os.Getenv("QINIU_TEST_API_SERVER_HOSTS"), ","),
		os.Getenv("QINIU_TEST_API_SERVER_HOST"),
	)
}

func getIoHosts() []string {
	return append(
		strings.Split(os.Getenv("QINIU_TEST_IO_HOSTS"), ","),
		os.Getenv("QINIU_TEST_IO_HOST"),
	)
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

		UpHosts: getUpHosts(), RsfHosts: getRsfHosts(), RsHosts: getRsHosts(),
		ApiServerHosts: getApiServerHosts(), IoHosts: getIoHosts(), UcHosts: getUcHosts(),

		Bucket: getBucketWithN(1),
		//RecycleBin: "recycle",
		BatchSize: 500,
	}
}

func getConfig2() *Config {
	return &Config{
		Ak: getAccessKey(), Sk: getSecretKey(),

		UpHosts: getUpHosts(), RsfHosts: getRsfHosts(), RsHosts: getRsHosts(),
		ApiServerHosts: getApiServerHosts(), IoHosts: getIoHosts(), UcHosts: getUcHosts(),

		Bucket: getBucketWithN(2),
		//RecycleBin: "recycle",
		BatchSize: 500,
	}
}

func getSingleClusterConfigurable() Configurable {
	return getConfig1()
}

func getMultiClusterConfigurable() Configurable {
	return &MultiClustersConfig{}
}

// 检查是否应该跳过测试
func checkSkipTest(t *testing.T) {
	if os.Getenv("QINIU_KODO_TEST") == "" {
		t.Skip("skipping test in short mode.")
	}
}
