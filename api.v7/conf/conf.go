package conf

import (
	"fmt"
	"runtime"
	"syscall"

	"github.com/service-sdk/go-sdk-qn/x/ctype.v7"
	"github.com/service-sdk/go-sdk-qn/x/rpc.v7"
)

const (
	version = "1.4.0"
)

// 全局的ak，sk
var (
	AccessKey string
	SecretKey string
)

// checkAppName userAppName should be [A-Za-z0-9_\ \-\.]*
func checkAppName(appName string) error {
	const ctypeAppName = ctype.ALPHA | ctype.DIGIT | ctype.UNDERLINE | ctype.SPACE_BAR | ctype.SUB | ctype.DOT
	if appName != "" && !ctype.IsType(ctypeAppName, appName) {
		return syscall.EINVAL
	}
	return nil
}

func uaString(userAppName string) string {
	return fmt.Sprintf(
		"QiniuGo/%s (%s; %s; %s) %s",
		version,
		runtime.GOOS,
		runtime.GOARCH,
		userAppName,
		runtime.Version(),
	)
}

// SetAppName userAppName should be [A-Za-z0-9_\ \-\.]*
func SetAppName(userAppName string) error {
	if err := checkAppName(userAppName); err != nil {
		return err
	}
	rpc.UserAgent = uaString(userAppName)
	return nil
}

func init() {
	_ = SetAppName("")
}
