package operation

import (
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/kodocli"
)

// elog is embedded logger
var elog kodocli.Ilog

// SetLogger 设置全局 Logger
func SetLogger(logger kodocli.Ilog) {
	elog = logger
	kodocli.SetLogger(logger)
}

func init() {
	if elog == nil {
		elog = kodocli.NewLogger()
	}
}
