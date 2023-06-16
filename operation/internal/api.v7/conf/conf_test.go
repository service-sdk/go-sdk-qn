package conf

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/x/rpc.v7"
)

var noErrorAppNames = []string{
	"",
	"Test0-_.",
}
var errorAppNames = []string{
	"错误的UA",
}

func TestSetAppName(t *testing.T) {
	for _, appName := range noErrorAppNames {
		assert.NoError(t, SetAppName(appName))
	}

	for _, appName := range errorAppNames {
		assert.Error(t, SetAppName(appName))
	}

	appName := "tesT0.-_"
	assert.NoError(t, SetAppName(appName))
	v := rpc.UserAgent
	assert.True(t, strings.Contains(v, appName))
	assert.True(t, strings.HasPrefix(v, "QiniuGo/"+version))
}
