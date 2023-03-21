package conf

import (
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"

	"github.com/service-sdk/go-sdk-qn/x/rpc.v7"
)

var noErrorAppNames = []string{
	"",
	"Test0-_.",
}
var errorAppNames = []string{
	"错误的UA",
}

func TestSetAppName(t *testing.T) {
	client := &rpc.Client{}
	for _, appName := range noErrorAppNames {
		assert.NoError(t, SetAppName(appName, client))
	}

	for _, appName := range errorAppNames {
		assert.Error(t, SetAppName(appName, client))
	}
}

func TestSetDefaultAppName(t *testing.T) {
	client := &rpc.Client{}
	for _, appName := range noErrorAppNames {
		assert.NoError(t, SetAppName(appName, client))
	}

	for _, appName := range errorAppNames {
		assert.Error(t, SetAppName(appName, client))
	}

	appName := "tesT0.-_"
	assert.NoError(t, SetDefaultAppName(appName))
	v := rpc.DefaultUserAgent
	assert.True(t, strings.Contains(v, appName))
	assert.True(t, strings.HasPrefix(v, "QiniuGo/"+version))
}
