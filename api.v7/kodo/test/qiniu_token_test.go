package test

import (
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient_MakeUpTokenWithExpires(t *testing.T) {
	checkSkipTest(t)
	token := client.MakeUpTokenWithExpires(&kodo.PutPolicy{
		Scope: bucketName,
	}, 3600)
	assert.NotEmpty(t, token)
}
