package kodo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient_MakeUpTokenWithExpires(t *testing.T) {
	checkSkipTest(t)
	token := client.MakeUpTokenWithExpires(&PutPolicy{
		Scope: bucketName,
	}, 3600)
	assert.NotEmpty(t, token)
}
