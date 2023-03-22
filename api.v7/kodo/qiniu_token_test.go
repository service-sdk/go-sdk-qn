package kodo

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient_MakeUptokenBucket(t *testing.T) {
	c := NewClient(nil)
	token := c.MakeUptoken(&PutPolicy{
		Scope:   "gosdk",
		Expires: 3600,
	})
	assert.NotEmpty(t, token)

	token, err := c.MakeUptokenWithSafe(&PutPolicy{
		Scope:   "NotExistBucket",
		Expires: 3600,
	})
	if err == nil {
		t.Fatal("make up token fail")
	}
}
