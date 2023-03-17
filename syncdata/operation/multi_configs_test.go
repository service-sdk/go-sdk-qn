package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMultuClustersConfigDefaultSelectConfigCallbackFunc(t *testing.T) {
	mCfg := MultiClustersConfig{
		configs: map[string]*Config{
			"/node/1":   {Ak: "ak-1"},
			"/node/12":  {Ak: "ak-2"},
			"/node/123": {Ak: "ak-3"},
		},
	}

	c, ok := mCfg.defaultSelectConfigCallbackFunc("/node/12")
	assert.True(t, ok)
	assert.Equal(t, c.Ak, "ak-2")

	c, ok = mCfg.defaultSelectConfigCallbackFunc("/node/12/file")
	assert.True(t, ok)
	assert.Equal(t, c.Ak, "ak-2")

	c, ok = mCfg.defaultSelectConfigCallbackFunc("/node/1")
	assert.True(t, ok)
	assert.Equal(t, c.Ak, "ak-1")

	c, ok = mCfg.defaultSelectConfigCallbackFunc("/node/1/file")
	assert.True(t, ok)
	assert.Equal(t, c.Ak, "ak-1")

	_, ok = mCfg.defaultSelectConfigCallbackFunc("/node/1234")
	assert.False(t, ok)
}
