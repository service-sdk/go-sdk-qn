package operation

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getMultiClusterListerForTest() *multiClusterLister {
	return newMultiClusterLister(
		&MultiClustersConfig{
			configs: map[string]*Config{
				"/c1": getConfig1(),
				"/c2": getConfig2(),
			},
		},
		10,
	)
}

func Test_multiClusterLister_canTransfer(t *testing.T) {
	lister := getMultiClusterListerForTest()

	// fromKey找不到, toKey能找到
	fromKey, toKey := "/c3/1", "/c2/2"
	_, e := lister.config.forKey(fromKey)
	assert.False(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err := lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrUndefinedConfig, err)

	// fromKey能找到, toKey找不到
	fromKey, toKey = "/c1/1", "/c3/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.False(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrUndefinedConfig, err)

	// fromKey能找到, toKey能找到, 但是fromKey和toKey不在同一个集群
	fromKey, toKey = "/c1/1", "/c2/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrCannotTransferBetweenDifferentClusters, err)

	// fromKey能找到, toKey能找到, 且fromKey和toKey在同一个集群
	fromKey, toKey = "/c1/1", "/c1/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.NoError(t, err)
}
