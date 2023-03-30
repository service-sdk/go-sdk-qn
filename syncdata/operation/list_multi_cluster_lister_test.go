package operation

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func getMultiClusterListerForTest() *multiClusterLister {
	return newMultiClusterLister(
		&MultiClustersConfig{
			configs: map[string]*Config{
				"c1": getConfig1(),
				"c2": getConfig2(),
			},
		},
		10,
	)
}

func clearMultiClusterBuckets(t *testing.T) {
	checkSkipTest(t)
	clearBucket(t, newSingleClusterLister(getConfig1()))
	clearBucket(t, newSingleClusterLister(getConfig2()))
}

func Test_multiClusterLister_canTransfer(t *testing.T) {
	lister := getMultiClusterListerForTest()

	// fromKey找不到, toKey能找到
	fromKey, toKey := "c3/1", "c2/2"
	_, e := lister.config.forKey(fromKey)
	assert.False(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err := lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrUndefinedConfig, err)

	// fromKey能找到, toKey找不到
	fromKey, toKey = "c1/1", "c3/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.False(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrUndefinedConfig, err)

	// fromKey能找到, toKey能找到, 但是fromKey和toKey不在同一个集群
	fromKey, toKey = "c1/1", "c2/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.Equal(t, ErrCannotTransferBetweenDifferentClusters, err)

	// fromKey能找到, toKey能找到, 且fromKey和toKey在同一个集群
	fromKey, toKey = "c1/1", "c1/2"
	_, e = lister.config.forKey(fromKey)
	assert.True(t, e)
	_, e = lister.config.forKey(toKey)
	assert.True(t, e)

	_, err = lister.canTransfer(fromKey, toKey)
	assert.NoError(t, err)
}

func Test_multiClusterLister_groupBy(t *testing.T) {
	lister := getMultiClusterListerForTest()
	keysGroup1 := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
	keysGroup2 := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
	keys := append(keysGroup1, keysGroup2...)

	errKeyGroup := []string{"c3/1", "c3/2", "c3/3", "c3/4", "c3/5", "c3/6"}

	// 不存在的key，需要报错
	_, err := lister.groupBy(errKeyGroup)
	assert.Equal(t, ErrUndefinedConfig, err)

	// 存在的key，不需要报错
	groups, err := lister.groupBy(keys)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(groups))
}

func Test_multiClusterLister_listStat(t *testing.T) {
	cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
	cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
	allKeys := append(cluster1Keys, cluster2Keys...)

	uploader1 := newSingleClusterUploader(getConfig1())
	for _, key := range cluster1Keys {
		assert.NoError(t, uploader1.uploadData(nil, key))
	}

	uploader2 := newSingleClusterUploader(getConfig2())
	for _, key := range cluster2Keys {
		assert.NoError(t, uploader2.uploadData(nil, key))
	}

	lister := getMultiClusterListerForTest()
	stats, err := lister.listStat(context.Background(), allKeys)
	assert.NoError(t, err)
	assert.Equal(t, len(allKeys), len(stats))

	for i, stat := range stats {
		assert.Equal(t, int64(0), stat.Size)
		assert.Equal(t, 200, stat.code)
		assert.Equal(t, allKeys[i], stat.Name)
	}

	clearMultiClusterBuckets(t)
}

func Test_multiClusterLister_listPrefix(t *testing.T) {
	cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
	sort.Strings(cluster1Keys)

	cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
	sort.Strings(cluster2Keys)

	allKeys := append(cluster1Keys, cluster2Keys...)
	sort.Strings(allKeys)

	uploader1 := newSingleClusterUploader(getConfig1())
	for _, key := range cluster1Keys {
		assert.NoError(t, uploader1.uploadData(nil, key))
	}

	uploader2 := newSingleClusterUploader(getConfig2())
	for _, key := range cluster2Keys {
		assert.NoError(t, uploader2.uploadData(nil, key))
	}

	lister := getMultiClusterListerForTest()
	items, err := lister.listPrefix(context.Background(), "c1/")
	assert.NoError(t, err)
	assert.Equal(t, len(cluster1Keys), len(items))
	assert.Equal(t, cluster1Keys, items)

	items, err = lister.listPrefix(context.Background(), "c2/")
	assert.NoError(t, err)
	assert.Equal(t, len(cluster2Keys), len(items))
	assert.Equal(t, cluster2Keys, items)

	items, err = lister.listPrefix(context.Background(), "c")
	assert.NoError(t, err)
	assert.Equal(t, len(allKeys), len(items))
	assert.Equal(t, allKeys, items)

	clearMultiClusterBuckets(t)
}

func Test_multiClusterLister_delete(t *testing.T) {
	lister := getMultiClusterListerForTest()
	// 无法根据key找到对应的cluster config
	// 需要返回一个错误
	err := lister.delete("c3/1", true)
	assert.Equal(t, ErrUndefinedConfig, err)

	// 能够根据key找到对应的cluster config
	// 需要正确删除
	uploader := newSingleClusterUploader(getConfig1())
	assert.NoError(t, uploader.uploadData(nil, "c1/1"))
	assert.NoError(t, uploader.uploadData(nil, "c1/2"))
	assert.NoError(t, uploader.uploadData(nil, "c1/3"))

	err = lister.delete("c1/1", true)
	assert.NoError(t, err)
	err = lister.delete("c1/2", true)
	assert.NoError(t, err)
	err = lister.delete("c1/3", true)
	assert.NoError(t, err)
}
