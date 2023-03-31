package operation

import (
	"context"
	"github.com/stretchr/testify/assert"
	"sort"
	"strings"
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

func Test_multiClusterLister_copy(t *testing.T) {
	lister := getMultiClusterListerForTest()
	// 无法根据key找到对应的cluster config
	// 需要返回一个错误
	err := lister.copy("c3/1", "c3/2")
	assert.Equal(t, ErrUndefinedConfig, err)

	// 能够根据key找到对应的cluster config
	// 需要正确复制
	uploader := newSingleClusterUploader(getConfig1())
	assert.NoError(t, uploader.uploadData(nil, "c1/1"))
	err = lister.copy("c1/1", "c1/2")
	assert.NoError(t, err)

	res, err := lister.listPrefix(context.Background(), "c1/")
	assert.NoError(t, err)
	assert.Equal(t, []string{"c1/1", "c1/2"}, res)

	assert.NoError(t, lister.delete("c1/1", true))
	assert.NoError(t, lister.delete("c1/2", true))
}

func Test_multiCluster_moveTo(t *testing.T) {
	lister := getMultiClusterListerForTest()
	// 无法根据key找到对应的cluster config
	// 需要返回一个错误
	err := lister.moveTo("c3/1", "go-ipfs-sdk-3", "c3/2")
	assert.Equal(t, ErrUndefinedConfig, err)

	// 能够根据key找到对应的cluster config
	// 需要正确移动
	uploader := newSingleClusterUploader(getConfig1())
	assert.NoError(t, uploader.uploadData(nil, "c1/1"))

	err = lister.moveTo("c1/1", getConfig1().Bucket, "c1/2")
	assert.NoError(t, err)

	res, err := lister.listPrefix(context.Background(), "c1/")
	assert.NoError(t, err)
	assert.Equal(t, []string{"c1/2"}, res)

	assert.NoError(t, lister.delete("c1/2", true))
}

func Test_multiClusterLister_rename(t *testing.T) {
	lister := getMultiClusterListerForTest()
	// 无法根据key找到对应的cluster config
	// 需要返回一个错误
	err := lister.rename("c3/1", "c3/2")
	assert.Equal(t, ErrUndefinedConfig, err)

	// 能够根据key找到对应的cluster config
	// 需要正确重命名
	uploader := newSingleClusterUploader(getConfig1())
	assert.NoError(t, uploader.uploadData(nil, "c1/1"))

	err = lister.rename("c1/1", "c1/2")
	assert.NoError(t, err)

	res, err := lister.listPrefix(context.Background(), "c1/")
	assert.NoError(t, err)
	assert.Equal(t, []string{"c1/2"}, res)

	assert.NoError(t, lister.delete("c1/2", true))
}

func Test_multiClusterLister_deleteKeys(t *testing.T) {
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
	errs, err := lister.deleteKeys(context.Background(), allKeys, true)
	assert.NoError(t, err)
	assert.Equal(t, len(allKeys), len(errs))
	for _, e := range errs {
		assert.Nil(t, e)
	}

	// 再次列举所有的key应当为空key
	items, err := lister.listPrefix(context.Background(), "c")
	assert.NoError(t, err)
	assert.Empty(t, items)

	clearMultiClusterBuckets(t)
}

func Test_multiClusterLister_copyKeys(t *testing.T) {
	t.Run("copy keys between same clusters", func(t *testing.T) {
		clearMultiClusterBuckets(t)

		cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
		sort.Strings(cluster1Keys)

		uploader1 := newSingleClusterUploader(getConfig1())
		for _, key := range cluster1Keys {
			assert.NoError(t, uploader1.uploadData(nil, key))
		}

		// 开始复制
		var copyInputs []CopyKeyInput
		for _, key := range cluster1Keys {
			copyInputs = append(copyInputs, CopyKeyInput{
				FromKey: key,
				ToKey:   key + "-copy",
			})
		}
		lister := getMultiClusterListerForTest()
		errs, err := lister.copyKeys(context.Background(), copyInputs)
		assert.NoError(t, err)
		assert.Equal(t, len(cluster1Keys), len(errs))
		for _, e := range errs {
			assert.Nil(t, e)
		}

		items, err := lister.listPrefix(context.Background(), "c1/")
		assert.NoError(t, err)
		assert.Equal(t, len(cluster1Keys)*2, len(items))
	})
	t.Run("copy keys between different clusters", func(t *testing.T) {
		clearMultiClusterBuckets(t)
		cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
		uploader1 := newSingleClusterUploader(getConfig1())
		for _, key := range cluster1Keys {
			assert.NoError(t, uploader1.uploadData(nil, key))
		}

		// 开始复制
		var copyInputs []CopyKeyInput
		for _, key := range cluster1Keys {
			copyInputs = append(copyInputs, CopyKeyInput{
				FromKey: key,
				ToKey:   strings.Replace(key, "c1/", "c2/", 1),
			})
		}
		lister := getMultiClusterListerForTest()
		_, err := lister.copyKeys(context.Background(), copyInputs)
		assert.Error(t, err)

		items, err := lister.listPrefix(context.Background(), "c2/")
		assert.NoError(t, err)
		assert.Empty(t, items)
	})
	t.Run("copy keys between partial same clusters", func(t *testing.T) {
		clearMultiClusterBuckets(t)
		copyInputs := []CopyKeyInput{
			{"c1/1", "c1/1-copy"},
			{"c2/1", "c2/1-copy"},
			{"c3/1", "c3/1-copy"},
		}

		uploader1 := newSingleClusterUploader(getConfig1())
		for _, input := range copyInputs {
			assert.NoError(t, uploader1.uploadData(nil, input.FromKey))
		}

		// 开始复制
		lister := getMultiClusterListerForTest()
		_, err := lister.copyKeys(context.Background(), copyInputs)
		assert.Error(t, err)

		// c1, c2 全都不应该成功
		items, err := lister.listPrefix(context.Background(), "")
		assert.NoError(t, err)
		for _, input := range copyInputs {
			assert.Contains(t, items, input.FromKey)
			assert.NotContains(t, items, input.ToKey)
		}
	})
}

func Test_multiClusterLister_deleteKeysFromChannel(t *testing.T) {
	cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
	cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
	allKeys := append(cluster1Keys, cluster2Keys...)

	clearMultiClusterBuckets(t)

	uploader1 := newSingleClusterUploader(getConfig1())
	for _, key := range cluster1Keys {
		assert.NoError(t, uploader1.uploadData(nil, key))
	}

	uploader2 := newSingleClusterUploader(getConfig2())
	for _, key := range cluster2Keys {
		assert.NoError(t, uploader2.uploadData(nil, key))
	}

	ch := make(chan string, 10)
	go func() {
		for _, key := range allKeys {
			ch <- key
		}
		close(ch)
	}()
	lister := getMultiClusterListerForTest()

	assert.NoError(t, lister.deleteKeysFromChannel(context.Background(), ch, true, func() chan<- DeleteKeysError {
		errCh := make(chan DeleteKeysError, 10)
		go func() {
			t.Error(<-errCh)
		}()
		return errCh
	}()))
	// 再次列举所有的key应当为空key
	items, err := lister.listPrefix(context.Background(), "c")
	assert.NoError(t, err)
	assert.Empty(t, items)
}

func Test_multiClusterLister_copyKeysFromChannel(t *testing.T) {
	t.Run("copy keys between same clusters", func(t *testing.T) {
		cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
		cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
		allKeys := append(cluster1Keys, cluster2Keys...)

		clearMultiClusterBuckets(t)

		uploader1 := newSingleClusterUploader(getConfig1())
		for _, key := range cluster1Keys {
			assert.NoError(t, uploader1.uploadData(nil, key))
		}

		uploader2 := newSingleClusterUploader(getConfig2())
		for _, key := range cluster2Keys {
			assert.NoError(t, uploader2.uploadData(nil, key))
		}

		ch := make(chan CopyKeyInput, 10)
		go func() {
			defer close(ch)
			for _, key := range allKeys {
				ch <- CopyKeyInput{
					FromKey: key,
					ToKey:   key + "-copy",
				}
			}
		}()
		lister := getMultiClusterListerForTest()

		assert.NoError(t, lister.copyKeysFromChannel(context.Background(), ch, func() chan<- CopyKeysError {
			errCh := make(chan CopyKeysError, 10)
			go func() {
				t.Error(<-errCh)
			}()
			return errCh
		}()))
		// 再次列举所有的key应当全部复制成功
		items, err := lister.listPrefix(context.Background(), "c")
		assert.NoError(t, err)
		assert.Equal(t, len(allKeys)*2, len(items))
	})
}

func Test_multiClusterLister_moveKeysFromChannel(t *testing.T) {
	t.Run("move keys between same clusters", func(t *testing.T) {
		cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
		cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
		allKeys := append(cluster1Keys, cluster2Keys...)

		clearMultiClusterBuckets(t)

		uploader1 := newSingleClusterUploader(getConfig1())
		for _, key := range cluster1Keys {
			assert.NoError(t, uploader1.uploadData(nil, key))
		}

		uploader2 := newSingleClusterUploader(getConfig2())
		for _, key := range cluster2Keys {
			assert.NoError(t, uploader2.uploadData(nil, key))
		}

		ch := make(chan MoveKeyInput, 10)
		go func() {
			defer close(ch)
			for _, key := range allKeys {
				ch <- MoveKeyInput{
					FromToKey: FromToKey{
						FromKey: key,
						ToKey:   key + "-move",
					},
					ToBucket: getConfig1().Bucket,
				}
			}
		}()
		lister := getMultiClusterListerForTest()

		assert.NoError(t, lister.moveKeysFromChannel(context.Background(), ch, func() chan<- MoveKeysError {
			errCh := make(chan MoveKeysError, 10)
			go func() {
				t.Error(<-errCh)
			}()
			return errCh
		}()))
		// 再次列举所有的key应当全部移动成功
		items, err := lister.listPrefix(context.Background(), "c")
		assert.NoError(t, err)
		assert.Equal(t, len(allKeys), len(items))
		for _, item := range items {
			assert.True(t, strings.HasSuffix(item, "-move"))
		}
	})
}

func Test_multiClusterLister_renameKeysFromChannel(t *testing.T) {
	t.Run("rename keys between same clusters", func(t *testing.T) {
		cluster1Keys := []string{"c1/1", "c1/2", "c1/3", "c1/4", "c1/5", "c1/6"}
		cluster2Keys := []string{"c2/1", "c2/2", "c2/3", "c2/4", "c2/5", "c2/6"}
		allKeys := append(cluster1Keys, cluster2Keys...)

		clearMultiClusterBuckets(t)

		uploader1 := newSingleClusterUploader(getConfig1())
		for _, key := range cluster1Keys {
			assert.NoError(t, uploader1.uploadData(nil, key))
		}

		uploader2 := newSingleClusterUploader(getConfig2())
		for _, key := range cluster2Keys {
			assert.NoError(t, uploader2.uploadData(nil, key))
		}

		ch := make(chan RenameKeyInput, 10)
		go func() {
			defer close(ch)
			for _, key := range allKeys {
				ch <- RenameKeyInput{
					FromKey: key,
					ToKey:   key + "-rename",
				}
			}
		}()
		lister := getMultiClusterListerForTest()

		assert.NoError(t, lister.renameKeysFromChannel(context.Background(), ch, func() chan<- RenameKeysError {
			errCh := make(chan RenameKeysError, 10)
			go func() {
				t.Error(<-errCh)
			}()
			return errCh
		}()))
		// 再次列举所有的key应当全部重命名成功
		items, err := lister.listPrefix(context.Background(), "c")
		assert.NoError(t, err)
		assert.Equal(t, len(allKeys), len(items))
		for _, item := range items {
			assert.True(t, strings.HasSuffix(item, "-rename"))
		}
	})
}
