package test

import (
	"bytes"
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"github.com/stretchr/testify/assert"
	"testing"
)

func prepareKey(t *testing.T, key string, data []byte) {
	err := bucket.Put(nil, nil, key, bytes.NewReader(data), int64(len(data)), nil)
	assert.NoError(t, err)
}

func deleteKey(t *testing.T, key string) {
	err := bucket.Delete(nil, key)
	assert.NoError(t, err)
}

func TestBatchStat(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		key  string // 要测试的key
		data []byte // 要测试上传的数据
		put  bool   // 是否需要put
	}

	// 测试用例
	testCases := []TestCase{
		{"key1", []byte{1, 2, 3}, true},
		{"key2", []byte("Hello"), true},
		{"key3", []byte("1"), false},
	}

	// 创建一批文件
	for _, tc := range testCases {
		if tc.put {
			prepareKey(t, tc.key, tc.data)
		}
	}

	// 测试结束后删除文件
	defer func() {
		for _, tc := range testCases {
			if tc.put {
				deleteKey(t, tc.key)
			}
		}
	}()

	// 单独测试每个key
	for _, tc := range testCases {
		stat, err := bucket.Stat(nil, tc.key)
		if tc.put {
			assert.NoError(t, err)
			assert.NotNil(t, stat)
			assert.Equal(t, len(tc.data), int(stat.Fsize))
		} else {
			assert.Error(t, err)
		}
	}

	// 批量测试
	var keys []string
	for _, tc := range testCases {
		keys = append(keys, tc.key)
	}
	rets, err := bucket.BatchStat(nil, keys...)
	assert.NoError(t, err)
	assert.Equal(t, len(testCases), len(rets))

	for i, tc := range testCases {
		if tc.put {
			assert.Empty(t, rets[i].Error)
			assert.Equal(t, len(tc.data), int(rets[i].Data.Fsize))
		} else {
			assert.Equal(t, rets[i].Code, 612)
		}
	}

}

func TestBatchMove(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		key1 string // 原key
		data []byte // 要测试上传的数据
		put  bool   // 是否需要put
		key2 string // 目标key
	}

	// 测试用例
	testCases := []TestCase{
		{"key11", []byte{1, 2, 3}, true, "key21"},
		{"key12", []byte("Hello"), true, "key22"},
		{"key13", []byte("12"), false, "key23"},
	}

	prepare := func() {
		// 创建一批文件
		for _, tc := range testCases {
			if tc.put {
				prepareKey(t, tc.key1, tc.data)
			}
		}
	}

	clear := func() {
		// 测试结束后删除文件
		for _, tc := range testCases {
			if tc.put {
				deleteKey(t, tc.key1)
			}
		}
	}

	// 准备一批文件
	prepare()
	defer clear()

	// 批量测试
	var pairs []kodo.KeyPair
	for _, tc := range testCases {
		pairs = append(pairs, kodo.KeyPair{Src: tc.key1, Dest: tc.key2})
	}

	rets, err := bucket.BatchMove(nil, pairs...)
	assert.NoError(t, err)

	for i, tc := range testCases {
		// 检查移动结果
		if tc.put {
			assert.Empty(t, rets[i].Error)

			// 检查key1文件应该不存在
			stat, err := bucket.Stat(nil, tc.key1)
			assert.Error(t, err)

			// 检查key2文件应该存在
			stat, err = bucket.Stat(nil, tc.key2)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.data), int(stat.Fsize))

			// 移回去
			err = bucket.Move(nil, tc.key2, tc.key1)
		} else {
			assert.Equal(t, rets[i].Code, 612)
		}
	}

}

func TestBatchCopy(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		key1 string // 原key
		data []byte // 要测试上传的数据
		put  bool   // 是否需要put
		key2 string // 目标key
	}

	// 测试用例
	testCases := []TestCase{
		{"key11", []byte{1, 2, 3}, true, "key21"},
		{"key12", []byte("Hello"), true, "key22"},
		{"key13", []byte("12"), false, "key23"},
	}

	prepare := func() {
		// 创建一批文件
		for _, tc := range testCases {
			if tc.put {
				prepareKey(t, tc.key1, tc.data)
			}
		}
	}

	clear := func() {
		// 测试结束后删除文件
		for _, tc := range testCases {
			if tc.put {
				deleteKey(t, tc.key1)
			}
		}
	}

	// 准备一批文件
	prepare()
	defer clear()

	// 批量测试
	var pairs []kodo.KeyPair
	for _, tc := range testCases {
		pairs = append(pairs, kodo.KeyPair{Src: tc.key1, Dest: tc.key2})
	}

	rets, err := bucket.BatchCopy(nil, pairs...)
	assert.NoError(t, err)

	for i, tc := range testCases {
		// 检查复制结果
		if tc.put {
			assert.Empty(t, rets[i].Error)

			// 检查key1文件应该存在
			stat, err := bucket.Stat(nil, tc.key1)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.data), int(stat.Fsize))

			// 检查key2文件应该存在
			stat, err = bucket.Stat(nil, tc.key2)
			assert.NoError(t, err)
			assert.Equal(t, len(tc.data), int(stat.Fsize))

			// 删除key2
			err = bucket.Delete(nil, tc.key2)
		} else {
			assert.Equal(t, rets[i].Code, 612)
		}
	}
}

func TestBatchDelete(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		key1 string // 原key
		data []byte // 要测试上传的数据
		put  bool   // 是否需要put
	}

	// 测试用例
	testCases := []TestCase{
		{"key11", []byte{1, 2, 3}, true},
		{"key12", []byte("Hello"), true},
		{"key13", []byte("12"), false},
	}

	prepare := func() {
		// 创建一批文件
		for _, tc := range testCases {
			if tc.put {
				prepareKey(t, tc.key1, tc.data)
			}
		}
	}

	// 准备一批文件
	prepare()

	// 批量测试
	var keys []string
	for _, tc := range testCases {
		keys = append(keys, tc.key1)
	}

	rets, err := bucket.BatchDelete(nil, keys...)
	assert.NoError(t, err)

	for i, tc := range testCases {
		// 检查删除结果
		if tc.put {
			assert.Equal(t, rets[i].Code, 200)
			assert.Empty(t, rets[i].Error)

			// 检查key1文件应该不存在
			_, err := bucket.Stat(nil, tc.key1)
			assert.Error(t, err)
		} else {
			assert.Equal(t, rets[i].Code, 612)
		}
	}
}

func TestBatch(t *testing.T) {
	checkSkipTest(t)

	key1 := "key1"
	key2 := "key2"
	prepareKey(t, key1, nil)
	defer deleteKey(t, key1)

	var rets []kodo.BatchItemRet
	err := client.Batch(nil, &rets, []string{
		kodo.URICopy(bucketName, key1, bucketName, key2),
		kodo.URIDelete(bucketName, key1),
		kodo.URIMove(bucketName, key2, bucketName, key1),
	})
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rets))
	for i := 0; i < 3; i++ {
		assert.Equal(t, 200, rets[i].Code)

	}

	// 检查key1文件应该存在
	_, err = bucket.Stat(nil, key1)
	assert.NoError(t, err)

	// 检查key2文件应该不存在
	_, err = bucket.Stat(nil, key2)
	assert.Error(t, err)
}
