package test

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestListPrefix(t *testing.T) {
	checkSkipTest(t)

	result := lister.ListPrefix("")
	_, err := lister.DeleteKeys(result)
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test2"), "test2")
	defer lister.Delete("test2")
	assert.NoError(t, err)

	result = lister.ListPrefix("")
	assert.Contains(t, result, "test1")
	assert.Contains(t, result, "test2")
}

func TestLister_Rename(t *testing.T) {
	checkSkipTest(t)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 重命名文件 test1 -> test2
	err = lister.Rename("test1", "test2")
	defer lister.Delete("test2")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")

	// 测试文件 test2 应当存在
	assert.Contains(t, result, "test2")

}

func TestLister_MoveTo(t *testing.T) {
	checkSkipTest(t)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 移动文件 test1 -> test2
	err = lister.MoveTo("test1", bucketName(), "test2")
	defer lister.Delete("test2")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")

	// 测试文件 test2 应当存在
	assert.Contains(t, result, "test2")
}

func TestLister_Copy(t *testing.T) {
	checkSkipTest(t)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 复制文件 test1 -> test2
	err = lister.Copy("test1", "test2")
	defer lister.Delete("test2")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当存在
	assert.Contains(t, result, "test1")

	// 测试文件 test2 应当存在
	assert.Contains(t, result, "test2")
}

func TestLister_Delete(t *testing.T) {
	checkSkipTest(t)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当存在
	assert.Contains(t, result, "test1")

	// 删除文件 test1
	err = lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result = lister.ListPrefix("")

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")
}

func TestLister_ForceDelete(t *testing.T) {
	checkSkipTest(t)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当存在
	assert.Contains(t, result, "test1")

	// 删除文件 test1
	err = lister.Delete("test1")
	assert.NoError(t, err)

	// 列举出所有文件
	result = lister.ListPrefix("")

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")
}

func TestLister_ListStat(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	// 提取keys
	keys := make([]string, len(testCases))
	for i, _ := range testCases {
		keys[i] = testCases[i].name
	}

	defer lister.DeleteKeys(keys)

	// 列举出所有文件的stat
	fileStats := lister.ListStat(keys)

	for i, stat := range fileStats {
		assert.Equal(t, testCases[i].name, stat.Name)
		assert.Equal(t, int64(len(testCases[i].content)), stat.Size)
	}

}

func TestLister_DeleteKeys(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	result := lister.ListPrefix("")

	// 提取keys，并验证每个key是否存在于result中
	keys := make([]string, len(testCases))
	for i, tc := range testCases {
		keys[i] = tc.name
		assert.Contains(t, result, tc.name)
	}

	// 批量删除
	lister.DeleteKeys(keys)

	// 删除结束后每个key都不存在result中了
	result = lister.ListPrefix("")

	for _, key := range keys {
		assert.NotContains(t, result, key)
	}
}

func TestLister_ForceDeleteKeys(t *testing.T) {
	checkSkipTest(t)

	type TestCase struct {
		name    string
		content []byte
	}
	testCases := []TestCase{
		{name: "test1", content: []byte{1, 2, 3}},
		{name: "test2", content: []byte("test123")},
		{name: "test3", content: []byte("123")},
	}

	for _, tc := range testCases {
		err := uploader.UploadData(tc.content, tc.name)
		assert.NoError(t, err)
	}

	result := lister.ListPrefix("")

	// 提取keys，并验证每个key是否存在于result中
	keys := make([]string, len(testCases))
	for i, tc := range testCases {
		keys[i] = tc.name
		assert.Contains(t, result, tc.name)
	}

	// 批量删除
	_, err := lister.ForceDeleteKeys(keys)
	assert.NoError(t, err)

	// 删除结束后每个key都不存在result中了
	result = lister.ListPrefix("")

	for _, key := range keys {
		assert.NotContains(t, result, key)
	}
}

func TestLister_RenameDirectory(t *testing.T) {
	checkSkipTest(t)

}

func TestLister_MoveDirectoryTo(t *testing.T) {
	checkSkipTest(t)

}
func TestLister_CopyDirectory(t *testing.T) {
	checkSkipTest(t)

}

func TestLister_DeleteDirectory(t *testing.T) {
	checkSkipTest(t)

}

func TestLister_ForceDeleteDirectory(t *testing.T) {
	checkSkipTest(t)

}