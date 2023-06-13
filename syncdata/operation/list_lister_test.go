package operation

import (
	"context"
	"fmt"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

func getClearedListerForTest(t *testing.T) *Lister {
	return &Lister{getClearedSingleClusterListerForTest(t)}
}

func TestListPrefix(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	result := lister.ListPrefix("")
	_, err := lister.DeleteKeys(result)
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test1"), "test1")
	assert.NoError(t, err)

	err = uploader.UploadData([]byte("test2"), "test2")
	assert.NoError(t, err)

	result = lister.ListPrefix("")
	assert.Contains(t, result, "test1")
	assert.Contains(t, result, "test2")
}

func TestLister_Rename(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 重命名文件 test1 -> test2
	err = lister.Rename("test1", "test2")
	defer lister.Delete("test2")
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	// 列举出所有文件
	result := lister.ListPrefix("")
	assert.NotEmpty(t, result)

	// 测试文件 test1 应当不存在
	assert.NotContains(t, result, "test1")

	// 测试文件 test2 应当存在
	assert.Contains(t, result, "test2")

}

func TestLister_MoveTo(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	// 创建文件 test1
	err := uploader.UploadData([]byte("test1"), "test1")
	defer lister.Delete("test1")
	assert.NoError(t, err)

	// 移动文件 test1 -> test2
	err = lister.MoveTo("test1", config.Bucket, "test2")
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
	lister := getClearedListerForTest(t)
	uploader := NewUploader(getConfig1())

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

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
	lister := getClearedListerForTest(t)
	config := getConfig1()

	uploader := NewUploader(config)

	keys := []string{
		"test11", "test12", "test13",
		"test1/test1", "test1/test2", "test1/test3",
		"test2/test1", "test2/test2", "test2/test3",
	}
	// 创建目录与目录文件
	for _, key := range keys {
		err := uploader.UploadData([]byte("test"), key)
		assert.NoError(t, err)
	}

	// 移动目录
	copyErrors, err := lister.RenameDirectory("test1", "test3")
	assert.NoError(t, err)
	assert.Empty(t, copyErrors)

	// 列举出所有文件
	result := lister.ListPrefix("")
	fmt.Println(result)
}

func TestLister_CopyDirectory(t *testing.T) {
	lister := getClearedListerForTest(t)
	config := getConfig1()
	uploader := NewUploader(config)

	keys := []string{
		"test11", "test12", "test13",
		"test1/test1", "test1/test2", "test1/test3",
		"test2/test1", "test2/test2", "test2/test3",
	}
	// 创建目录与目录文件
	for _, key := range keys {
		err := uploader.UploadData([]byte("test"), key)
		assert.NoError(t, err)
	}

	// 复制目录
	copyErrors, err := lister.CopyDirectory("test1", "test3")
	assert.NoError(t, err)
	assert.Empty(t, copyErrors)

	// 列举出所有文件
	result := lister.ListPrefix("")
	fmt.Println(result)

}

func TestLister_DeleteDirectory(t *testing.T) {
	lister := getClearedListerForTest(t)
	uploader := NewUploader(getConfig1())
	files := []string{
		"dir1/file1", "dir1/file2", "dir1/file3",
		"dir2/file1", "dir2/file2", "dir2/file3",
	}
	for _, file := range files {
		assert.NoError(t, uploader.uploadData(nil, file))
	}

	// 删除目录
	errs, err := lister.DeleteDirectory("dir1")
	assert.NoError(t, err)
	assert.Empty(t, errs)

	// 列举出所有文件
	result := lister.ListPrefix("")
	// 全都是dir2/前缀的文件
	for _, file := range result {
		assert.True(t, strings.HasPrefix(file, "dir2/"))
	}
}
func TestLister_MoveDirectoryTo(t *testing.T) {
	lister := getClearedListerForTest(t)

	makeLotsFilesWithPrefix(t, 2000, 100, "dir1/")

	// 移动目录
	errs, err := lister.MoveDirectoryTo("dir1", "dir2", getConfig1().Bucket)
	assert.NoError(t, err)
	assert.Empty(t, errs)

	// 列举出所有文件
	result := lister.ListPrefix("")
	// 全都是dir2/前缀的文件
	for _, file := range result {
		assert.True(t, strings.HasPrefix(file, "dir2/"))
	}
}

func TestLister_ForceDeleteDirectory(t *testing.T) {
	lister := getClearedListerForTest(t)
	uploader := NewUploader(getConfig1())
	files := []string{
		"dir1/file1", "dir1/file2", "dir1/file3",
		"dir2/file1", "dir2/file2", "dir2/file3",
	}
	for _, file := range files {
		assert.NoError(t, uploader.uploadData(nil, file))
	}

	// 删除目录
	errs, err := lister.ForceDeleteDirectory("dir1")
	assert.NoError(t, err)
	assert.Empty(t, errs)

	// 列举出所有文件
	result := lister.ListPrefix("")
	// 全都是dir2/前缀的文件
	for _, file := range result {
		assert.True(t, strings.HasPrefix(file, "dir2/"))
	}
}

func makeLotsFilesWithPrefix(t *testing.T, files uint, batchConcurrency int, prefix string) (paths []string) {
	config := getConfig1()
	uploader := NewUploader(config)

	pool := goroutine_pool.NewGoroutinePool(batchConcurrency)
	for i := uint(0); i < files; i++ {
		func(id uint) {
			p := fmt.Sprintf("%stest%d", prefix, id)
			pool.Go(func(ctx context.Context) (err error) {
				return uploader.UploadData(nil, p)
			})
		}(i)
	}
	err := pool.Wait(context.Background())
	assert.NoError(t, err)

	// 文件列表
	for i := uint(0); i < files; i++ {
		paths = append(paths, fmt.Sprintf("test%d", i))
	}
	return paths
}

func makeLotsFiles(t *testing.T, files uint, batchConcurrency int) (paths []string) {
	return makeLotsFilesWithPrefix(t, files, batchConcurrency, "")
}

func TestDeleteLotsFile(t *testing.T) {
	lister := getClearedListerForTest(t)
	makeLotsFiles(t, 2000, 500)

	paths := lister.ListPrefix("")
	assert.Equal(t, 2000, len(paths))
	_, err := lister.DeleteKeys(paths)
	assert.NoError(t, err)
	assert.Empty(t, lister.ListPrefix(""))
}

func TestListStatLotsFile(t *testing.T) {
	lister := getClearedListerForTest(t)
	makeLotsFiles(t, 2000, 500)

	paths := makeLotsFiles(t, 2000, 500)
	defer lister.DeleteKeys(paths)
	stats := lister.ListStat(paths)
	assert.Equal(t, 2000, len(stats))
}
