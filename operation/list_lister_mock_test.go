package operation

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLister_MockForceDeleteKeys(t *testing.T) {
	mockServer := newMockServer(t)
	defer mockServer.Close()
	mockConfig := mockServer.getConfig()

	lister := &Lister{newSingleClusterLister(mockConfig)}

	uploader := NewUploader(mockConfig)

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
	keys = append(keys, "no_exists_key")
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
