package operation

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWatcher(t *testing.T) {
	dirPath1, err := ioutil.TempDir("", "folder-1")
	assert.NoError(t, err)
	defer os.RemoveAll(dirPath1)

	dirPath2, err := ioutil.TempDir("", "folder-2")
	assert.NoError(t, err)
	defer os.RemoveAll(dirPath2)

	dirPath3, err := ioutil.TempDir("", "folder-3")
	assert.NoError(t, err)
	defer os.RemoveAll(dirPath3)

	err = ensureWatches([]string{filepath.Join(dirPath1, "1"), filepath.Join(dirPath2, "2"), filepath.Join(dirPath3, "3")})
	assert.NoError(t, err)

	_, ok := globalWatchedFiles.Load(filepath.Join(dirPath1, "1"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath2, "2"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath3, "3"))
	assert.True(t, ok)

	_, ok = globalWatchedDirs.Load(dirPath1)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath2)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath3)
	assert.True(t, ok)

	err = ensureWatches([]string{filepath.Join(dirPath1, "1")})
	assert.NoError(t, err)

	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath1, "1"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath2, "2"))
	assert.False(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath3, "3"))
	assert.False(t, ok)

	_, ok = globalWatchedDirs.Load(dirPath1)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath2)
	assert.False(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath3)
	assert.False(t, ok)

	err = ensureWatches([]string{filepath.Join(dirPath2, "2"), filepath.Join(dirPath3, "3")})
	assert.NoError(t, err)

	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath1, "1"))
	assert.False(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath2, "2"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath3, "3"))
	assert.True(t, ok)

	_, ok = globalWatchedDirs.Load(dirPath1)
	assert.False(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath2)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath3)
	assert.True(t, ok)

	err = ensureWatches(nil)
	assert.NoError(t, err)

	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath1, "1"))
	assert.False(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath2, "2"))
	assert.False(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath3, "3"))
	assert.False(t, ok)

	_, ok = globalWatchedDirs.Load(dirPath1)
	assert.False(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath2)
	assert.False(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath3)
	assert.False(t, ok)

	err = ensureWatches([]string{filepath.Join(dirPath1, "1"), filepath.Join(dirPath2, "2")})
	assert.NoError(t, err)

	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath1, "1"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath2, "2"))
	assert.True(t, ok)
	_, ok = globalWatchedFiles.Load(filepath.Join(dirPath3, "3"))
	assert.False(t, ok)

	_, ok = globalWatchedDirs.Load(dirPath1)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath2)
	assert.True(t, ok)
	_, ok = globalWatchedDirs.Load(dirPath3)
	assert.False(t, ok)
}
