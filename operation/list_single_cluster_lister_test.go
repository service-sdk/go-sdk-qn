package operation

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func getClearedSingleClusterListerForTest(t *testing.T) *singleClusterLister {
	checkSkipTest(t)
	l := newSingleClusterLister(getConfig1())
	clearBucket(t, l)
	return l
}

// 上传，列举，拷贝，列举，删除，列举
func TestSingleClusterLister_upload_listPrefix_copy_delete(t *testing.T) {
	l := getClearedSingleClusterListerForTest(t)

	uploader := NewUploader(getConfig1())

	err := uploader.UploadData(nil, "copy1")
	assert.NoError(t, err)

	r, err := l.listPrefix(context.Background(), "copy1")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, "copy1", r[0])

	err = l.copy("copy1", "copy2")
	assert.NoError(t, err)

	r, err = l.listPrefix(context.Background(), "copy2")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r))
	assert.Equal(t, "copy2", r[0])

	err = l.delete("copy1", true)
	assert.NoError(t, err)

	err = l.delete("copy2", true)
	assert.NoError(t, err)

	r, err = l.listPrefix(context.Background(), "copy1")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(r))

	r, err = l.listPrefix(context.Background(), "copy2")
	assert.NoError(t, err)
	assert.Equal(t, 0, len(r))
}

// 上传，列举，删除
func TestSingleClusterLister_upload_listPrefixToChannel_delete(t *testing.T) {

	uploader := NewUploader(getConfig1())

	l := getClearedSingleClusterListerForTest(t)

	ch := make(chan string, 10)
	for i := 0; i < 10; i++ {
		err := uploader.UploadData(nil, fmt.Sprintf("listPrefixToChannel%d", i))
		assert.NoError(t, err)
	}

	go func() {
		defer close(ch)
		err := l.listPrefixToChannel(context.Background(), "listPrefixToChannel", ch)
		assert.NoError(t, err)
	}()

	for i := 0; i < 10; i++ {
		key := <-ch
		assert.Equal(t, fmt.Sprintf("listPrefixToChannel%d", i), key)
		err := l.delete(key, true)
		assert.NoError(t, err)
	}
}

func TestSingleClusterLister_upload_copyKeys(t *testing.T) {

	l := getClearedSingleClusterListerForTest(t)
	uploader := NewUploader(getConfig1())

	err := uploader.UploadData(nil, "copyKeys1")
	assert.NoError(t, err)

	_, err = l.copyKeys(context.Background(), []CopyKeyInput{
		{FromKey: "copyKeys1", ToKey: "copyKeys11"},
		{FromKey: "copyKeys1", ToKey: "copyKeys12"},
	})
	assert.NoError(t, err)

	r, err := l.listPrefix(context.Background(), "copyKeys")
	assert.NoError(t, err)
	assert.Contains(t, r, "copyKeys1")
	assert.Contains(t, r, "copyKeys11")
	assert.Contains(t, r, "copyKeys12")

	err = l.delete("copyKeys1", true)
	assert.NoError(t, err)
	err = l.delete("copyKeys11", true)
	assert.NoError(t, err)
	err = l.delete("copyKeys12", true)
	assert.NoError(t, err)

	r, err = l.listPrefix(context.Background(), "copyKeys")
	assert.NoError(t, err)
	assert.Empty(t, r)
}

// 上传，拷贝，列举
func TestSingleClusterLister_upload_copyKeyFromChannel_listPrefix(t *testing.T) {

	uploader := NewUploader(getConfig1())
	l := getClearedSingleClusterListerForTest(t)

	ch1 := make(chan string, 10)
	ch2 := make(chan CopyKeyInput, 10)

	// 上传10个文件
	for i := 0; i < 10; i++ {
		err := uploader.UploadData(nil, fmt.Sprintf("CopyKeyFromChannel%d", i))
		assert.NoError(t, err)
	}

	time.Sleep(time.Millisecond * 500)

	// 列举所有文件到ch1中
	go func() {
		defer close(ch1)
		err := l.listPrefixToChannel(context.Background(), "CopyKeyFromChannel", ch1)
		assert.NoError(t, err)
	}()

	// 从ch1中读取文件名，构造CopyInput到ch2中
	go func() {
		defer close(ch2)
		for key := range ch1 {
			ch2 <- CopyKeyInput{
				FromKey: key,
				ToKey:   key + "copy",
			}
		}
	}()

	// 从ch2中读取CopyInput，拷贝文件
	err := l.copyKeysFromChannel(context.Background(), ch2, nil)
	assert.NoError(t, err)

	time.Sleep(time.Millisecond * 500)

	r, err := l.listPrefix(context.Background(), "CopyKeyFromChannel")
	assert.NoError(t, err)

	// 检查拷贝后的文件是否存在, 10个文件拷贝后应该有20个文件
	assert.Equal(t, 20, len(r))
	for i := 0; i < 20; i++ {
		err := l.delete(r[i], true)
		assert.NoError(t, err)
	}
}

func TestSingleClusterLister_upload_deleteKeysFromChannel_listPrefix(t *testing.T) {

	l := getClearedSingleClusterListerForTest(t)

	// 批量上传10000个文件
	makeLotsFiles(t, 10000, 500)

	// 列举所有
	ch := make(chan string, 1000)
	go func() {
		defer close(ch)
		elog.Info("listPrefixToChannel start")
		defer elog.Info("listPrefixToChannel end")
		err := l.listPrefixToChannel(context.Background(), "", ch)

		assert.NoError(t, err)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			elog.Info("deleteKeysFromChannel start")
			defer elog.Info("deleteKeysFromChannel end")
			err := l.deleteKeysFromChannel(context.Background(), ch, true, nil)

			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func clearBucket(t *testing.T, l clusterLister) {
	// 列举所有
	ch := make(chan string, 1000)
	go func() {
		defer close(ch)
		err := l.listPrefixToChannel(context.Background(), "", ch)
		assert.NoError(t, err)
	}()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := l.deleteKeysFromChannel(context.Background(), ch, true, nil)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()
}

func TestSingleClusterLister_upload_moveKeysFromChannel_listPrefix(t *testing.T) {

	l := getClearedSingleClusterListerForTest(t)

	// 批量上传5000个文件
	makeLotsFiles(t, 5000, 500)

	// 列举所有文件名到ch1
	ch1 := make(chan string, 1000)
	go func() {
		defer close(ch1)
		err := l.listPrefixToChannel(context.Background(), "", ch1)
		assert.NoError(t, err)
	}()

	// 从ch1获取所有文件名并转换为MoveKeyInput到ch2
	ch2 := make(chan MoveKeyInput, 1000)
	go func() {
		defer close(ch2)
		for key := range ch1 {
			ch2 <- MoveKeyInput{
				FromToKey: FromToKey{
					FromKey: key,
					ToKey:   "move/" + key,
				},
				ToBucket: getConfig1().Bucket,
			}
		}
	}()

	// 开20个消费者批量move
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := l.moveKeysFromChannel(context.Background(), ch2, nil)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	r, err := l.listPrefix(context.Background(), "move/")
	assert.NoError(t, err)
	assert.Equal(t, 5000, len(r))
}
