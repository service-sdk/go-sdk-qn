package kodo

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

var (
	bkey     = "abatch"
	bnewkey1 = "abatch/newkey1"
	bnewkey2 = "abatch/newkey2"
)

func init() {

	if skipTest() {
		return
	}

	rand.Seed(time.Now().UnixNano())
	bkey += strconv.Itoa(rand.Int())
	bnewkey1 += strconv.Itoa(rand.Int())
	bnewkey2 += strconv.Itoa(rand.Int())
	// 删除 可能存在的 key
	bucket.BatchDelete(nil, bkey, bnewkey1, bnewkey2)
}

func TestAll(t *testing.T) {

	if skipTest() {
		return
	}

	//上传一个文件用用于测试
	err := upFile("bucket_test.go", bkey)
	if err != nil {
		t.Fatal(err)
	}
	defer bucket.Delete(nil, bkey)

	testBatchStat(t)
	testBatchCopy(t)
	testBatchMove(t)
	testBatchDelete(t)
	testBatch(t)
	testClient_MakeUptokenBucket(t)
}

func testBatchStat(t *testing.T) {
	err := bucket.Put(nil, nil, bkey, bytes.NewReader([]byte("HelloWorld1")), 11, nil)
	assert.NoError(t, err)
	err = bucket.Put(nil, nil, bnewkey1, bytes.NewReader([]byte("HelloWorld2")), 11, nil)
	assert.NoError(t, err)
	err = bucket.Put(nil, nil, bnewkey2, bytes.NewReader([]byte("HelloWorld3")), 11, nil)
	assert.NoError(t, err)
	defer func() {
		assert.NoError(t, bucket.Delete(nil, bkey))
		assert.NoError(t, bucket.Delete(nil, bnewkey1))
		assert.NoError(t, bucket.Delete(nil, bnewkey2))
	}()

	rets, err := bucket.BatchStat(nil, bkey, bkey, bkey)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rets))

	stat, err := bucket.Stat(nil, bkey)
	assert.NoError(t, err)

	assert.Equal(t, stat, rets[0].Data)
	assert.Equal(t, stat, rets[1].Data)
	assert.Equal(t, stat, rets[2].Data)
}

func testBatchMove(t *testing.T) {

	stat0, err := bucket.Stat(nil, bkey)
	assert.NoError(t, err)

	_, err = bucket.BatchMove(nil, KeyPair{bkey, bnewkey1}, KeyPair{bnewkey1, bnewkey2})
	defer bucket.Move(nil, bnewkey2, bkey)
	assert.NoError(t, err)

	stat1, err := bucket.Stat(nil, bnewkey2)
	assert.NoError(t, err)

	assert.Equal(t, stat0.Hash, stat1.Hash)
}

func testBatchCopy(t *testing.T) {
	_, err := bucket.BatchCopy(nil, KeyPair{bkey, bnewkey1}, KeyPair{bkey, bnewkey2})
	defer bucket.Delete(nil, bnewkey1)
	defer bucket.Delete(nil, bnewkey2)
	assert.NoError(t, err)

	stat0, _ := bucket.Stat(nil, bkey)
	stat1, _ := bucket.Stat(nil, bnewkey1)
	stat2, _ := bucket.Stat(nil, bnewkey2)
	assert.Equal(t, stat0.Hash, stat1.Hash)
	assert.Equal(t, stat0.Hash, stat2.Hash)
}

func testBatchDelete(t *testing.T) {

	bucket.Copy(nil, bkey, bnewkey1)
	bucket.Copy(nil, bkey, bnewkey2)

	_, err := bucket.BatchDelete(nil, bnewkey1, bnewkey2)
	assert.NoError(t, err)

	//这里 err1 != nil && err2 != nil，否则文件没被成功删除，测试失败
	_, err1 := bucket.Stat(nil, bnewkey1)
	assert.Error(t, err1)

	_, err2 := bucket.Stat(nil, bnewkey2)
	assert.Error(t, err2)

}

func testBatch(t *testing.T) {

	ops := []string{
		URICopy(bucketName, bkey, bucketName, bnewkey1),
		URIDelete(bucketName, bkey),
		URIMove(bucketName, bnewkey1, bucketName, bkey),
	}

	var rets []BatchItemRet
	err := client.Batch(nil, &rets, ops)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rets))
}
