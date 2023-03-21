package test

import (
	"bytes"
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	bkey     = "abatch"
	bnewkey1 = "abatch/newkey1"
	bnewkey2 = "abatch/newkey2"
)

func prepareKey(t *testing.T) {
	err := bucket.Put(nil, nil, bkey, bytes.NewReader([]byte("HelloWorld1")), 11, nil)
	assert.NoError(t, err)
}

func deleteKey(t *testing.T) {
	err := bucket.Delete(nil, bkey)
	assert.NoError(t, err)
}

func TestBatchStat(t *testing.T) {
	checkSkipTest(t)

	prepareKey(t)
	defer deleteKey(t)

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

func TestBatchMove(t *testing.T) {
	checkSkipTest(t)

	prepareKey(t)
	defer deleteKey(t)

	stat0, err := bucket.Stat(nil, bkey)
	assert.NoError(t, err)

	_, err = bucket.BatchMove(
		nil,
		kodo.KeyPair{Src: bkey, Dest: bnewkey1},
		kodo.KeyPair{Src: bnewkey1, Dest: bnewkey2},
	)
	defer bucket.Move(nil, bnewkey2, bkey)
	assert.NoError(t, err)

	stat1, err := bucket.Stat(nil, bnewkey2)
	assert.NoError(t, err)

	assert.Equal(t, stat0.Hash, stat1.Hash)
}

func TestBatchCopy(t *testing.T) {
	checkSkipTest(t)

	prepareKey(t)
	defer deleteKey(t)

	_, err := bucket.BatchCopy(
		nil,
		kodo.KeyPair{Src: bkey, Dest: bnewkey1},
		kodo.KeyPair{Src: bkey, Dest: bnewkey2},
	)
	defer bucket.Delete(nil, bnewkey1)
	defer bucket.Delete(nil, bnewkey2)
	assert.NoError(t, err)

	stat0, _ := bucket.Stat(nil, bkey)
	stat1, _ := bucket.Stat(nil, bnewkey1)
	stat2, _ := bucket.Stat(nil, bnewkey2)
	assert.Equal(t, stat0.Hash, stat1.Hash)
	assert.Equal(t, stat0.Hash, stat2.Hash)
}

func TestBatchDelete(t *testing.T) {
	checkSkipTest(t)

	prepareKey(t)
	defer deleteKey(t)

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

func TestBatch(t *testing.T) {
	checkSkipTest(t)

	prepareKey(t)
	defer deleteKey(t)

	ops := []string{
		kodo.URICopy(bucketName, bkey, bucketName, bnewkey1),
		kodo.URIDelete(bucketName, bkey),
		kodo.URIMove(bucketName, bnewkey1, bucketName, bkey),
	}

	var rets []kodo.BatchItemRet
	err := client.Batch(nil, &rets, ops)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(rets))
}
