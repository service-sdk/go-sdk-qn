package kodo

import (
	"encoding/base64"
	"fmt"
)

func encodeURI(uri string) string {
	return base64.URLEncoding.EncodeToString([]byte(uri))
}

func uriFetch(bucket, key, url string) string {
	return "/fetch/" + encodeURI(url) + "/to/" + encodeURI(bucket+":"+key)
}

func URIDelete(bucket, key string) string {
	return "/delete/" + encodeURI(bucket+":"+key)
}

func URIStat(bucket, key string) string {
	return "/stat/" + encodeURI(bucket+":"+key)
}

func URICopy(bucketSrc, keySrc, bucketDest, keyDest string) string {
	return "/copy/" + encodeURI(bucketSrc+":"+keySrc) + "/" + encodeURI(bucketDest+":"+keyDest)
}

func URIMove(bucketSrc, keySrc, bucketDest, keyDest string) string {
	return "/move/" + encodeURI(bucketSrc+":"+keySrc) + "/" + encodeURI(bucketDest+":"+keyDest)
}

func URIRename(bucketSrc, keySrc, bucketDest, keyDest string) string {
	return "/rename/" + encodeURI(bucketSrc+":"+keySrc) + "/" + encodeURI(bucketDest+":"+keyDest)
}

func URIChangeMime(bucket, key, mime string) string {
	return "/chgm/" + encodeURI(bucket+":"+key) + "/mime/" + encodeURI(mime)
}

const (
	TypeNormal = iota
	TypeLine
)

func URIChangeType(bucket, key string, Type FileType) string {
	return "/chtype/" + encodeURI(bucket+":"+key) + "/type/" + fmt.Sprint(Type)
}
