package kodo

import (
	"github.com/service-sdk/go-sdk-qn/v2/operation/internal/api.v7/common"
)

type FileType = common.FileType
type PutPolicy = common.PutPolicy

type Entry struct {
	Hash     string `json:"hash"`
	Fsize    int64  `json:"fsize"`
	PutTime  int64  `json:"putTime"`
	MimeType string `json:"mimeType"`
	EndUser  string `json:"endUser"`
}

type ListItem struct {
	Key      string `json:"key"`
	Hash     string `json:"hash"`
	Fsize    int64  `json:"fsize"`
	PutTime  int64  `json:"putTime"`
	MimeType string `json:"mimeType"`
	EndUser  string `json:"endUser"`
}

type BatchStatItemRet struct {
	Data  Entry  `json:"data"`
	Error string `json:"error"`
	Code  int    `json:"code"`
}

type BatchItemRet struct {
	Error string `json:"error"`
	Code  int    `json:"code"`
}
