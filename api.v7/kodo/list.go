package kodo

import (
	"context"
	"io"
	"net/url"
	"strconv"
)

// List 首次请求，请将 marker 设置为 ""。
// 无论 err 值如何，均应该先看 entries 是否有内容。
// 如果后续没有更多数据，err 返回 EOF，markerOut 返回 ""（但不通过该特征来判断是否结束）。
func (p Bucket) List(
	ctx context.Context, prefix, delimiter, marker string, limit int) (entries []ListItem, commonPrefixes []string, markerOut string, err error) {

	listUrl := p.makeListURL(prefix, delimiter, marker, limit)

	var listRet struct {
		Marker   string     `json:"marker"`
		Items    []ListItem `json:"items"`
		Prefixes []string   `json:"commonPrefixes"`
	}
	err = p.Conn.Call(ctx, &listRet, "POST", listUrl)
	if err != nil {
		return
	}
	if listRet.Marker == "" {
		return listRet.Items, listRet.Prefixes, "", io.EOF
	}
	return listRet.Items, listRet.Prefixes, listRet.Marker, nil
}

func (p Bucket) makeListURL(prefix, delimiter, marker string, limit int) string {

	query := make(url.Values)
	query.Add("bucket", p.Name)
	if prefix != "" {
		query.Add("prefix", prefix)
	}
	if delimiter != "" {
		query.Add("delimiter", delimiter)
	}
	if marker != "" {
		query.Add("marker", marker)
	}
	if limit > 0 {
		query.Add("limit", strconv.FormatInt(int64(limit), 10))
	}
	return p.Conn.RSFHost + "/list?" + query.Encode()
}
