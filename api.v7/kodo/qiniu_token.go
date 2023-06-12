package kodo

import (
	"encoding/json"
	"github.com/service-sdk/go-sdk-qn/api.v7/common"
	"github.com/service-sdk/go-sdk-qn/x/url.v7"
	"strconv"
	"strings"
	"time"
)

// MakeBaseUrl 根据空间(Bucket)的域名，以及文件的 key，获得 baseUrl。
// 如果空间是 public 的，那么通过 baseUrl 可以直接下载文件内容。
// 如果空间是 private 的，那么需要对 baseUrl 进行私有签名得到一个临时有效的 privateUrl 进行下载。
func MakeBaseUrl(domain, key string) (baseUrl string) {

	return "http://" + domain + "/" + url.Escape(key)
}

type GetPolicy struct {
	Expires uint32
}

func (p *QiniuClient) MakePrivateUrl(baseUrl string, policy *GetPolicy) (privateUrl string) {
	var expires int64
	if policy == nil || policy.Expires == 0 {
		expires = 180
	} else {
		expires = int64(policy.Expires)
	}
	deadline := time.Now().Unix() + expires

	if strings.Contains(baseUrl, "?") {
		baseUrl += "&e="
	} else {
		baseUrl += "?e="
	}
	baseUrl += strconv.FormatInt(deadline, 10)

	token := p.mac.Sign([]byte(baseUrl))
	return baseUrl + "&token=" + token
}

// MakeUpToken 根据给定的上传策略构造一个上传凭证
func (p *QiniuClient) MakeUpToken(policy *common.PutPolicy) string {
	if policy.Deadline == 0 {
		// 若未设置过期时间，则默认设置为 3600 秒之后失效
		return p.MakeUpTokenWithExpires(policy, 3600)
	} else {
		// 若设置了过期时间，则直接使用，则不用再加有效时间
		return p.MakeUpTokenWithExpires(policy, 0)
	}
}

// MakeUpTokenWithExpires 根据给定的上传策略和有效时间构造一个上传凭证，
// 有效时间单位为秒，将与上传策略中的过期时间叠加。
func (p *QiniuClient) MakeUpTokenWithExpires(policy *common.PutPolicy, expires uint32) string {
	// 解引用会复制一份 policy 的副本，避免修改原来的 policy
	rr := *policy
	rr.Deadline += expires + uint32(time.Now().Unix())
	b, _ := json.Marshal(rr)
	return p.mac.SignWithData(b)
}
