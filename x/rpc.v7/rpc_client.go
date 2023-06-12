package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/service-sdk/go-sdk-qn/x/xlog.v8"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	. "context"
)

var (
	ErrInvalidRequestURL = errors.New("invalid request url")
	UserAgent            = ""
)

// --------------------------------------------------------------------

type Client struct {
	*http.Client
}

// --------------------------------------------------------------------

func NewRequest(method, url1 string, body io.Reader) (req *http.Request, err error) {

	var host string

	// url1 = "-H <Host> http://<ip>[:<port>]/<path>"
	//
	if strings.HasPrefix(url1, "-H") {
		url2 := strings.TrimLeft(url1[2:], " \t")
		pos := strings.Index(url2, " ")
		if pos <= 0 {
			return nil, ErrInvalidRequestURL
		}
		host = url2[:pos]
		url1 = strings.TrimLeft(url2[pos+1:], " \t")
	}

	req, err = http.NewRequest(method, url1, body)
	if err != nil {
		return
	}
	if host != "" {
		req.Host = host
	}
	return
}

func (r Client) DoRequest(ctx Context, method, url string, timeout time.Duration) (resp *http.Response, err error) {

	req, err := NewRequest(method, url, nil)
	if err != nil {
		return
	}
	return r.DoWithTimeout(ctx, req, timeout)
}

func (r Client) DoRequestWith(
	ctx Context, method, url1 string,
	bodyType string, body io.Reader, bodyLength int,
	timeout time.Duration,
) (resp *http.Response, err error) {

	req, err := NewRequest(method, url1, body)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", bodyType)
	req.ContentLength = int64(bodyLength)
	return r.DoWithTimeout(ctx, req, timeout)
}

func (r Client) DoRequestWith64(
	ctx Context, method, url1 string,
	bodyType string, body io.Reader, bodyLength int64,
	timeout time.Duration,
) (resp *http.Response, err error) {

	req, err := NewRequest(method, url1, body)
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", bodyType)
	req.ContentLength = bodyLength
	return r.DoWithTimeout(ctx, req, timeout)
}

func (r Client) DoRequestWithForm(
	ctx Context, method, url1 string, data map[string][]string,
	timeout time.Duration,
) (resp *http.Response, err error) {
	msg := url.Values(data).Encode()
	if method == "GET" || method == "HEAD" || method == "DELETE" {
		if strings.ContainsRune(url1, '?') {
			url1 += "&"
		} else {
			url1 += "?"
		}
		return r.DoRequest(ctx, method, url1+msg, timeout)
	}
	return r.DoRequestWith(
		ctx, method, url1, "application/x-www-form-urlencoded", strings.NewReader(msg), len(msg), timeout)
}

func (r Client) DoRequestWithJson(
	ctx Context, method, url1 string, data interface{}, timeout time.Duration) (resp *http.Response, err error) {

	msg, err := json.Marshal(data)
	if err != nil {
		return
	}
	return r.DoRequestWith(
		ctx, method, url1, "application/json", bytes.NewReader(msg), len(msg), timeout)
}

func (r Client) Do(ctx Context, req *http.Request) (resp *http.Response, err error) {
	return r.DoWithTimeout(ctx, req, 0)
}

func (r Client) DoWithTimeout(ctx Context, req *http.Request, timeout time.Duration) (resp *http.Response, err error) {

	if ctx == nil {
		ctx = Background()
	}

	if timeout > 0 {
		var cancel CancelFunc
		ctx, cancel = WithTimeout(ctx, timeout)
		defer cancel()
	}

	xl := xlog.FromContextSafe(ctx)
	req.Header.Set("X-Reqid", xl.ReqId())

	if _, ok := req.Header["User-Agent"]; !ok {
		req.Header.Set("User-Agent", UserAgent)
	}

	transport := r.Transport // don't change r.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	// avoid cancel() is called before DoWithTimeout(req), but isn't accurate
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	default:
	}

	if tr, ok := getRequestCanceler(transport); ok { // support CancelRequest
		reqC := make(chan bool, 1)
		go func() {
			resp, err = r.Client.Do(req)
			reqC <- true
		}()
		select {
		case <-reqC:
		case <-ctx.Done():
			tr.CancelRequest(req)
			<-reqC
			if err == nil {
				// 有可能r.Client.Do还没有调用到就进入这个逻辑
				// 需要关掉这个http请求，否则会造成tcp连接泄露
				_ = resp.Body.Close()
			}
			err = ctx.Err()
		}
	} else {
		resp, err = r.Client.Do(req)
	}
	return
}

// --------------------------------------------------------------------

type ErrorInfo struct {
	Err   string `json:"error,omitempty"`
	Key   string `json:"key,omitempty"`
	Reqid string `json:"reqid,omitempty"`
	Errno int    `json:"errno,omitempty"`
	Code  int    `json:"code"`
}

func (r *ErrorInfo) ErrorDetail() string {

	msg, _ := json.Marshal(r)
	return string(msg)
}

func (r *ErrorInfo) Error() string {

	return r.Err
}

func (r *ErrorInfo) RpcError() (code, errno int, key, err string) {

	return r.Code, r.Errno, r.Key, r.Err
}

func (r *ErrorInfo) HttpCode() int {

	return r.Code
}

// --------------------------------------------------------------------

func parseError(e *ErrorInfo, r io.Reader) {

	body, err1 := io.ReadAll(r)
	if err1 != nil {
		e.Err = err1.Error()
		return
	}

	var ret struct {
		Err   string `json:"error"`
		Key   string `json:"key"`
		Errno int    `json:"errno"`
	}
	if json.Unmarshal(body, &ret) == nil && ret.Err != "" {
		// qiniu error msg style returns here
		e.Err, e.Key, e.Errno = ret.Err, ret.Key, ret.Errno
		return
	}
	e.Err = string(body)
}

func ResponseError(resp *http.Response) (err error) {

	e := &ErrorInfo{
		Reqid: resp.Header.Get("X-Reqid"),
		Code:  resp.StatusCode,
	}
	if resp.StatusCode > 299 {
		if resp.ContentLength != 0 {
			if ct := resp.Header.Get("Content-Type"); strings.TrimSpace(strings.SplitN(ct, ";", 2)[0]) == "application/json" {
				parseError(e, resp.Body)
			}
		}
	}
	return e
}

func CallRet(_ Context, ret interface{}, resp *http.Response) (err error) {
	defer func() {
		_, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			log.Println("copy error: ", err)
		}
		_ = resp.Body.Close()
	}()

	if resp.StatusCode/100 == 2 {
		if ret != nil && resp.ContentLength != 0 {
			err = json.NewDecoder(resp.Body).Decode(ret)
			if err != nil {
				return
			}
		}
		//if resp.StatusCode == 200 {
		return nil
		//}
	}
	return ResponseError(resp)
}

func (r Client) CallWithForm(
	ctx Context, ret interface{}, method, url1 string, param map[string][]string, timeout time.Duration) (err error) {

	resp, err := r.DoRequestWithForm(ctx, method, url1, param, timeout)
	if err != nil {
		log.Println("error is ", err)
		return err
	}
	log.Println("ret code", resp.StatusCode)
	return CallRet(ctx, ret, resp)
}

func (r Client) CallWithJson(
	ctx Context, ret interface{}, method, url1 string, param interface{}) (err error) {
	return r.CallWithJsonWithTimeout(ctx, ret, method, url1, param, 0)
}

func (r Client) CallWithJsonWithTimeout(
	ctx Context, ret interface{}, method, url1 string, param interface{}, timeout time.Duration) (err error) {

	resp, err := r.DoRequestWithJson(ctx, method, url1, param, timeout)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}
func (r Client) CallWith(
	ctx Context, ret interface{}, method, url1, bodyType string, body io.Reader, bodyLength int) (err error) {
	return r.CallWithWithTimeout(ctx, ret, method, url1, bodyType, body, bodyLength, 0)
}

func (r Client) CallWithWithTimeout(
	ctx Context, ret interface{}, method, url1, bodyType string, body io.Reader, bodyLength int, timeout time.Duration) (err error) {

	resp, err := r.DoRequestWith(ctx, method, url1, bodyType, body, bodyLength, timeout)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}

func (r Client) CallWith64(
	ctx Context, ret interface{}, method, url1, bodyType string, body io.Reader, bodyLength int64, timeout time.Duration) (err error) {

	resp, err := r.DoRequestWith64(ctx, method, url1, bodyType, body, bodyLength, timeout)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}

func (r Client) Call(
	ctx Context, ret interface{}, method, url1 string) (err error) {
	return r.CallWithTimeout(ctx, ret, method, url1, 0)
}

func (r Client) CallWithTimeout(
	ctx Context, ret interface{}, method, url1 string, timeout time.Duration) (err error) {

	resp, err := r.DoRequestWith(ctx, method, url1, "application/x-www-form-urlencoded", nil, 0, timeout)
	if err != nil {
		return err
	}
	return CallRet(ctx, ret, resp)
}

// ---------------------------------------------------------------------------

type requestCanceler interface {
	CancelRequest(req *http.Request)
}

type nestedObjectGetter interface {
	NestedObject() interface{}
}

func getRequestCanceler(tp http.RoundTripper) (rc requestCanceler, ok bool) {

	if rc, ok = tp.(requestCanceler); ok {
		return
	}

	p := interface{}(tp)
	for {
		getter, ok1 := p.(nestedObjectGetter)
		if !ok1 {
			return
		}
		p = getter.NestedObject()
		if rc, ok = p.(requestCanceler); ok {
			return
		}
	}
}

// --------------------------------------------------------------------
