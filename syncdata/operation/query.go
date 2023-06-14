package operation

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kirsle/configdir"
	"github.com/service-sdk/go-sdk-qn/v2/x/rpc.v7"
)

var (
	cacheMap         sync.Map
	cacheUpdaterLock sync.Mutex
	cachePersisting  uint32 = 0
	cacheDirectory          = configdir.LocalCache("qiniu", "go-sdk")
)

// Queryer 域名查询器
type Queryer struct {
	ak            string
	bucket        string
	ucHosts       []string
	httpTransport http.RoundTripper
	ucTimeout     time.Duration
	client        *http.Client
}

type IQueryer interface {
	QueryUpHosts(https bool) (urls []string)
	QueryRsHosts(https bool) (urls []string)
	QueryRsfHosts(https bool) (urls []string)
	QueryApiServerHosts(https bool) (urls []string)
	QueryIoHosts(https bool) (urls []string)
}

type (
	cache struct {
		CachedHosts    cachedHosts `json:"hosts"`
		CacheExpiredAt time.Time   `json:"expired_at"`
	}

	cachedHosts struct {
		Hosts []cachedHost `json:"hosts"`
	}

	cachedHost struct {
		Ttl       int64                `json:"ttl"`
		Io        cachedServiceDomains `json:"io"`
		Up        cachedServiceDomains `json:"up"`
		Rs        cachedServiceDomains `json:"rs"`
		Rsf       cachedServiceDomains `json:"rsf"`
		ApiServer cachedServiceDomains `json:"api"`
	}

	cachedServiceDomains struct {
		Domains []string `json:"domains"`
	}
)

func init() {
	_ = loadQueryersCache()
}

// NewQueryer 根据配置创建域名查询器
func NewQueryer(c *Config) IQueryer {
	q := Queryer{
		ak:            c.Ak,
		bucket:        c.Bucket,
		ucHosts:       dupStrings(c.UcHosts),
		httpTransport: getHttpClientTransport(c),
		ucTimeout:     buildDurationByMs(c.UcTimeoutMs, DefaultConfigUcTimeoutMs),
	}
	shuffleHosts(q.ucHosts)
	return &q
}

func (queryer *Queryer) getQueryClient() *http.Client {
	if queryer.client != nil {
		return queryer.client
	}

	queryer.client = &http.Client{
		Transport: queryer.httpTransport,
		Timeout:   queryer.ucTimeout,
	}
	return queryer.client
}

// QueryUpHosts 查询 UP 服务器 URL
func (queryer *Queryer) QueryUpHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Up.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

// QueryIoHosts 查询 IO 服务器 URL
func (queryer *Queryer) QueryIoHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Io.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

// QueryRsHosts 查询 RS 服务器 URL
func (queryer *Queryer) QueryRsHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Rs.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

// QueryRsfHosts 查询 RSF 服务器 URL
func (queryer *Queryer) QueryRsfHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].Rsf.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

// QueryApiServerHosts 查询 APISERVER 服务器 URL
func (queryer *Queryer) QueryApiServerHosts(https bool) (urls []string) {
	if cache, err := queryer.query(); err == nil {
		domains := cache.CachedHosts.Hosts[0].ApiServer.Domains
		urls = queryer.fromDomainsToUrls(https, domains)
	}
	return
}

func (queryer *Queryer) fromDomainsToUrls(https bool, domains []string) (urls []string) {
	urls = make([]string, len(domains))
	for i, domain := range domains {
		if strings.Contains(domain, "://") {
			urls[i] = domain
		} else if https {
			urls[i] = fmt.Sprintf("https://%s", domain)
		} else {
			urls[i] = fmt.Sprintf("http://%s", domain)
		}
	}
	return urls
}

func (queryer *Queryer) query() (*cache, error) {
	var err error
	c := queryer.getCache()
	if c == nil {
		return func() (*cache, error) {
			var err error
			cacheUpdaterLock.Lock()
			defer cacheUpdaterLock.Unlock()
			c := queryer.getCache()
			if c == nil {
				if c, err = queryer.mustQuery(); err != nil {
					return nil, err
				} else {
					queryer.setCache(c)
					_ = saveQueryersCache()
					return c, nil
				}
			} else {
				return c, nil
			}
		}()
	} else {
		if c.CacheExpiredAt.Before(time.Now()) {
			queryer.asyncRefresh()
		}
		return c, err
	}
}

func (queryer *Queryer) mustQuery() (c *cache, err error) {
	var req *http.Request
	var resp *http.Response

	client := queryer.getQueryClient()

	query := make(url.Values, 2)
	query.Set("ak", queryer.ak)
	query.Set("bucket", queryer.bucket)

	failedUcHosts := make(map[string]struct{})

	for i := 0; i < 10; i++ {
		ucHost := queryer.nextUcHost(failedUcHosts)
		queryUrl := fmt.Sprintf("%s/v4/query?%s", ucHost, query.Encode())
		req, err = http.NewRequest(http.MethodGet, queryUrl, http.NoBody)
		if err != nil {
			continue
		}
		req.Header.Set("User-Agent", rpc.UserAgent)
		resp, err = client.Do(req)
		if err != nil {
			failedUcHosts[ucHost] = struct{}{}
			failHostName(ucHost)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode/100 != 2 {
			failedUcHosts[ucHost] = struct{}{}
			failHostName(ucHost)
			err = fmt.Errorf("uc queryV4 status code error: %d", resp.StatusCode)
			continue
		}

		c = new(cache)
		if err = json.NewDecoder(resp.Body).Decode(&c.CachedHosts); err != nil {
			failedUcHosts[ucHost] = struct{}{}
			failHostName(ucHost)
			continue
		}
		if len(c.CachedHosts.Hosts) == 0 {
			failedUcHosts[ucHost] = struct{}{}
			failHostName(ucHost)
			return nil, errors.New("uc queryV4 returns empty hosts")
		}
		minTTL := c.CachedHosts.Hosts[0].Ttl
		for _, host := range c.CachedHosts.Hosts[1:] { // 取出 Hosts 内最小的 TTL
			if minTTL > host.Ttl {
				minTTL = host.Ttl
			}
		}
		c.CacheExpiredAt = time.Now().Add(time.Duration(minTTL) * time.Second)
		succeedHostName(ucHost)
		break
	}
	if err != nil {
		c = nil
	}
	return
}

func (queryer *Queryer) asyncRefresh() {
	go func() {
		var err error

		cacheUpdaterLock.Lock()
		defer cacheUpdaterLock.Unlock()

		c := queryer.getCache()
		if c == nil || c.CacheExpiredAt.Before(time.Now()) {
			if c, err = queryer.mustQuery(); err == nil {
				queryer.setCache(c)
				_ = saveQueryersCache()
			}
		}
	}()
}

func (queryer *Queryer) getCache() *cache {
	value, ok := cacheMap.Load(queryer.cacheKey())
	if !ok {
		return nil
	}
	return value.(*cache)
}

func (queryer *Queryer) setCache(c *cache) {
	cacheMap.Store(queryer.cacheKey(), c)
}

func (queryer *Queryer) cacheKey() string {
	ucHosts := dupStrings(queryer.ucHosts)
	sort.Strings(ucHosts)
	serializedUcHosts := strings.Join(ucHosts, "$")
	hostsCrc32 := crc32.ChecksumIEEE([]byte(serializedUcHosts))
	return fmt.Sprintf("cache-key-v2:%s:%s:%d", queryer.ak, queryer.bucket, hostsCrc32)
}

var curUcHostIndex uint32 = 0

func (queryer *Queryer) nextUcHost(failedHosts map[string]struct{}) string {
	switch len(queryer.ucHosts) {
	case 0:
		panic("No Uc hosts is configured")
	case 1:
		return queryer.ucHosts[0]
	default:
		var ucHost string
		for i := 0; i <= len(queryer.ucHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curUcHostIndex, 1) - 1)
			ucHost = queryer.ucHosts[index%len(queryer.ucHosts)]
			if _, isFailedBefore := failedHosts[ucHost]; !isFailedBefore && isHostNameValid(ucHost) {
				break
			}
		}
		return ucHost
	}
}

// SetCacheDirectoryAndLoad 设置查询结果缓存目录
func SetCacheDirectoryAndLoad(path string) error {
	cacheDirectory = path
	cacheMap.Range(func(key, _ interface{}) bool {
		cacheMap.Delete(key)
		return true
	})
	return loadQueryersCache()
}

func loadQueryersCache() error {
	cacheFile, err := os.Open(filepath.Join(cacheDirectory, "query-cache.json"))

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	err = json.NewDecoder(cacheFile).Decode(&m)
	if err != nil {
		return err
	}

	for key, value := range m {
		if strings.HasPrefix(key, "cache-key-v2:") {
			cacheMap.Store(key, value)
		}
	}
	return nil
}

func saveQueryersCache() error {
	cacheDirInfo, err := os.Stat(cacheDirectory)

	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.MkdirAll(cacheDirectory, 0700); err != nil {
				return err
			}
		} else {
			return err
		}
	} else if !cacheDirInfo.IsDir() {
		return errors.New("cache directory path is occupied and not directory")
	}

	if !atomic.CompareAndSwapUint32(&cachePersisting, 0, 1) {
		return nil
	}
	defer atomic.StoreUint32(&cachePersisting, 1)

	cacheFile, err := os.OpenFile(filepath.Join(cacheDirectory, "query-cache.json"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer cacheFile.Close()

	m := make(map[string]*cache)
	cacheMap.Range(func(key, value interface{}) bool {
		m[key.(string)] = value.(*cache)
		return true
	})

	bytes, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = cacheFile.Write(bytes)
	return err
}
