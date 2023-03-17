package operation

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/qiniupd/qiniu-go-sdk/api.v7/auth/qbox"
	"github.com/qiniupd/qiniu-go-sdk/api.v7/kodo"
)

type ApiServer struct {
	config                   Configurable
	singleClusterApiServer   *singleClusterApiServer
	multiClustersConcurrency int
}

// 根据配置创建 API Server
func NewApiServer(c *Config) *ApiServer {
	return &ApiServer{config: c, singleClusterApiServer: newSingleClusterApiServer(c)}
}

// 根据环境变量创建 API Server
func NewApiServerV2() *ApiServer {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	} else if singleClusterConfig, ok := c.(*Config); ok {
		return NewApiServer(singleClusterConfig)
	} else {
		var (
			concurrency = 1
			err         error
		)
		if concurrencyStr := os.Getenv("QINIU_MULTI_CLUSTERS_CONCURRENCY"); concurrencyStr != "" {
			if concurrency, err = strconv.Atoi(concurrencyStr); err != nil {
				elog.Warn("Invalid QINIU_MULTI_CLUSTERS_CONCURRENCY: ", err)
			}
		}
		return &ApiServer{config: c, multiClustersConcurrency: concurrency}
	}
}

func (svr *ApiServer) GetLogicalAvailableSizes() (map[string]uint64, error) {
	return svr.getLogicalAvailableSizes(context.Background())
}

const DefaultPathPrefix = ""

func (svr *ApiServer) getLogicalAvailableSizes(ctx context.Context) (map[string]uint64, error) {
	if svr.singleClusterApiServer != nil {
		size, err := svr.singleClusterApiServer.getLogicalAvailableSize(context.Background())
		if err != nil {
			return nil, err
		}
		return map[string]uint64{DefaultPathPrefix: size}, nil
	}

	pool := newGoroutinePool(svr.multiClustersConcurrency)
	sizes := make(map[string]uint64)
	var sizesLock sync.Mutex
	svr.config.forEachClusterConfig(func(pathPrefix string, config *Config) error {
		pool.Go(func(ctx context.Context) error {
			if size, err := svr.getLogicalAvailableSizeForConfig(ctx, config); err != nil {
				return err
			} else {
				sizesLock.Lock()
				sizes[pathPrefix] = size
				sizesLock.Unlock()
				return nil
			}
		})
		return nil
	})
	err := pool.Wait(ctx)
	return sizes, err
}

func (svr *ApiServer) getLogicalAvailableSizeForConfig(ctx context.Context, config *Config) (uint64, error) {
	return newSingleClusterApiServer(config).getLogicalAvailableSize(ctx)
}

func newSingleClusterApiServer(c *Config) *singleClusterApiServer {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer *Queryer = nil

	if len(c.UcHosts) > 0 {
		queryer = NewQueryer(c)
	}

	svr := singleClusterApiServer{
		apiServerHosts: dupStrings(c.ApiServerHosts),
		credentials:    mac,
		queryer:        queryer,
	}
	shuffleHosts(svr.apiServerHosts)
	return &svr
}

type singleClusterApiServer struct {
	apiServerHosts []string
	credentials    *qbox.Mac
	queryer        *Queryer
}

var curApiServerHostIndex uint32 = 0

func (svr *singleClusterApiServer) nextApiServerHost(failedHosts map[string]struct{}) string {
	apiServerHosts := svr.apiServerHosts
	if svr.queryer != nil {
		if hosts := svr.queryer.QueryApiServerHosts(false); len(hosts) > 0 {
			shuffleHosts(hosts)
			apiServerHosts = hosts
		}
	}
	switch len(apiServerHosts) {
	case 0:
		panic("No ApiServer hosts is configured")
	case 1:
		return apiServerHosts[0]
	default:
		var apiServerHost string
		for i := 0; i <= len(apiServerHosts)*MaxFindHostsPrecent/100; i++ {
			index := int(atomic.AddUint32(&curApiServerHostIndex, 1) - 1)
			apiServerHost = apiServerHosts[index%len(apiServerHosts)]
			if _, isFailedBefore := failedHosts[apiServerHost]; !isFailedBefore && isHostNameValid(apiServerHost) {
				break
			}
		}
		return apiServerHost
	}
}

func (svr *singleClusterApiServer) getLogicalAvailableSize(ctx context.Context) (uint64, error) {
	mcfgs, err := svr.miscconfigs(ctx)
	if err != nil {
		return 0, err
	}
	_, _, n, m, err := parseWriteModeString(mcfgs.DefaultWriteMode)
	if err != nil {
		return 0, err
	}
	s, err := svr.scale(ctx, n, m)
	if err != nil {
		return 0, err
	}
	return s.LogicalAvailableSize, nil
}

type miscConfigs struct {
	DefaultWriteMode string `json:"default_write_mode"`
}

func (svr *singleClusterApiServer) miscconfigs(ctx context.Context) (mcfgs *miscConfigs, err error) {
	failedApiServerHosts := make(map[string]struct{})
	for i := 0; i < 2; i++ {
		apiServerHost := svr.nextApiServerHost(failedApiServerHosts)
		url := fmt.Sprintf("%s/miscconfigs", apiServerHost)
		var ret miscConfigs
		if err := svr.newClient(apiServerHost).Call(ctx, &ret, http.MethodGet, url); err != nil {
			failedApiServerHosts[apiServerHost] = struct{}{}
			failHostName(apiServerHost)
			continue
		}
		succeedHostName(apiServerHost)
		mcfgs = &ret
		break
	}
	return
}

type scale struct {
	LogicalAvailableSize uint64 `json:"logical_avail_size"`
}

func (svr *singleClusterApiServer) scale(ctx context.Context, n, m uint64) (s *scale, err error) {
	failedApiServerHosts := make(map[string]struct{})

	for i := 0; i < 2; i++ {
		apiServerHost := svr.nextApiServerHost(failedApiServerHosts)
		url := fmt.Sprintf("%s/tool/scale/n/%d/m/%d", apiServerHost, n, m)
		var ret scale
		if err := svr.newClient(apiServerHost).Call(ctx, &ret, http.MethodGet, url); err != nil {
			failedApiServerHosts[apiServerHost] = struct{}{}
			failHostName(apiServerHost)
			continue
		}
		succeedHostName(apiServerHost)
		s = &ret
		break
	}
	return
}

func (svr *singleClusterApiServer) newClient(host string) *kodo.Client {
	cfg := kodo.Config{
		AccessKey: svr.credentials.AccessKey,
		SecretKey: string(svr.credentials.SecretKey),
		APIHost:   host,
	}
	return kodo.NewWithoutZone(&cfg)
}

func parseWriteModeString(modeString string) (index, replica, n, m uint64, err error) {
	var parsed int
	parsed, err = fmt.Sscanf(modeString, "Mode%dR%dN%dM%d", &index, &replica, &n, &m)
	if err != nil {
		err = fmt.Errorf("invalid mode string `%s`: %s", modeString, err)
	} else if parsed != 4 {
		err = fmt.Errorf("invalid mode string `%s`: got %d parameters, expected 4", modeString, parsed)
	}
	return
}
