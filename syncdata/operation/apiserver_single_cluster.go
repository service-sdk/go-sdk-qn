package operation

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"

	"github.com/service-sdk/go-sdk-qn/api.v7/auth/qbox"
	"github.com/service-sdk/go-sdk-qn/api.v7/kodo"
)

type singleClusterApiServer struct {
	apiServerHosts []string
	credentials    *qbox.Mac
	queryer        IQueryer
}

func newSingleClusterApiServer(c *Config) *singleClusterApiServer {
	mac := qbox.NewMac(c.Ak, c.Sk)

	var queryer IQueryer = nil

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
		AccessKey: svr.credentials.GetAccessKey(),
		SecretKey: string(svr.credentials.GetSecretKey()),
		APIHost:   host,
	}
	return kodo.NewClient(&cfg)
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

const DefaultPathPrefix = ""

func (svr *singleClusterApiServer) getLogicalAvailableSizes() (map[string]uint64, error) {
	size, err := svr.getLogicalAvailableSize(context.Background())
	if err != nil {
		return nil, err
	}
	return map[string]uint64{DefaultPathPrefix: size}, nil
}
