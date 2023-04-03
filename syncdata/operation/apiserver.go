package operation

import (
	"os"
	"strconv"
)

type apiServer interface {
	getLogicalAvailableSizes() (map[string]uint64, error)
}

type ApiServer struct {
	apiServer
}

// NewApiServer 根据配置创建 API Server
func NewApiServer(c *Config) *ApiServer {
	return &ApiServer{newSingleClusterApiServer(c)}
}

// NewApiServerV2 根据环境变量创建 API Server
func NewApiServerV2() *ApiServer {
	c := getCurrentConfigurable()
	if c == nil {
		return nil
	}
	if singleClusterConfig, ok := c.(*Config); ok {
		return NewApiServer(singleClusterConfig)
	}
	if concurrencyStr := os.Getenv("QINIU_MULTI_CLUSTERS_CONCURRENCY"); concurrencyStr != "" {
		if concurrency, err := strconv.Atoi(concurrencyStr); err != nil {
			elog.Warn("Invalid QINIU_MULTI_CLUSTERS_CONCURRENCY: ", err)
		} else {
			return &ApiServer{newMultiClusterApiServer(c, concurrency)}
		}
	}
	return &ApiServer{newMultiClusterApiServer(c, 1)}
}

func (svr *ApiServer) GetLogicalAvailableSizes() (map[string]uint64, error) {
	return svr.getLogicalAvailableSizes()
}
