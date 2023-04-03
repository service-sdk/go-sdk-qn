package operation

import (
	"context"
	"github.com/service-sdk/go-sdk-qn/x/goroutine_pool.v7"
	"sync"
)

type multiClusterApiServer struct {
	config                   Configurable
	multiClustersConcurrency int
}

func newMultiClusterApiServer(config Configurable, multiClustersConcurrency int) *multiClusterApiServer {
	return &multiClusterApiServer{config, multiClustersConcurrency}
}

func (svr *multiClusterApiServer) getLogicalAvailableSizes() (map[string]uint64, error) {
	pool := goroutine_pool.NewGoroutinePool(svr.multiClustersConcurrency)
	sizes := make(map[string]uint64)
	var sizesLock sync.Mutex
	svr.config.forEachClusterConfig(func(pathPrefix string, config *Config) error {
		pool.Go(func(ctx context.Context) error {
			if size, err := newSingleClusterApiServer(config).getLogicalAvailableSize(ctx); err != nil {
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
	err := pool.Wait(context.Background())
	return sizes, err
}
