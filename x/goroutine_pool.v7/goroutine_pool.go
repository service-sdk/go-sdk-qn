package goroutine_pool

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// GoroutinePool is a goroutine pool.
type GoroutinePool struct {
	workers           []func(context.Context) error
	maxGoroutineCount int
}

// NewGoroutinePoolWithoutLimit returns a new goroutine pool without limit.
func NewGoroutinePoolWithoutLimit() *GoroutinePool {
	return NewGoroutinePool(0)
}

// NewGoroutinePool returns a new goroutine pool.
// If the max goroutine count is 0, it means no limit.
func NewGoroutinePool(maxGoroutineCount int) *GoroutinePool {
	return &GoroutinePool{maxGoroutineCount: maxGoroutineCount}
}

// CurrentGoroutineCount returns the current goroutine count of the pool.
func (pool *GoroutinePool) CurrentGoroutineCount() int {
	return len(pool.workers)
}

// MaxGoroutineCount returns the max goroutine count of the pool.
// If the max goroutine count is 0, it means no limit.
func (pool *GoroutinePool) MaxGoroutineCount() int {
	return pool.maxGoroutineCount
}

// Go adds a new worker to the pool.
func (pool *GoroutinePool) Go(worker func(context.Context) error) {
	pool.workers = append(pool.workers, worker)
}

// Wait waits for all workers to finish.
// If any worker returns an error, it will return the error.
func (pool *GoroutinePool) Wait(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	workersChan := make(chan func(context.Context) error)

	// 取worker的数量与最大协程数中的最小值作为consumerCount
	// 如果consumerCount为0，则取worker的数量，即不限制协程数
	consumerCount := pool.maxGoroutineCount
	if consumerCount == 0 || consumerCount > len(pool.workers) {
		consumerCount = len(pool.workers)
	}

	// worker consumer
	for i := 0; i < consumerCount; i++ {
		group.Go(func() error {
			for worker := range workersChan {
				if err := worker(ctx); err != nil {
					return err
				}
			}
			return nil
		})
	}

	// worker producer
	for _, worker := range pool.workers {
		workersChan <- worker
	}
	close(workersChan)

	return group.Wait()
}
