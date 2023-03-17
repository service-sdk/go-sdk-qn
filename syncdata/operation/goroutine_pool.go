package operation

import (
	"context"

	"golang.org/x/sync/errgroup"
)

type goroutinePool struct {
	workers           []func(context.Context) error
	maxGoroutineCount int
}

func newGoroutinePool(maxGoroutineCount int) *goroutinePool {
	return &goroutinePool{maxGoroutineCount: maxGoroutineCount}
}

func (pool *goroutinePool) Go(worker func(context.Context) error) {
	pool.workers = append(pool.workers, worker)
}

func (pool *goroutinePool) Wait(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	workersChan := make(chan func(context.Context) error)

	for i := 0; i < pool.maxGoroutineCount; i++ {
		func(i int) {
			group.Go(func() error {
				for worker := range workersChan {
					if err := worker(ctx); err != nil {
						return err
					}
				}
				return nil
			})
		}(i)
	}

	for _, worker := range pool.workers {
		workersChan <- worker
	}
	close(workersChan)

	return group.Wait()
}
