package operation

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGoroutinePool(t *testing.T) {
	pool := newGoroutinePool(5)
	arr := make([]uint64, 50)
	for i := uint64(0); i < 50; i++ {
		func(i uint64) {
			pool.Go(func(c context.Context) error {
				for n := 0; n < 10000; n++ {
					arr[i] += 1
				}
				return nil
			})
		}(i)
	}
	pool.Wait(context.Background())
	for _, n := range arr {
		assert.Equal(t, n, uint64(10000))
	}
}
