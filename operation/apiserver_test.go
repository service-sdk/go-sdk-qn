package operation

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestModeString(t *testing.T) {
	index, replica, n, m, err := parseWriteModeString("Mode0R3N28M4")
	assert.NoError(t, err)
	assert.Equal(t, index, uint64(0))
	assert.Equal(t, replica, uint64(3))
	assert.Equal(t, n, uint64(28))
	assert.Equal(t, m, uint64(4))
}
