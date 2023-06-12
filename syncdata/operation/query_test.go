package operation

import (
	"github.com/stretchr/testify/assert"
	"log"
	"testing"
)

func TestNewQueryer(t *testing.T) {
	assert.NotNil(t, NewQueryer(getConfig1()))
}

func TestQueryer_QueryApiServerHosts(t *testing.T) {
	q := NewQueryer(getConfig1())
	a := q.QueryApiServerHosts(true)
	log.Println(a)
	b := q.QueryUpHosts(true)
	log.Println(b)
}
