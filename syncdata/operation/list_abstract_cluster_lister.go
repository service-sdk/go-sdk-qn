package operation

import "context"

type DeleteKeysError struct {
	Error string
	Code  int
	Name  string
}

type FromToKeyError struct {
	Error   string
	Code    int
	FromKey string
	ToKey   string
}

type clusterLister interface {
	listStat(ctx context.Context, keys []string) ([]*FileStat, error)
	listPrefix(ctx context.Context, prefix string) ([]string, error)
	listPrefixToChannel(ctx context.Context, prefix string, ch chan<- string) error
	delete(key string, isForce bool) error
	copy(fromKey, toKey string) error
	moveTo(fromKey, toBucket, toKey string) error
	rename(fromKey, toKey string) error
	deleteKeys(ctx context.Context, keys []string, isForce bool) ([]*DeleteKeysError, error)
	copyKeys(ctx context.Context, fromKeys, toKeys []string) ([]*FromToKeyError, error)
	moveKeys(ctx context.Context, fromKeys, toBuckets, toKeys []string) ([]*FromToKeyError, error)
	renameKeys(ctx context.Context, fromKeys, toKeys []string) ([]*FromToKeyError, error)
}
