package operation

import "context"

type SingleKeyError struct {
	Error string
	Code  int
	Name  string
}

type DeleteKeysError SingleKeyError

type FromToKey struct {
	FromKey string
	ToKey   string
}

type CopyKeyInput FromToKey
type RenameKeyInput FromToKey

type MoveKeyInput struct {
	FromToKey
	toBucket string
}

type FromToKeyError struct {
	Error   string
	Code    int
	FromKey string
	ToKey   string
}

type CopyKeysError FromToKeyError
type RenameKeysError FromToKeyError

type MoveKeysError struct {
	FromToKeyError
	ToBucket string
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
	copyKeys(ctx context.Context, input []CopyKeyInput) ([]*CopyKeysError, error)
	moveKeys(ctx context.Context, input []MoveKeyInput) ([]*MoveKeysError, error)
	renameKeys(ctx context.Context, input []RenameKeyInput) ([]*RenameKeysError, error)
}
