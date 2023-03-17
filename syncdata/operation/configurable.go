package operation

import (
	"os"
	"sync"
)

type Configurable interface {
	forKey(string) (*Config, bool)
	forEachClusterConfig(func(string, *Config) error) error
	getOriginalPaths() []string
}

var (
	globalConfigurable        Configurable
	globalConfigurableRwLock  sync.RWMutex
	onceForGlobalConfigurable sync.Once
)

const (
	QINIU_ENV               = "QINIU"
	QINIU_MULTI_CLUSTER_ENV = "QINIU_MULTI_CLUSTER"
)

func WithCurrentConfigurable(f func(configurable Configurable)) {
	c := getCurrentConfigurable()

	globalConfigurableRwLock.Lock()
	defer globalConfigurableRwLock.Unlock()
	f(c)
	_ensureWatchesOrUnwatchAll(c)
}

func getCurrentConfigurable() Configurable {
	onceForGlobalConfigurable.Do(initCurrentConfigurableOnce)

	globalConfigurableRwLock.RLock()
	defer globalConfigurableRwLock.RUnlock()
	return globalConfigurable
}

func initCurrentConfigurableOnce() {
	configurable, envVal, err := _loadConfigurableFromEnvironmentVariable()
	if err != nil {
		elog.Warn("Init config from env failed", envVal, err)
		return
	}

	globalConfigurableRwLock.Lock()
	defer globalConfigurableRwLock.Unlock()
	globalConfigurable = configurable
	_ensureWatchesOrUnwatchAll(configurable)
}

func reloadCurrentConfigurable() {
	configurable, envVal, err := _loadConfigurableFromEnvironmentVariable()
	if err != nil {
		elog.Warn("Reload config from env failed", envVal, err)
		return
	}

	globalConfigurableRwLock.Lock()
	defer globalConfigurableRwLock.Unlock()
	globalConfigurable = configurable
	elog.Info("Reload config from env", configurable)
	_ensureWatchesOrUnwatchAll(configurable)
}

func _loadConfigurableFromEnvironmentVariable() (configurable Configurable, envVal string, err error) {
	if envVal = os.Getenv(QINIU_MULTI_CLUSTER_ENV); envVal != "" {
		configurable, err = LoadMultiClusterConfigs(envVal)
	} else if envVal = os.Getenv(QINIU_ENV); envVal != "" {
		configurable, err = Load(envVal)
	}
	return
}

func _ensureWatchesOrUnwatchAll(config Configurable) {
	if config != nil {
		ensureWatches(config.getOriginalPaths())
	} else {
		ensureWatches(nil)
	}
}
