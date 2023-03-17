package operation

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pelletier/go-toml"
)

// 多集群配置文件
type MultiClustersConfig struct {
	configs                    map[string]*Config
	originalPath               string
	selectConfigCallback       func(map[string]*Config, string) (*Config, bool)
	selectConfigCallbackRwLock sync.RWMutex
}

func (multiConfigs *MultiClustersConfig) SetConfigSelectCallback(f func(map[string]*Config, string) (*Config, bool)) {
	multiConfigs.selectConfigCallbackRwLock.Lock()
	multiConfigs.selectConfigCallback = f
	multiConfigs.selectConfigCallbackRwLock.Unlock()
}

func (multiConfigs *MultiClustersConfig) forEachClusterConfig(f func(string, *Config) error) error {
	for pathPrefix, config := range multiConfigs.configs {
		if err := f(pathPrefix, config); err != nil {
			return err
		}
	}
	return nil
}

func (config *MultiClustersConfig) forKey(key string) (*Config, bool) {
	return config.selectConfig(key)
}

func (config *MultiClustersConfig) selectConfig(key string) (*Config, bool) {
	if config.selectConfigCallback != nil {
		config.selectConfigCallbackRwLock.RLock()
		defer config.selectConfigCallbackRwLock.RUnlock()
		return config.selectConfigCallback(config.configs, key)
	} else {
		return config.defaultSelectConfigCallbackFunc(key)
	}
}

func (multiConfigs *MultiClustersConfig) defaultSelectConfigCallbackFunc(key string) (*Config, bool) {
	for keyPrefix, config := range multiConfigs.configs {
		if isKeyStartsWithPrefix(key, keyPrefix) {
			return config, true
		}
	}
	return nil, false
}

func isKeyStartsWithPrefix(key, prefix string) bool {
	keySplited := strings.Split(key, string(filepath.Separator))
	prefixSplited := strings.Split(prefix, string(filepath.Separator))
	if len(keySplited) < len(prefixSplited) {
		return false
	}
	for i := 0; i < len(prefixSplited); i++ {
		if keySplited[i] != prefixSplited[i] {
			return false
		}
	}
	return true
}

func (multiConfigs *MultiClustersConfig) getOriginalPaths() []string {
	paths := make([]string, 0, 1+len(multiConfigs.configs))
	if multiConfigs.originalPath != "" {
		paths = append(paths, multiConfigs.originalPath)
	}
	for _, config := range multiConfigs.configs {
		paths = append(paths, config.getOriginalPaths()...)
	}
	return paths
}

// 加载多集群配置文件
func LoadMultiClusterConfigs(file string) (*MultiClustersConfig, error) {
	namePathMap := make(map[string]string)
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(path.Ext(file))
	if ext == ".json" {
		err = json.Unmarshal(raw, &namePathMap)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &namePathMap)
	} else {
		return nil, errors.New("invalid configuration format")
	}
	if err != nil {
		return nil, err
	}

	multiConfigs := MultiClustersConfig{configs: make(map[string]*Config), originalPath: file}
	for name, path := range namePathMap {
		if config, err := Load(path); err != nil {
			return &multiConfigs, err
		} else {
			multiConfigs.configs[name] = config
		}
	}

	return &multiConfigs, nil
}
