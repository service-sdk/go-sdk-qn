package operation

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/pelletier/go-toml"
)

// MultiClustersConfig 多集群配置文件
type MultiClustersConfig struct {
	configs                    map[string]*Config
	originalPath               string
	selectConfigCallback       func(configs map[string]*Config, key string) (*Config, bool)
	selectConfigCallbackRwLock sync.RWMutex
}

// SetConfigSelectCallback 设置一个配置选取策略回调
func (config *MultiClustersConfig) SetConfigSelectCallback(f func(configs map[string]*Config, key string) (*Config, bool)) {
	config.selectConfigCallbackRwLock.Lock()
	defer config.selectConfigCallbackRwLock.Unlock()

	config.selectConfigCallback = f
}

// 处理多集群配置文件
func (config *MultiClustersConfig) forEachClusterConfig(f func(string, *Config) error) error {
	for pathPrefix, config := range config.configs {
		if err := f(pathPrefix, config); err != nil {
			return err
		}
	}
	return nil
}

// 根据key获取对应的配置
func (config *MultiClustersConfig) forKey(key string) (*Config, bool) {
	return config.selectConfig(key)
}

// 根据key获取对应的配置
func (config *MultiClustersConfig) selectConfig(key string) (*Config, bool) {
	// 如果没有设置回调，则使用默认的回调
	if config.selectConfigCallback == nil {
		return config.defaultSelectConfigCallbackFunc(key)
	}

	config.selectConfigCallbackRwLock.RLock()
	defer config.selectConfigCallbackRwLock.RUnlock()

	return config.selectConfigCallback(config.configs, key)
}

// 默认的配置选取策略
func (config *MultiClustersConfig) defaultSelectConfigCallbackFunc(key string) (*Config, bool) {
	isKeyStartsWithPrefix := func(key, prefix string) bool {
		if !strings.HasSuffix(key, "/") {
			key = key + "/"
		}
		if !strings.HasSuffix(prefix, "/") {
			prefix = prefix + "/"
		}
		return strings.HasPrefix(key, prefix)
	}

	for keyPrefix, config := range config.configs {
		if isKeyStartsWithPrefix(key, keyPrefix) {
			return config, true
		}
	}
	return nil, false
}

func (config *MultiClustersConfig) getOriginalPaths() []string {
	paths := make([]string, 0, 1+len(config.configs))
	if config.originalPath != "" {
		paths = append(paths, config.originalPath)
	}
	for _, config := range config.configs {
		paths = append(paths, config.getOriginalPaths()...)
	}
	return paths
}

// LoadMultiClusterConfigs 加载多集群配置文件
func LoadMultiClusterConfigs(file string) (*MultiClustersConfig, error) {
	namePathMap := make(map[string]string)
	raw, err := os.ReadFile(file)
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

	config := MultiClustersConfig{configs: make(map[string]*Config), originalPath: file}
	for name, p := range namePathMap {
		// 依次加载多集群的每个配置文件
		if configFile, err := Load(p); err != nil {
			return &config, err
		} else {
			config.configs[name] = configFile
		}
	}

	return &config, nil
}
