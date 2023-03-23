package operation

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pelletier/go-toml"
)

// MultiClustersConfig 多集群配置文件
type MultiClustersConfig struct {
	configs                    map[string]*Config
	originalPath               string
	selectConfigCallback       func(map[string]*Config, string) (*Config, bool)
	selectConfigCallbackRwLock sync.RWMutex
}

func (config *MultiClustersConfig) SetConfigSelectCallback(f func(map[string]*Config, string) (*Config, bool)) {
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
	if config.selectConfigCallback != nil {
		config.selectConfigCallbackRwLock.RLock()
		defer config.selectConfigCallbackRwLock.RUnlock()
		return config.selectConfigCallback(config.configs, key)
	} else {
		return config.defaultSelectConfigCallbackFunc(key)
	}
}

func (config *MultiClustersConfig) defaultSelectConfigCallbackFunc(key string) (*Config, bool) {
	for keyPrefix, config := range config.configs {
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
		if configFile, err := Load(p); err != nil {
			return &config, err
		} else {
			config.configs[name] = configFile
		}
	}

	return &config, nil
}
