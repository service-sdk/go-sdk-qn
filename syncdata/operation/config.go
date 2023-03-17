package operation

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"path"
	"strings"

	"github.com/pelletier/go-toml"
)

// 单集群配置文件
type SingleClusterConfig = Config

// 配置文件
type Config struct {
	UpHosts          []string `json:"up_hosts" toml:"up_hosts"`
	RsHosts          []string `json:"rs_hosts" toml:"rs_hosts"`
	RsfHosts         []string `json:"rsf_hosts" toml:"rsf_hosts"`
	ApiServerHosts   []string `json:"api_server_hosts" toml:"api_server_hosts"`
	Bucket           string   `json:"bucket" toml:"bucket"`
	Ak               string   `json:"ak" toml:"ak"`
	Sk               string   `json:"sk" toml:"sk"`
	PartSize         int64    `json:"part" toml:"part"`
	Addr             string   `json:"addr" toml:"addr"`
	Delete           bool     `json:"delete" toml:"delete"`
	UpConcurrency    int      `json:"up_concurrency" toml:"up_concurrency"`
	BatchConcurrency int      `json:"batch_concurrency" toml:"batch_concurrency"`
	BatchSize        int      `json:"batch_size" toml:"batch_size"`

	DownPath string `json:"down_path" toml:"down_path"`
	Sim      bool   `json:"sim" toml:"sim"`

	IoHosts []string `json:"io_hosts" toml:"io_hosts"`
	UcHosts []string `json:"uc_hosts" toml:"uc_hosts"`

	RecycleBin string `json:"recycle_bin" toml:"recycle_bin"`

	originalPath string `json:"-" toml:"-"`
}

func (config *Config) forEachClusterConfig(f func(string, *Config) error) error {
	return f(DefaultPathPrefix, config)
}

func (config *Config) forKey(key string) (*Config, bool) {
	return config, true
}

func (config *Config) getOriginalPaths() []string {
	paths := make([]string, 0, 1)
	if config.originalPath != "" {
		paths = append(paths, config.originalPath)
	}
	return paths
}

// 加载配置文件
func Load(file string) (*Config, error) {
	var configuration Config
	raw, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}
	ext := strings.ToLower(path.Ext(file))
	if ext == ".json" {
		err = json.Unmarshal(raw, &configuration)
	} else if ext == ".toml" {
		err = toml.Unmarshal(raw, &configuration)
	} else {
		return nil, errors.New("invalid configuration format")
	}
	configuration.originalPath = file

	return &configuration, err
}
