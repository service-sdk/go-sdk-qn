package operation

import "io"

type multiClusterUploader struct {
	config Configurable
}

func newMultiClusterUploader(config Configurable) *multiClusterUploader {
	return &multiClusterUploader{config: config}
}

func (p *multiClusterUploader) uploadReader(reader io.Reader, key string) error {
	if config, exists := p.config.forKey(key); !exists {
		return ErrUndefinedConfig
	} else {
		return newSingleClusterUploader(config).uploadReader(reader, key)
	}
}

func (p *multiClusterUploader) upload(file string, key string) error {
	if config, exists := p.config.forKey(key); !exists {
		return ErrUndefinedConfig
	} else {
		return newSingleClusterUploader(config).upload(file, key)
	}
}

func (p *multiClusterUploader) uploadDataReader(data io.ReaderAt, size int, key string) error {
	if config, exists := p.config.forKey(key); !exists {
		return ErrUndefinedConfig
	} else {
		return newSingleClusterUploader(config).uploadDataReader(data, size, key)
	}
}

func (p *multiClusterUploader) uploadData(data []byte, key string) error {
	if config, exists := p.config.forKey(key); !exists {
		return ErrUndefinedConfig
	} else {
		return newSingleClusterUploader(config).uploadData(data, key)
	}
}
