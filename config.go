package icebergexporter

import (
	"errors"
	"fmt"
	"time"
)

// Config defines the configuration for the Iceberg exporter.
type Config struct {
	Storage   StorageConfig   `mapstructure:"storage"`
	Catalog   CatalogConfig   `mapstructure:"catalog"`
	Buffer    BufferConfig    `mapstructure:"buffer"`
	Partition PartitionConfig `mapstructure:"partition"`
	Promoted  PromotedConfig  `mapstructure:"promoted"`
}

// StorageConfig holds S3-compatible storage settings.
type StorageConfig struct {
	Endpoint  string `mapstructure:"endpoint"`
	Bucket    string `mapstructure:"bucket"`
	Prefix    string `mapstructure:"prefix"`
	Region    string `mapstructure:"region"`
	AccessKey string `mapstructure:"access_key"`
	SecretKey string `mapstructure:"secret_key"`
	PathStyle bool   `mapstructure:"path_style"`
}

// CatalogConfig holds Iceberg catalog settings.
type CatalogConfig struct {
	Type      string `mapstructure:"type"` // "rest" or "noop"
	URI       string `mapstructure:"uri"`
	Namespace string `mapstructure:"namespace"`
	Warehouse string `mapstructure:"warehouse"`
}

// BufferConfig holds buffering and flush settings.
type BufferConfig struct {
	MaxSizeBytes  int           `mapstructure:"max_size_bytes"`
	FlushInterval time.Duration `mapstructure:"flush_interval"`
}

// PromotedConfig allows overriding default promoted attributes per signal.
type PromotedConfig struct {
	Traces  []string `mapstructure:"traces"`
	Logs    []string `mapstructure:"logs"`
	Metrics []string `mapstructure:"metrics"`
}

// PartitionConfig holds time-based partitioning settings.
type PartitionConfig struct {
	Granularity PartitionGranularity `mapstructure:"granularity"`
}

// PartitionGranularity controls time-based partition resolution.
type PartitionGranularity string

const (
	PartitionHour  PartitionGranularity = "hour"
	PartitionDay   PartitionGranularity = "day"
	PartitionMonth PartitionGranularity = "month"
)

func (cfg *Config) Validate() error {
	if cfg.Storage.Endpoint == "" {
		return errors.New("storage.endpoint is required")
	}
	if cfg.Storage.Bucket == "" {
		return errors.New("storage.bucket is required")
	}
	if cfg.Catalog.Type != "" && cfg.Catalog.Type != "rest" && cfg.Catalog.Type != "noop" {
		return fmt.Errorf("catalog.type must be \"rest\" or \"noop\", got %q", cfg.Catalog.Type)
	}
	if cfg.Catalog.Type == "rest" && cfg.Catalog.URI == "" {
		return errors.New("catalog.uri is required when catalog.type is \"rest\"")
	}
	if cfg.Buffer.MaxSizeBytes < 0 {
		return errors.New("buffer.max_size_bytes must be non-negative")
	}
	if cfg.Buffer.FlushInterval < 0 {
		return errors.New("buffer.flush_interval must be non-negative")
	}
	switch cfg.Partition.Granularity {
	case PartitionHour, PartitionDay, PartitionMonth, "":
		// valid
	default:
		return fmt.Errorf("partition.granularity must be \"hour\", \"day\", or \"month\", got %q", cfg.Partition.Granularity)
	}
	return nil
}

func defaultConfig() *Config {
	return &Config{
		Storage: StorageConfig{
			Region:    "us-east-1",
			Prefix:    "iceberg",
			PathStyle: true,
		},
		Catalog: CatalogConfig{
			Type:      "rest",
			Namespace: "otel",
		},
		Buffer: BufferConfig{
			MaxSizeBytes:  128 * 1024 * 1024, // 128 MB
			FlushInterval: 60 * time.Second,
		},
		Partition: PartitionConfig{
			Granularity: PartitionHour,
		},
	}
}
