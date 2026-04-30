// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package icebergexporter

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
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
	MaxSize       ByteSize            `mapstructure:"max_size"`
	FlushInterval time.Duration       `mapstructure:"flush_interval"`
	Storage       BufferStorageConfig `mapstructure:"storage"`
}

// BufferStorageConfig selects where the per-table buffer keeps its records
// before flushing to Iceberg.
type BufferStorageConfig struct {
	// Type selects the backend. "memory" (default) keeps records in RAM;
	// "disk" persists records to local files for durability across crashes.
	Type string `mapstructure:"type"`

	// Path is the root directory for disk-backed buffers. Each table gets a
	// subdirectory underneath. Required when Type is "disk".
	Path string `mapstructure:"path"`
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

// ByteSize is an int64 byte count that accepts human-readable strings in
// config (SI: "1G" = 10^9; IEC: "1Gi" = 2^30) as well as raw integers.
type ByteSize int64

// UnmarshalText parses a byte size from a config string. Accepts raw integers
// (interpreted as bytes), SI units (KB, MB, GB, TB, ...), and IEC units
// (KiB, MiB, GiB, TiB, ...). Empty input is treated as zero.
func (b *ByteSize) UnmarshalText(text []byte) error {
	s := strings.TrimSpace(string(text))
	if s == "" {
		*b = 0
		return nil
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*b = ByteSize(n)
		return nil
	}
	n, err := humanize.ParseBytes(s)
	if err != nil {
		return fmt.Errorf("invalid byte size %q: %w", s, err)
	}
	if n > math.MaxInt64 {
		return fmt.Errorf("byte size %q overflows int64", s)
	}
	*b = ByteSize(n)
	return nil
}

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
	if cfg.Buffer.MaxSize < 0 {
		return errors.New("buffer.max_size must be non-negative")
	}
	if cfg.Buffer.FlushInterval < 0 {
		return errors.New("buffer.flush_interval must be non-negative")
	}
	switch cfg.Buffer.Storage.Type {
	case "", "memory":
		// valid; empty defaults to memory
	case "disk":
		if cfg.Buffer.Storage.Path == "" {
			return errors.New("buffer.storage.path is required when buffer.storage.type is \"disk\"")
		}
	default:
		return fmt.Errorf("buffer.storage.type must be \"memory\" or \"disk\", got %q", cfg.Buffer.Storage.Type)
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
			MaxSize:       128 * 1024 * 1024, // 128 MiB
			FlushInterval: 60 * time.Second,
			Storage: BufferStorageConfig{
				Type: "memory",
			},
		},
		Partition: PartitionConfig{
			Granularity: PartitionHour,
		},
	}
}
