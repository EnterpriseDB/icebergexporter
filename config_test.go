package icebergexporter

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := defaultConfig()

	if cfg.Storage.Region != "us-east-1" {
		t.Errorf("expected default region us-east-1, got %s", cfg.Storage.Region)
	}
	if cfg.Buffer.MaxSizeBytes != 128*1024*1024 {
		t.Errorf("expected default max size 128MB, got %d", cfg.Buffer.MaxSizeBytes)
	}
	if cfg.Buffer.FlushInterval != 60*time.Second {
		t.Errorf("expected default flush interval 60s, got %v", cfg.Buffer.FlushInterval)
	}
	if cfg.Catalog.Type != "rest" {
		t.Errorf("expected default catalog type rest, got %s", cfg.Catalog.Type)
	}
	if cfg.Partition.Granularity != PartitionHour {
		t.Errorf("expected default partition granularity hour, got %s", cfg.Partition.Granularity)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name: "valid config",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120/api/v1"
			},
		},
		{
			name:    "missing endpoint",
			modify:  func(cfg *Config) { cfg.Storage.Bucket = "test" },
			wantErr: true,
		},
		{
			name: "missing bucket",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
			},
			wantErr: true,
		},
		{
			name: "invalid catalog type",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.Type = "invalid"
			},
			wantErr: true,
		},
		{
			name: "rest catalog without URI",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.Type = "rest"
				cfg.Catalog.URI = ""
			},
			wantErr: true,
		},
		{
			name: "noop catalog without URI is fine",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.Type = "noop"
			},
		},
		{
			name: "negative buffer size",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Buffer.MaxSizeBytes = -1
			},
			wantErr: true,
		},
		{
			name: "valid partition granularity day",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Partition.Granularity = PartitionDay
			},
		},
		{
			name: "valid partition granularity month",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Partition.Granularity = PartitionMonth
			},
		},
		{
			name: "invalid partition granularity",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Partition.Granularity = "minute"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := defaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
