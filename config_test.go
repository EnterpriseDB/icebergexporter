// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

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
	if cfg.Buffer.MaxSize != 128*1024*1024 {
		t.Errorf("expected default max size 128MiB, got %d", cfg.Buffer.MaxSize)
	}
	if cfg.Buffer.FlushInterval != 60*time.Second {
		t.Errorf("expected default flush interval 60s, got %v", cfg.Buffer.FlushInterval)
	}
	if cfg.Buffer.Storage.Type != "memory" {
		t.Errorf("expected default storage type memory, got %q", cfg.Buffer.Storage.Type)
	}
	if cfg.Catalog.Type != "rest" {
		t.Errorf("expected default catalog type rest, got %s", cfg.Catalog.Type)
	}
	if cfg.Partition.Granularity != PartitionHour {
		t.Errorf("expected default partition granularity hour, got %s", cfg.Partition.Granularity)
	}
}

func TestByteSizeUnmarshalText(t *testing.T) {
	cases := []struct {
		input   string
		want    ByteSize
		wantErr bool
	}{
		{input: "", want: 0},
		{input: "0", want: 0},
		{input: "1024", want: 1024},
		{input: "134217728", want: 128 * 1024 * 1024},
		{input: "1K", want: 1000},                    // SI
		{input: "1KB", want: 1000},                   // SI explicit
		{input: "1KiB", want: 1024},                  // IEC
		{input: "1Mi", want: 1024 * 1024},            // IEC short
		{input: "128Mi", want: 128 * 1024 * 1024},    // IEC short
		{input: "1Gi", want: 1024 * 1024 * 1024},     // IEC short
		{input: "1G", want: 1000 * 1000 * 1000},      // SI
		{input: "1GiB", want: 1024 * 1024 * 1024},    // IEC explicit
		{input: "  1Gi  ", want: 1024 * 1024 * 1024}, // whitespace tolerated
		{input: "garbage", wantErr: true},
		{input: "-5", want: -5}, // raw negative integer accepted (Validate catches it)
	}
	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			var b ByteSize
			err := b.UnmarshalText([]byte(tc.input))
			if (err != nil) != tc.wantErr {
				t.Fatalf("UnmarshalText(%q) error = %v, wantErr=%v", tc.input, err, tc.wantErr)
			}
			if !tc.wantErr && b != tc.want {
				t.Errorf("UnmarshalText(%q) = %d, want %d", tc.input, b, tc.want)
			}
		})
	}
}

func TestValidateBufferStorage(t *testing.T) {
	cases := []struct {
		name    string
		modify  func(*Config)
		wantErr bool
	}{
		{
			name: "memory storage default ok",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
			},
		},
		{
			name: "disk storage with path ok",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Buffer.Storage = BufferStorageConfig{Type: "disk", Path: "/tmp/buf"}
			},
		},
		{
			name: "disk storage without path fails",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Buffer.Storage = BufferStorageConfig{Type: "disk"}
			},
			wantErr: true,
		},
		{
			name: "unknown storage type fails",
			modify: func(cfg *Config) {
				cfg.Storage.Endpoint = "http://localhost:9000"
				cfg.Storage.Bucket = "test"
				cfg.Catalog.URI = "http://localhost:19120"
				cfg.Buffer.Storage = BufferStorageConfig{Type: "s3"}
			},
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := defaultConfig()
			tc.modify(cfg)
			err := cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
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
				cfg.Buffer.MaxSize = -1
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
