// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"testing"
)

func TestS3FileIOFullKey(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		path   string
		want   string
	}{
		{"with prefix", "iceberg", "otel_traces/data/file.parquet", "iceberg/otel_traces/data/file.parquet"},
		{"empty prefix", "", "otel_traces/data/file.parquet", "otel_traces/data/file.parquet"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fio := &S3FileIO{prefix: tt.prefix}
			if got := fio.fullKey(tt.path); got != tt.want {
				t.Errorf("fullKey(%s) = %s, want %s", tt.path, got, tt.want)
			}
		})
	}
}

func TestS3FileIOURI(t *testing.T) {
	fio := &S3FileIO{bucket: "otel-data", prefix: "iceberg"}
	got := fio.URI("otel_traces/data/file.parquet")
	want := "s3://otel-data/iceberg/otel_traces/data/file.parquet"
	if got != want {
		t.Errorf("URI() = %s, want %s", got, want)
	}
}

func TestS3FileIOURINoPrefix(t *testing.T) {
	fio := &S3FileIO{bucket: "otel-data", prefix: ""}
	got := fio.URI("otel_traces/data/file.parquet")
	want := "s3://otel-data/otel_traces/data/file.parquet"
	if got != want {
		t.Errorf("URI() = %s, want %s", got, want)
	}
}
