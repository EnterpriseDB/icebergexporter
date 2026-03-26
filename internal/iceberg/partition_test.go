// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"strings"
	"testing"
)

func TestDataFilePath(t *testing.T) {
	path := DataFilePath("otel_traces", "year=2025/month=03/day=15/hour=14")

	if !strings.HasPrefix(path, "otel_traces/data/year=2025/month=03/day=15/hour=14/") {
		t.Errorf("unexpected prefix: %s", path)
	}
	if !strings.HasSuffix(path, ".parquet") {
		t.Errorf("expected .parquet suffix: %s", path)
	}

	// UUID should make each call unique
	path2 := DataFilePath("otel_traces", "year=2025/month=03/day=15/hour=14")
	if path == path2 {
		t.Error("expected unique paths from UUID generation")
	}
}

func TestMetadataPath(t *testing.T) {
	path := MetadataPath("otel_logs")
	if path != "otel_logs/metadata" {
		t.Errorf("unexpected metadata path: %s", path)
	}
}
