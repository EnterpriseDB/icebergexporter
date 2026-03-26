// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
)

func TestNoopCatalog(t *testing.T) {
	cat := NewNoopCatalog()
	ctx := context.Background()

	if err := cat.EnsureNamespace(ctx, "test"); err != nil {
		t.Errorf("EnsureNamespace: %v", err)
	}

	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "val", Type: arrowlib.PrimitiveTypes.Int64},
	}, nil)
	if err := cat.EnsureTable(ctx, "test", "table", schema); err != nil {
		t.Errorf("EnsureTable: %v", err)
	}

	if err := cat.AppendDataFiles(ctx, "test", "table", []DataFile{
		{Path: "s3://bucket/path.parquet", RecordCount: 10, FileSizeBytes: 1024},
	}); err != nil {
		t.Errorf("AppendDataFiles: %v", err)
	}

	if err := cat.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
