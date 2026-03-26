// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
)

// DataFile represents metadata about a written Parquet data file.
type DataFile struct {
	// Path is the full S3 URI of the data file.
	Path string
	// Format is the file format (always "PARQUET").
	Format string
	// RecordCount is the number of rows in the file.
	RecordCount int64
	// FileSizeBytes is the size of the file in bytes.
	FileSizeBytes int64
	// PartitionValues holds the Hive-style partition values.
	PartitionValues map[string]string
}

// Catalog abstracts Iceberg catalog operations.
type Catalog interface {
	// EnsureNamespace creates the namespace if it doesn't exist.
	EnsureNamespace(ctx context.Context, namespace string) error

	// EnsureTable creates the table if it doesn't exist, using the given Arrow
	// schema to derive the Iceberg schema. A no-op if the table already exists.
	EnsureTable(ctx context.Context, namespace, table string, schema *arrowlib.Schema) error

	// AppendDataFiles atomically appends data files to the table.
	AppendDataFiles(ctx context.Context, namespace, table string, files []DataFile) error

	// Close releases any resources held by the catalog.
	Close() error
}
