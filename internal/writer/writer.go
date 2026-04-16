// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package writer

import (
	"context"
	"fmt"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap"

	iarrow "github.com/enterprisedb/icebergexporter/internal/arrow"
	"github.com/enterprisedb/icebergexporter/internal/iceberg"
)

// TimestampColumns maps table names to their primary timestamp column for partitioning.
var TimestampColumns = map[string]string{
	iarrow.TableTraces:       "start_time_unix_nano",
	iarrow.TableLogs:         "time_unix_nano",
	iarrow.TableGauge:        "time_unix_nano",
	iarrow.TableSum:          "time_unix_nano",
	iarrow.TableHistogram:    "time_unix_nano",
	iarrow.TableExpHistogram: "time_unix_nano",
	iarrow.TableSummary:      "time_unix_nano",
}

// Writer orchestrates the flush path: merge records → split by partition →
// write Parquet → upload to S3 → commit to Iceberg catalog.
type Writer struct {
	fileIO      iceberg.FileIO
	catalog     iceberg.Catalog
	namespace   string
	granularity iarrow.Granularity
	logger      *zap.Logger
	alloc       memory.Allocator

	// schemas caches the Arrow schema per table for EnsureTable calls.
	schemas map[string]*arrowlib.Schema
}

// Config holds Writer configuration.
type Config struct {
	FileIO      iceberg.FileIO
	Catalog     iceberg.Catalog
	Namespace   string
	Granularity iarrow.Granularity
	Logger      *zap.Logger
}

// New creates a new Writer.
func New(cfg Config) *Writer {
	g := cfg.Granularity
	if g == "" {
		g = iarrow.GranularityHour
	}
	return &Writer{
		fileIO:      cfg.FileIO,
		catalog:     cfg.Catalog,
		namespace:   cfg.Namespace,
		granularity: g,
		logger:      cfg.Logger,
		alloc:       memory.DefaultAllocator,
		schemas:     make(map[string]*arrowlib.Schema),
	}
}

// RegisterSchema registers the Arrow schema for a table, used for
// EnsureTable calls to the catalog.
func (w *Writer) RegisterSchema(table string, schema *arrowlib.Schema) {
	w.schemas[table] = schema
}

// Flush is the buffer manager's flush callback. It merges records, splits by
// partition, writes Parquet files to S3, and commits to the catalog.
// Returns the total Parquet bytes written (for buffer calibration).
func (w *Writer) Flush(ctx context.Context, table string, records []arrowlib.RecordBatch, totalRows int64) (int64, error) {
	if len(records) == 0 {
		return 0, nil
	}

	// Ensure the table exists in the catalog
	if schema, ok := w.schemas[table]; ok {
		if err := w.catalog.EnsureNamespace(ctx, w.namespace); err != nil {
			return 0, fmt.Errorf("ensuring namespace: %w", err)
		}
		if err := w.catalog.EnsureTable(ctx, w.namespace, table, schema); err != nil {
			return 0, fmt.Errorf("ensuring table %s: %w", table, err)
		}
	}

	// Merge all records into a single batch
	merged, err := iarrow.MergeRecords(w.alloc, records)
	if err != nil {
		return 0, fmt.Errorf("merging records: %w", err)
	}
	defer merged.Release()

	// Split by time partition
	tsCol := TimestampColumns[table]
	if tsCol == "" {
		tsCol = "time_unix_nano" // fallback
	}

	partitions, err := iarrow.SplitByPartition(w.alloc, merged, tsCol, w.granularity)
	if err != nil {
		return 0, fmt.Errorf("splitting by partition: %w", err)
	}
	defer func() {
		for _, p := range partitions {
			p.Record.Release()
		}
	}()

	// Write each partition as a Parquet file
	var totalBytes int64
	var dataFiles []iceberg.DataFile

	for _, part := range partitions {
		data, err := iarrow.WriteParquet(part.Record, iarrow.DefaultCompression())
		if err != nil {
			return totalBytes, fmt.Errorf("writing parquet for partition %s: %w", part.Key.HivePath(), err)
		}

		path := iceberg.DataFilePath(table, part.Key.HivePath())
		if err := w.fileIO.Write(ctx, path, data); err != nil {
			return totalBytes, fmt.Errorf("uploading to S3 %s: %w", path, err)
		}

		totalBytes += int64(len(data))

		dataFiles = append(dataFiles, iceberg.DataFile{
			Path:            w.fileIO.URI(path),
			Format:          "PARQUET",
			RecordCount:     part.Record.NumRows(),
			FileSizeBytes:   int64(len(data)),
			PartitionValues: part.Key.PartitionValues(),
		})

		w.logger.Debug("wrote partition",
			zap.String("table", table),
			zap.String("partition", part.Key.HivePath()),
			zap.Int64("rows", part.Record.NumRows()),
			zap.Int("bytes", len(data)),
		)
	}

	// Commit to Iceberg catalog
	if err := w.catalog.AppendDataFiles(ctx, w.namespace, table, dataFiles); err != nil {
		return totalBytes, fmt.Errorf("committing data files: %w", err)
	}

	w.logger.Info("flushed table",
		zap.String("table", table),
		zap.Int64("rows", totalRows),
		zap.Int64("bytes", totalBytes),
		zap.Int("partitions", len(partitions)),
	)

	return totalBytes, nil
}
