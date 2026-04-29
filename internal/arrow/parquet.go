// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
)

// WriteParquet serialises an Arrow record to Parquet bytes using the given compression.
func WriteParquet(rec arrow.RecordBatch, compression compress.Compression) ([]byte, error) {
	var buf bytes.Buffer

	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compression),
		parquet.WithDictionaryDefault(true),
		parquet.WithStats(true),
	)
	arrowProps := pqarrow.DefaultWriterProps()

	writer, err := pqarrow.NewFileWriter(rec.Schema(), &buf, writerProps, arrowProps)
	if err != nil {
		return nil, fmt.Errorf("creating parquet writer: %w", err)
	}

	if err := writer.Write(rec); err != nil {
		writer.Close()
		return nil, fmt.Errorf("writing parquet data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("closing parquet writer: %w", err)
	}

	return buf.Bytes(), nil
}

// MergeRecords concatenates multiple Arrow records with the same schema into one.
// All input records must share the same schema. Caller must release the returned record.
func MergeRecords(alloc memory.Allocator, records []arrow.RecordBatch) (arrow.RecordBatch, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records to merge")
	}
	if len(records) == 1 {
		records[0].Retain()
		return records[0], nil
	}

	schema := records[0].Schema()
	var totalRows int64
	for _, r := range records {
		totalRows += r.NumRows()
	}

	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	// Reserve capacity
	for i := 0; i < schema.NumFields(); i++ {
		builder.Field(i).Reserve(int(totalRows))
	}

	// Append all records by rebuilding through the concatenation of arrays
	cols := make([]arrow.Array, schema.NumFields())
	for i := 0; i < schema.NumFields(); i++ {
		arrays := make([]arrow.Array, len(records))
		for j, rec := range records {
			arrays[j] = rec.Column(i)
		}
		concatenated, err := array.Concatenate(arrays, alloc)
		if err != nil {
			// Release any already-concatenated columns
			for k := 0; k < i; k++ {
				cols[k].Release()
			}
			return nil, fmt.Errorf("concatenating column %s: %w", schema.Field(i).Name, err)
		}
		cols[i] = concatenated
	}

	merged := array.NewRecordBatch(schema, cols, totalRows)
	// Release the concatenated arrays (the record holds references)
	for _, col := range cols {
		col.Release()
	}
	return merged, nil
}

// DefaultCompression returns the default Parquet compression codec.
func DefaultCompression() compress.Compression {
	return compress.Codecs.Zstd
}
