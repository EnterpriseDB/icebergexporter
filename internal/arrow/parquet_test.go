// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/compress"
)

func makeSimpleRecord(nRows int) arrowlib.Record {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "id", Type: arrowlib.PrimitiveTypes.Int64},
		{Name: "name", Type: arrowlib.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < nRows; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		b.Field(1).(*array.StringBuilder).Append("row")
	}
	return b.NewRecord()
}

func TestWriteParquet(t *testing.T) {
	rec := makeSimpleRecord(100)
	defer rec.Release()

	data, err := WriteParquet(rec, compress.Codecs.Zstd)
	if err != nil {
		t.Fatalf("WriteParquet failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty Parquet data")
	}
	// Parquet magic bytes: PAR1
	if string(data[:4]) != "PAR1" {
		t.Error("expected Parquet magic header")
	}
}

func TestWriteParquetSnappy(t *testing.T) {
	rec := makeSimpleRecord(10)
	defer rec.Release()

	data, err := WriteParquet(rec, compress.Codecs.Snappy)
	if err != nil {
		t.Fatalf("WriteParquet (snappy) failed: %v", err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty Parquet data")
	}
}

func TestMergeRecords(t *testing.T) {
	r1 := makeSimpleRecord(5)
	defer r1.Release()
	r2 := makeSimpleRecord(10)
	defer r2.Release()

	merged, err := MergeRecords(memory.DefaultAllocator, []arrowlib.Record{r1, r2})
	if err != nil {
		t.Fatalf("MergeRecords failed: %v", err)
	}
	defer merged.Release()

	if merged.NumRows() != 15 {
		t.Errorf("expected 15 merged rows, got %d", merged.NumRows())
	}
}

func TestMergeRecordsSingle(t *testing.T) {
	r := makeSimpleRecord(5)
	defer r.Release()

	merged, err := MergeRecords(memory.DefaultAllocator, []arrowlib.Record{r})
	if err != nil {
		t.Fatalf("MergeRecords single failed: %v", err)
	}
	defer merged.Release()

	if merged.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", merged.NumRows())
	}
}

func TestMergeRecordsEmpty(t *testing.T) {
	_, err := MergeRecords(memory.DefaultAllocator, nil)
	if err == nil {
		t.Error("expected error for empty records")
	}
}
