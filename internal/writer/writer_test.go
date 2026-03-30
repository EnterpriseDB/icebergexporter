// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package writer

import (
	"context"
	"sync"
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.uber.org/zap/zaptest"

	iarrow "github.com/enterprisedb/icebergexporter/internal/arrow"
	"github.com/enterprisedb/icebergexporter/internal/iceberg"
)

// memoryFileIO is an in-memory FileIO for testing.
type memoryFileIO struct {
	mu    sync.Mutex
	files map[string][]byte
}

func newMemoryFileIO() *memoryFileIO {
	return &memoryFileIO{files: make(map[string][]byte)}
}

func (f *memoryFileIO) Write(_ context.Context, path string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.files[path] = data
	return nil
}

func (f *memoryFileIO) Read(_ context.Context, path string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	data, ok := f.files[path]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (f *memoryFileIO) List(_ context.Context, prefix string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.files {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (f *memoryFileIO) Delete(_ context.Context, path string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.files, path)
	return nil
}

func (f *memoryFileIO) URI(path string) string {
	return "mem://" + path
}

func (f *memoryFileIO) fileCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.files)
}

func makeTracesRecord(nRows int) arrowlib.RecordBatch {
	promoted := iarrow.DefaultTracesPromoted
	schema := iarrow.TracesSchema(promoted)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	for i := 0; i < nRows; i++ {
		col := 0
		// Resource fields
		b.Field(col).(*array.StringBuilder).AppendNull() // resource_schema_url
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // resource_dropped_attributes_count
		col++
		// Scope fields
		b.Field(col).(*array.StringBuilder).AppendNull() // scope_name
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // scope_version
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // scope_dropped_attributes_count
		col++
		// Span fields
		b.Field(col).(*array.StringBuilder).Append("00000000000000000000000000000000") // trace_id
		col++
		b.Field(col).(*array.StringBuilder).Append("0000000000000000") // span_id
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // trace_state
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // parent_span_id
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // flags
		col++
		b.Field(col).(*array.StringBuilder).Append("test-span") // name
		col++
		b.Field(col).(*array.Int32Builder).Append(1) // kind
		col++
		b.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(1709424000000000000 / 1000)) // start_time (2024-03-03 00:00)
		col++
		b.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(1709424001000000000 / 1000)) // end_time
		col++
		b.Field(col).(*array.Int64Builder).Append(1000000000) // duration
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // status_code
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // status_message
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // dropped_attributes_count
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // dropped_events_count
		col++
		b.Field(col).(*array.Int32Builder).Append(0) // dropped_links_count
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // events
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // links
		col++
		// Promoted attributes (8 for traces)
		for j := 0; j < len(promoted); j++ {
			b.Field(col).(*array.StringBuilder).AppendNull()
			col++
		}
		// Remainder attributes
		b.Field(col).(*array.StringBuilder).AppendNull() // resource_attributes
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // scope_attributes
		col++
		b.Field(col).(*array.StringBuilder).AppendNull() // attributes_remaining
	}

	return b.NewRecordBatch()
}

func TestWriterFlush(t *testing.T) {
	fio := newMemoryFileIO()
	cat := iceberg.NewNoopCatalog()
	logger := zaptest.NewLogger(t)

	w := New(Config{
		FileIO:    fio,
		Catalog:   cat,
		Namespace: "test",
		Logger:    logger,
	})
	w.RegisterSchema(iarrow.TableTraces, iarrow.TracesSchema(iarrow.DefaultTracesPromoted))

	rec := makeTracesRecord(10)
	defer rec.Release()

	bytes, err := w.Flush(context.Background(), iarrow.TableTraces, []arrowlib.RecordBatch{rec}, 10)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if bytes <= 0 {
		t.Error("expected positive bytes written")
	}

	// Should have written one Parquet file (all rows in same partition)
	if fio.fileCount() != 1 {
		t.Errorf("expected 1 file, got %d", fio.fileCount())
	}
}

func TestWriterFlushEmpty(t *testing.T) {
	fio := newMemoryFileIO()
	cat := iceberg.NewNoopCatalog()
	logger := zaptest.NewLogger(t)

	w := New(Config{
		FileIO:    fio,
		Catalog:   cat,
		Namespace: "test",
		Logger:    logger,
	})

	bytes, err := w.Flush(context.Background(), iarrow.TableTraces, nil, 0)
	if err != nil {
		t.Fatalf("Flush empty failed: %v", err)
	}
	if bytes != 0 {
		t.Errorf("expected 0 bytes for empty flush, got %d", bytes)
	}
}
