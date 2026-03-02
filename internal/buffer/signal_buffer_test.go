package buffer

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeTestRecord(nRows int) arrow.Record {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < nRows; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	return b.NewRecord()
}

func TestSignalBufferAddDrain(t *testing.T) {
	buf := NewSignalBuffer("test_table")
	if !buf.IsEmpty() {
		t.Error("new buffer should be empty")
	}

	rec := makeTestRecord(10)
	defer rec.Release()

	buf.Add(rec)
	if buf.IsEmpty() {
		t.Error("buffer should not be empty after Add")
	}
	if buf.Rows() != 10 {
		t.Errorf("expected 10 rows, got %d", buf.Rows())
	}
	if buf.EstimatedSize() <= 0 {
		t.Error("estimated size should be positive")
	}

	records, rows := buf.Drain()
	if len(records) != 1 {
		t.Errorf("expected 1 record, got %d", len(records))
	}
	if rows != 10 {
		t.Errorf("expected 10 rows from drain, got %d", rows)
	}
	if !buf.IsEmpty() {
		t.Error("buffer should be empty after drain")
	}

	// Release drained records
	for _, r := range records {
		r.Release()
	}
}

func TestSignalBufferReappend(t *testing.T) {
	buf := NewSignalBuffer("test")
	rec := makeTestRecord(5)
	defer rec.Release()

	buf.Add(rec)
	records, _ := buf.Drain()

	buf.Reappend(records)
	if buf.Rows() != 5 {
		t.Errorf("expected 5 rows after reappend, got %d", buf.Rows())
	}

	// Clean up
	drained, _ := buf.Drain()
	for _, r := range drained {
		r.Release()
	}
	for _, r := range records {
		r.Release()
	}
}

func TestSignalBufferCalibrate(t *testing.T) {
	buf := NewSignalBuffer("test")

	// Before calibration, default estimate
	rec := makeTestRecord(100)
	defer rec.Release()
	buf.Add(rec)
	defaultSize := buf.EstimatedSize()

	buf.Drain()

	// Calibrate with actual data
	buf.Calibrate(5000, 100) // 50 bytes per row

	// Add again — should use calibrated estimate
	buf.Add(rec)
	calibratedSize := buf.EstimatedSize()

	// Calibrated should be ~5000 (100 rows * 50 bpr), not default ~25600 (100 rows * 256)
	if calibratedSize >= defaultSize {
		t.Errorf("calibrated size (%d) should be less than default estimate (%d)", calibratedSize, defaultSize)
	}

	records, _ := buf.Drain()
	for _, r := range records {
		r.Release()
	}
}
