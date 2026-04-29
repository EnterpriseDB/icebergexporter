// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"errors"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func makeTestRecord(nRows int) arrow.RecordBatch {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "val", Type: arrow.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	for i := 0; i < nRows; i++ {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
	}
	return b.NewRecordBatch()
}

func TestSignalBufferAddState(t *testing.T) {
	buf := NewSignalBuffer("test_table")
	if !buf.IsEmpty() {
		t.Error("new buffer should be empty")
	}

	rec := makeTestRecord(10)
	defer rec.Release()

	if err := buf.Add(rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if buf.IsEmpty() {
		t.Error("buffer should not be empty after Add")
	}
	if buf.Rows() != 10 {
		t.Errorf("expected 10 rows, got %d", buf.Rows())
	}
	if buf.EstimatedSize() <= 0 {
		t.Error("estimated size should be positive")
	}

	// Drain via FlushVia with a successful op so cleanup runs.
	if err := buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		if len(records) != 1 {
			t.Errorf("expected 1 record in op, got %d", len(records))
		}
		if rows != 10 {
			t.Errorf("expected 10 rows in op, got %d", rows)
		}
		return 0, nil
	}); err != nil {
		t.Fatalf("FlushVia failed: %v", err)
	}

	if !buf.IsEmpty() {
		t.Error("buffer should be empty after successful FlushVia")
	}
}

func TestSignalBufferFlushViaSuccess(t *testing.T) {
	buf := NewSignalBuffer("test")
	rec := makeTestRecord(5)
	defer rec.Release()

	if err := buf.Add(rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		if rows != 5 {
			t.Errorf("expected 5 rows, got %d", rows)
		}
		return 250, nil
	}); err != nil {
		t.Fatalf("FlushVia failed: %v", err)
	}

	if !buf.IsEmpty() {
		t.Error("buffer should be empty after successful flush")
	}
	if buf.Rows() != 0 {
		t.Errorf("expected 0 rows after flush, got %d", buf.Rows())
	}
	if buf.EstimatedSize() != 0 {
		t.Errorf("expected zero estimated size after flush, got %d", buf.EstimatedSize())
	}
}

func TestSignalBufferFlushViaFailureRetry(t *testing.T) {
	buf := NewSignalBuffer("test")
	rec := makeTestRecord(7)
	defer rec.Release()

	if err := buf.Add(rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// First flush fails — records must remain drainable.
	failErr := errors.New("simulated flush failure")
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, failErr
	}); !errors.Is(err, failErr) {
		t.Fatalf("expected wrapped failErr, got %v", err)
	}

	if buf.IsEmpty() {
		t.Error("buffer should not be empty after failed flush")
	}
	if buf.Rows() != 7 {
		t.Errorf("expected 7 rows after failed flush, got %d", buf.Rows())
	}

	// Adding more records during the "failed" state — they should be drained
	// alongside the prior failed-flush records on the retry.
	rec2 := makeTestRecord(3)
	defer rec2.Release()
	if err := buf.Add(rec2); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if buf.Rows() != 10 {
		t.Errorf("expected 10 rows total (7 failed + 3 new), got %d", buf.Rows())
	}

	// Retry flush succeeds — should see all 10 rows.
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, rows int64) (int64, error) {
		if rows != 10 {
			t.Errorf("retry expected 10 rows, got %d", rows)
		}
		return 500, nil
	}); err != nil {
		t.Fatalf("retry FlushVia failed: %v", err)
	}

	if !buf.IsEmpty() {
		t.Error("buffer should be empty after successful retry")
	}
}

func TestSignalBufferCalibration(t *testing.T) {
	buf := NewSignalBuffer("test")

	rec := makeTestRecord(100)
	defer rec.Release()
	if err := buf.Add(rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	defaultSize := buf.EstimatedSize()

	// First flush calibrates: 5000 bytes for 100 rows = 50 bytes/row.
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		return 5000, nil
	}); err != nil {
		t.Fatalf("FlushVia failed: %v", err)
	}

	// Add again — calibrated estimate should be lower than the default.
	if err := buf.Add(rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	calibratedSize := buf.EstimatedSize()

	if calibratedSize >= defaultSize {
		t.Errorf("calibrated size (%d) should be less than default estimate (%d)", calibratedSize, defaultSize)
	}

	// Drain leftover.
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, nil
	}); err != nil {
		t.Fatalf("cleanup FlushVia failed: %v", err)
	}
}

func TestSignalBufferEmptyFlushViaIsNoop(t *testing.T) {
	buf := NewSignalBuffer("test")

	called := false
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		called = true
		return 0, nil
	}); err != nil {
		t.Fatalf("FlushVia on empty buffer failed: %v", err)
	}
	if called {
		t.Error("op should not be invoked when buffer is empty")
	}
}
