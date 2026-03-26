// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap/zaptest"
)

func TestManagerAddAndFlush(t *testing.T) {
	var flushCount atomic.Int32
	flushFn := func(ctx context.Context, table string, records []arrow.Record, totalRows int64) (int64, error) {
		flushCount.Add(1)
		return totalRows * 50, nil
	}

	logger := zaptest.NewLogger(t)
	mgr := NewManager(0, time.Hour, flushFn, logger) // size=0 means no size-triggered flush
	mgr.Start()
	defer mgr.Stop(context.Background())

	rec := makeTestRecord(10)
	defer rec.Release()

	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	// No flush should have happened (size threshold disabled)
	if flushCount.Load() != 0 {
		t.Errorf("expected 0 flushes, got %d", flushCount.Load())
	}
}

func TestManagerSizeTriggeredFlush(t *testing.T) {
	var flushCount atomic.Int32
	flushFn := func(ctx context.Context, table string, records []arrow.Record, totalRows int64) (int64, error) {
		flushCount.Add(1)
		return totalRows * 50, nil
	}

	logger := zaptest.NewLogger(t)
	// Set very low size threshold to trigger flush
	mgr := NewManager(100, time.Hour, flushFn, logger)
	mgr.Start()
	defer mgr.Stop(context.Background())

	rec := makeTestRecord(10)
	defer rec.Release()

	// This should trigger a size-based flush (10 rows * 256 default = 2560 > 100)
	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if flushCount.Load() != 1 {
		t.Errorf("expected 1 size-triggered flush, got %d", flushCount.Load())
	}
}

func TestManagerStopDrains(t *testing.T) {
	var flushedRows atomic.Int64
	flushFn := func(ctx context.Context, table string, records []arrow.Record, totalRows int64) (int64, error) {
		flushedRows.Add(totalRows)
		return totalRows * 50, nil
	}

	logger := zaptest.NewLogger(t)
	mgr := NewManager(0, time.Hour, flushFn, logger) // No auto-flush
	mgr.Start()

	rec := makeTestRecord(42)
	defer rec.Release()

	mgr.Add(context.Background(), "test_table", rec)

	// Stop should drain
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if flushedRows.Load() != 42 {
		t.Errorf("expected 42 rows flushed on stop, got %d", flushedRows.Load())
	}
}

func TestManagerMultipleTables(t *testing.T) {
	flushed := make(map[string]int64)
	flushFn := func(ctx context.Context, table string, records []arrow.Record, totalRows int64) (int64, error) {
		flushed[table] += totalRows
		return totalRows * 50, nil
	}

	logger := zaptest.NewLogger(t)
	mgr := NewManager(0, time.Hour, flushFn, logger)
	mgr.Start()

	rec1 := makeTestRecord(10)
	defer rec1.Release()
	rec2 := makeTestRecord(20)
	defer rec2.Release()

	mgr.Add(context.Background(), "traces", rec1)
	mgr.Add(context.Background(), "logs", rec2)

	mgr.Stop(context.Background())

	if flushed["traces"] != 10 {
		t.Errorf("expected 10 trace rows, got %d", flushed["traces"])
	}
	if flushed["logs"] != 20 {
		t.Errorf("expected 20 log rows, got %d", flushed["logs"])
	}
}
