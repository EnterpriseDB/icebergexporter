// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap/zaptest"
)

func newTestManager(t *testing.T, maxSize int64, flushFn FlushFunc) *Manager {
	t.Helper()
	mgr, err := NewManager(ManagerOptions{
		MaxSizeBytes:  maxSize,
		FlushInterval: time.Hour,
	}, flushFn, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return mgr
}

func TestManagerAddAndFlush(t *testing.T) {
	var flushCount atomic.Int32
	flushFn := func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (int64, error) {
		flushCount.Add(1)
		return totalRows * 50, nil
	}

	mgr := newTestManager(t, 0, flushFn) // size=0 means no size-triggered flush
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := mgr.Stop(context.Background()); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	}()

	rec := makeTestRecord(10)
	defer rec.Release()

	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if flushCount.Load() != 0 {
		t.Errorf("expected 0 flushes, got %d", flushCount.Load())
	}
}

func TestManagerSizeTriggeredFlush(t *testing.T) {
	var flushCount atomic.Int32
	flushFn := func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (int64, error) {
		flushCount.Add(1)
		return totalRows * 50, nil
	}

	// Threshold of 100 bytes; each rec(10) is 10 rows × 256 default bpr = 2560 estimated.
	mgr := newTestManager(t, 100, flushFn)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() {
		if err := mgr.Stop(context.Background()); err != nil {
			t.Fatalf("Stop failed: %v", err)
		}
	}()

	rec := makeTestRecord(10)
	defer rec.Release()

	// First Add: buffer was empty (0 < 100 threshold), no pre-flush triggered.
	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if flushCount.Load() != 0 {
		t.Errorf("expected 0 flushes after first Add (buffer was empty), got %d", flushCount.Load())
	}

	// Second Add: buffer now ≥ threshold, pre-Add flush should trigger before
	// the new record is buffered.
	rec2 := makeTestRecord(10)
	defer rec2.Release()
	if err := mgr.Add(context.Background(), "test_table", rec2); err != nil {
		t.Fatalf("second Add failed: %v", err)
	}
	if flushCount.Load() != 1 {
		t.Errorf("expected 1 size-triggered flush after second Add, got %d", flushCount.Load())
	}
}

func TestManagerStopDrains(t *testing.T) {
	var flushedRows atomic.Int64
	flushFn := func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (int64, error) {
		flushedRows.Add(totalRows)
		return totalRows * 50, nil
	}

	mgr := newTestManager(t, 0, flushFn)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rec := makeTestRecord(42)
	defer rec.Release()

	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if flushedRows.Load() != 42 {
		t.Errorf("expected 42 rows flushed on stop, got %d", flushedRows.Load())
	}
}

func TestManagerMultipleTables(t *testing.T) {
	flushed := make(map[string]int64)
	flushFn := func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (int64, error) {
		flushed[table] += totalRows
		return totalRows * 50, nil
	}

	mgr := newTestManager(t, 0, flushFn)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rec1 := makeTestRecord(10)
	defer rec1.Release()
	rec2 := makeTestRecord(20)
	defer rec2.Release()

	if err := mgr.Add(context.Background(), "traces", rec1); err != nil {
		t.Fatalf("Add traces failed: %v", err)
	}
	if err := mgr.Add(context.Background(), "logs", rec2); err != nil {
		t.Fatalf("Add logs failed: %v", err)
	}

	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if flushed["traces"] != 10 {
		t.Errorf("expected 10 trace rows, got %d", flushed["traces"])
	}
	if flushed["logs"] != 20 {
		t.Errorf("expected 20 log rows, got %d", flushed["logs"])
	}
}

func TestManagerWithDiskStorage(t *testing.T) {
	dir := t.TempDir()
	flushed := make(map[string]int64)
	flushFn := func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (int64, error) {
		flushed[table] += totalRows
		return totalRows * 50, nil
	}

	mgr, err := NewManager(ManagerOptions{
		MaxSizeBytes:  0,
		FlushInterval: time.Hour,
		Storage: StorageOptions{
			Type: StorageDisk,
			Path: dir,
		},
	}, flushFn, zaptest.NewLogger(t))
	if err != nil {
		t.Fatalf("NewManager with disk storage: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	rec := makeTestRecord(15)
	defer rec.Release()

	if err := mgr.Add(context.Background(), "otel_traces", rec); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}

	if flushed["otel_traces"] != 15 {
		t.Errorf("expected 15 rows flushed, got %d", flushed["otel_traces"])
	}
}

func TestManagerStartIsIdempotent(t *testing.T) {
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, nil
	}
	mgr := newTestManager(t, 0, flushFn)
	if err := mgr.Start(); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("second Start should be no-op, got: %v", err)
	}
	// A third call too — definitely shouldn't double-register or double-spawn.
	if err := mgr.Start(); err != nil {
		t.Fatalf("third Start should be no-op, got: %v", err)
	}
	if err := mgr.Stop(context.Background()); err != nil {
		t.Fatalf("Stop failed: %v", err)
	}
}

func TestManagerAddBackpressuresWhenFlushFailsAtCap(t *testing.T) {
	failErr := errors.New("synthetic flush failure")
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, failErr
	}
	// Threshold of 100 bytes; first rec(10) fits, subsequent Adds hit the cap
	// and trigger the failing flush — those Adds must be rejected, not buffered.
	mgr := newTestManager(t, 100, flushFn)
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	rec := makeTestRecord(10)
	defer rec.Release()
	if err := mgr.Add(context.Background(), "test_table", rec); err != nil {
		t.Fatalf("first Add failed: %v", err)
	}
	rowsBefore := mgr.buffers["test_table"].Rows()
	if rowsBefore != 10 {
		t.Fatalf("expected 10 rows before failing-flush Add, got %d", rowsBefore)
	}

	rec2 := makeTestRecord(10)
	defer rec2.Release()
	err := mgr.Add(context.Background(), "test_table", rec2)
	if err == nil {
		t.Fatal("expected Add to fail when at-cap flush fails")
	}
	if !errors.Is(err, failErr) {
		t.Fatalf("expected wrapped failErr, got %v", err)
	}
	// The rejected record must NOT be in the buffer (otherwise OTel retries
	// would duplicate it). Existing 10 rows from the first Add remain because
	// the flush failed without committing.
	rowsAfter := mgr.buffers["test_table"].Rows()
	if rowsAfter != 10 {
		t.Errorf("expected 10 rows after rejected Add (no double-buffer), got %d", rowsAfter)
	}
}

func TestManagerRejectsInvalidOptions(t *testing.T) {
	logger := zaptest.NewLogger(t)
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, nil
	}

	cases := []struct {
		name string
		opts ManagerOptions
	}{
		{
			name: "negative max size",
			opts: ManagerOptions{MaxSizeBytes: -1, FlushInterval: time.Hour},
		},
		{
			name: "negative flush interval",
			opts: ManagerOptions{MaxSizeBytes: 0, FlushInterval: -time.Second},
		},
		{
			name: "disk without path",
			opts: ManagerOptions{
				MaxSizeBytes:  0,
				FlushInterval: time.Hour,
				Storage:       StorageOptions{Type: StorageDisk},
			},
		},
		{
			name: "unknown storage type",
			opts: ManagerOptions{
				MaxSizeBytes:  0,
				FlushInterval: time.Hour,
				Storage:       StorageOptions{Type: "s3"},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewManager(tc.opts, flushFn, logger); err == nil {
				t.Errorf("expected error for %s, got nil", tc.name)
			}
		})
	}
}
