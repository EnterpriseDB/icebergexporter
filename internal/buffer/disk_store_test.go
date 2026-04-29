// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func mustNewDiskStore(t *testing.T, dir string) *diskStore {
	t.Helper()
	s, err := newDiskStore(dir, memory.DefaultAllocator)
	if err != nil {
		t.Fatalf("newDiskStore: %v", err)
	}
	return s
}

func listPendingFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var pending []string
	for _, e := range entries {
		if pendingFilenameRE.MatchString(e.Name()) {
			pending = append(pending, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(pending)
	return pending
}

func TestDiskStoreBasicAppendFlushCommit(t *testing.T) {
	dir := t.TempDir()
	buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))

	rec := makeTestRecord(10)
	defer rec.Release()
	if err := buf.Add(rec); err != nil {
		t.Fatal(err)
	}

	if err := buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		if rows != 10 {
			t.Errorf("expected 10 rows, got %d", rows)
		}
		if len(records) != 1 {
			t.Errorf("expected 1 record, got %d", len(records))
		}
		return 100, nil
	}); err != nil {
		t.Fatal(err)
	}

	if !buf.IsEmpty() {
		t.Error("buffer should be empty after successful flush")
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("expected empty spill dir after commit, found: %v", names)
	}
}

func TestDiskStoreFailedFlushKeepsPendingFile(t *testing.T) {
	dir := t.TempDir()
	buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))

	rec := makeTestRecord(7)
	defer rec.Release()
	if err := buf.Add(rec); err != nil {
		t.Fatal(err)
	}

	failErr := errors.New("simulated failure")
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, failErr
	}); !errors.Is(err, failErr) {
		t.Fatalf("expected failErr, got %v", err)
	}

	if buf.IsEmpty() {
		t.Error("buffer should not be empty after failed flush")
	}
	if buf.Rows() != 7 {
		t.Errorf("expected 7 rows, got %d", buf.Rows())
	}

	pending := listPendingFiles(t, dir)
	if len(pending) != 1 {
		t.Errorf("expected 1 pending file, got %d: %v", len(pending), pending)
	}

	// Retry succeeds — pending file consumed.
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, rows int64) (int64, error) {
		if rows != 7 {
			t.Errorf("retry expected 7 rows, got %d", rows)
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}

	if !buf.IsEmpty() {
		t.Error("buffer should be empty after successful retry")
	}
	if got := len(listPendingFiles(t, dir)); got != 0 {
		t.Errorf("expected 0 pending files after commit, got %d", got)
	}
}

func TestDiskStoreFailedFlushPlusNewAppendsCombineOnRetry(t *testing.T) {
	dir := t.TempDir()
	buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))

	rec1 := makeTestRecord(7)
	defer rec1.Release()
	if err := buf.Add(rec1); err != nil {
		t.Fatal(err)
	}

	_ = buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, errors.New("fail")
	})

	rec2 := makeTestRecord(3)
	defer rec2.Release()
	if err := buf.Add(rec2); err != nil {
		t.Fatal(err)
	}

	if buf.Rows() != 10 {
		t.Errorf("expected 10 rows total, got %d", buf.Rows())
	}

	if err := buf.FlushVia(func(_ []arrow.RecordBatch, rows int64) (int64, error) {
		if rows != 10 {
			t.Errorf("retry expected 10 rows, got %d", rows)
		}
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if !buf.IsEmpty() {
		t.Error("buffer should be empty after combined retry")
	}
}

func TestDiskStoreRecoversPendingFiles(t *testing.T) {
	dir := t.TempDir()

	// First "process": append, fail flush, leave a pending file.
	{
		buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))
		rec := makeTestRecord(11)
		defer rec.Release()
		if err := buf.Add(rec); err != nil {
			t.Fatal(err)
		}
		_ = buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
			return 0, errors.New("fail")
		})
	}

	if got := len(listPendingFiles(t, dir)); got != 1 {
		t.Fatalf("expected 1 pending file before recovery, got %d", got)
	}

	// Second "process": new diskStore should recover the pending file.
	buf2 := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))
	if buf2.IsEmpty() {
		t.Error("recovered buffer should not be empty")
	}
	if buf2.Rows() != 11 {
		t.Errorf("expected 11 rows recovered, got %d", buf2.Rows())
	}

	var seenRows int64
	if err := buf2.FlushVia(func(_ []arrow.RecordBatch, rows int64) (int64, error) {
		seenRows = rows
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if seenRows != 11 {
		t.Errorf("expected 11 rows in recovered drain, got %d", seenRows)
	}
	if !buf2.IsEmpty() {
		t.Error("buffer should be empty after drain")
	}
}

func TestDiskStoreRecoversOrphanedActive(t *testing.T) {
	dir := t.TempDir()

	// First "process": append but never drain (simulates crash with data in active.ipc).
	{
		store := mustNewDiskStore(t, dir)
		rec := makeTestRecord(13)
		defer rec.Release()
		if err := store.Append(rec); err != nil {
			t.Fatal(err)
		}
		store.Close() // closes writer + file but leaves files on disk
	}

	activePath := filepath.Join(dir, activeFilename)
	if _, err := os.Stat(activePath); err != nil {
		t.Fatalf("expected active.ipc to exist before recovery, got: %v", err)
	}

	// Recovery: orphaned active should be promoted to pending.
	store2 := mustNewDiskStore(t, dir)
	if _, err := os.Stat(activePath); !os.IsNotExist(err) {
		t.Errorf("expected active.ipc to be gone after recovery, got err=%v", err)
	}
	if got := len(listPendingFiles(t, dir)); got != 1 {
		t.Errorf("expected 1 pending file after recovery, got %d", got)
	}

	buf := newSignalBufferWithStore("test", store2)
	var rows int64
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, r int64) (int64, error) {
		rows = r
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 13 {
		t.Errorf("expected 13 rows recovered, got %d", rows)
	}
}

func TestDiskStoreSchemaMismatchRejected(t *testing.T) {
	dir := t.TempDir()
	store := mustNewDiskStore(t, dir)

	rec := makeTestRecord(5)
	defer rec.Release()
	if err := store.Append(rec); err != nil {
		t.Fatal(err)
	}

	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "different", Type: arrow.PrimitiveTypes.Float64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema2)
	b.Field(0).(*array.Float64Builder).Append(1.0)
	rec2 := b.NewRecordBatch()
	defer rec2.Release()
	b.Release()

	if err := store.Append(rec2); err == nil {
		t.Fatal("expected schema mismatch error, got nil")
	}
}

func TestDiskStoreEmptyDrainNoop(t *testing.T) {
	dir := t.TempDir()
	buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))

	called := false
	if err := buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
		called = true
		return 0, nil
	}); err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("op should not be called for empty drain")
	}
}

func TestDiskStoreSequenceMonotonic(t *testing.T) {
	dir := t.TempDir()
	buf := newSignalBufferWithStore("test", mustNewDiskStore(t, dir))

	for i := 0; i < 3; i++ {
		rec := makeTestRecord(2)
		if err := buf.Add(rec); err != nil {
			t.Fatal(err)
		}
		rec.Release()
		_ = buf.FlushVia(func(_ []arrow.RecordBatch, _ int64) (int64, error) {
			return 0, errors.New("fail")
		})
	}

	pending := listPendingFiles(t, dir)
	expected := []string{"pending-000001.ipc", "pending-000002.ipc", "pending-000003.ipc"}
	if len(pending) != len(expected) {
		t.Fatalf("expected %d pending files, got %d", len(expected), len(pending))
	}
	for i, p := range pending {
		if filepath.Base(p) != expected[i] {
			t.Errorf("pending[%d]: expected %s, got %s", i, expected[i], filepath.Base(p))
		}
	}
}
