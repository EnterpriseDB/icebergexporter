// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// defaultBytesPerRow is the conservative pre-calibration estimate used when
// no successful flush has happened yet to derive a per-row byte cost.
const defaultBytesPerRow = 256

// FlushOp is the caller-supplied function invoked by FlushVia with the
// records to be flushed. It returns the actual Parquet bytes written (used
// for bytes-per-row calibration) or an error.
type FlushOp func(records []arrow.RecordBatch, rows int64) (parquetBytes int64, err error)

// SignalBuffer accumulates Arrow records for a single table and tracks an
// estimated byte size derived from store row count × calibrated
// bytes-per-row. Storage is delegated to a recordStore (memStore by default;
// diskStore for persistent buffers).
//
// Concurrency:
//   - mu guards bytesPerRow and per-op store calls (Append, IsEmpty, Rows).
//   - flushMu serialises drain/commit cycles. FlushVia holds flushMu for the
//     entire drain → op → commit sequence, so at most one drain is in flight.
//     Add can proceed concurrently with the op (it acquires only mu, not flushMu).
type SignalBuffer struct {
	mu      sync.Mutex
	flushMu sync.Mutex

	table       string
	store       recordStore
	bytesPerRow float64 // calibrated after first flush; zero means uncalibrated
}

// NewSignalBuffer creates a buffer for the given table name with an in-memory store.
func NewSignalBuffer(table string) *SignalBuffer {
	return newSignalBufferWithStore(table, newMemStore())
}

// newSignalBufferWithStore is for internal use — allows wiring a non-default store.
func newSignalBufferWithStore(table string, store recordStore) *SignalBuffer {
	return &SignalBuffer{table: table, store: store}
}

// Table returns the table name for this buffer.
func (b *SignalBuffer) Table() string {
	return b.table
}

// Add appends a record to the buffer. The record is retained by the store
// (in-memory) or serialised to disk (disk-backed) before the call returns.
func (b *SignalBuffer) Add(rec arrow.RecordBatch) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.store.Append(rec)
}

// FlushVia drains the buffer and runs op against the drained records. On
// op success the records are committed (discarded from the store, refs
// released) and the bytes-per-row calibration is updated. On op failure or
// drain failure, records remain drainable for retry. Concurrent FlushVia
// calls on the same buffer serialise via flushMu.
//
// Returns the row count that was passed to op (zero for an empty drain) and
// any drain or op error. The row count lets callers skip telemetry emission
// when there was nothing to do.
//
// FlushVia is panic-safe: if op panics, drained records are still released
// before the panic propagates.
func (b *SignalBuffer) FlushVia(op FlushOp) (rows int64, err error) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	records, drainedRows, commit, drainErr := b.store.Drain()
	b.mu.Unlock()
	if drainErr != nil {
		return 0, drainErr
	}

	// Records belong to the caller from this point — release on every exit
	// path (success, failure, or panic from op).
	defer func() {
		for _, rec := range records {
			rec.Release()
		}
	}()

	if len(records) == 0 {
		// Nothing drained — call commit for symmetry (no-op for both backends).
		b.mu.Lock()
		commit()
		b.mu.Unlock()
		return 0, nil
	}

	parquetBytes, opErr := op(records, drainedRows)

	if opErr == nil {
		b.mu.Lock()
		commit()
		b.mu.Unlock()
		if parquetBytes > 0 {
			b.calibrate(parquetBytes, drainedRows)
		}
		return drainedRows, nil
	}
	// On failure, no commit; records stay in the store's draining set for
	// the next attempt.
	return drainedRows, opErr
}

// Metrics is a snapshot of per-buffer telemetry counters.
type Metrics struct {
	// Rows is the current total row count (active + drained-but-not-committed).
	Rows int64
	// PendingFiles, PendingBytes, OldestPendingAgeSeconds are populated only
	// for disk-backed buffers; in-memory buffers always report zeros.
	PendingFiles            int
	PendingBytes            int64
	OldestPendingAgeSeconds float64
}

// Metrics returns a snapshot of the buffer's telemetry counters. Safe to call
// concurrently with Add/FlushVia.
func (b *SignalBuffer) Metrics() Metrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	sm := b.store.Metrics()
	return Metrics{
		Rows:                    b.store.Rows(),
		PendingFiles:            sm.PendingFiles,
		PendingBytes:            sm.PendingBytes,
		OldestPendingAgeSeconds: sm.OldestPendingAgeSeconds,
	}
}

// EstimatedSize returns the estimated total buffer size in bytes (active +
// drained-but-not-committed records). Computed from the store's row count
// times the calibrated bytes-per-row, so disk-recovered records contribute.
func (b *SignalBuffer) EstimatedSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.estimatedSizeLocked()
}

// estimatedSizeLocked computes the size estimate. Caller must hold mu.
func (b *SignalBuffer) estimatedSizeLocked() int64 {
	rows := b.store.Rows()
	if b.bytesPerRow > 0 {
		return int64(float64(rows) * b.bytesPerRow)
	}
	return rows * defaultBytesPerRow
}

// Rows returns the total number of buffered rows.
func (b *SignalBuffer) Rows() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.store.Rows()
}

// IsEmpty returns true if the buffer holds no records.
func (b *SignalBuffer) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.store.IsEmpty()
}

// calibrate updates the bytes-per-row estimate from actual Parquet output size.
// Internal — invoked by FlushVia after a successful op.
func (b *SignalBuffer) calibrate(parquetBytes int64, rows int64) {
	if rows <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// Exponential moving average for stability
	newBPR := float64(parquetBytes) / float64(rows)
	if b.bytesPerRow == 0 {
		b.bytesPerRow = newBPR
	} else {
		b.bytesPerRow = 0.7*b.bytesPerRow + 0.3*newBPR
	}
}
