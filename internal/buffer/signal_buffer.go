// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// FlushOp is the caller-supplied function invoked by FlushVia with the
// records to be flushed. It returns the actual Parquet bytes written (used
// for bytes-per-row calibration) or an error.
type FlushOp func(records []arrow.RecordBatch, rows int64) (parquetBytes int64, err error)

// SignalBuffer accumulates Arrow records for a single table and tracks
// estimated size. It calibrates bytes-per-row after the first Parquet write.
// Storage is delegated to a recordStore (memStore by default; diskStore for
// persistent buffers).
//
// Concurrency:
//   - mu guards the size estimate and per-op store calls (Append, IsEmpty, Rows).
//   - flushMu serialises drain/commit cycles. FlushVia holds flushMu for the
//     entire drain → op → commit sequence, so at most one drain is in flight.
//     Add can proceed concurrently with the op (it acquires only mu, not flushMu).
type SignalBuffer struct {
	mu      sync.Mutex
	flushMu sync.Mutex

	table       string
	store       recordStore
	sizeActive  int64   // estimated bytes of records in store's active set
	sizeDrain   int64   // estimated bytes of records in store's draining set
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
	numRows := rec.NumRows()
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := b.store.Append(rec); err != nil {
		return err
	}

	if b.bytesPerRow > 0 {
		b.sizeActive += int64(float64(numRows) * b.bytesPerRow)
	} else {
		// Before calibration, use a rough estimate of 256 bytes per row
		b.sizeActive += numRows * 256
	}
	return nil
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
func (b *SignalBuffer) FlushVia(op FlushOp) (int64, error) {
	b.flushMu.Lock()
	defer b.flushMu.Unlock()

	b.mu.Lock()
	records, rows, commit, err := b.store.Drain()
	if err != nil {
		b.mu.Unlock()
		return 0, err
	}
	// The store moved active records into draining as part of Drain; mirror
	// that in our size accounting.
	b.sizeDrain += b.sizeActive
	b.sizeActive = 0
	b.mu.Unlock()

	if len(records) == 0 {
		// Empty drain — still call commit for symmetry (no-op).
		b.mu.Lock()
		commit()
		b.sizeDrain = 0
		b.mu.Unlock()
		return 0, nil
	}

	parquetBytes, opErr := op(records, rows)

	b.mu.Lock()
	if opErr == nil {
		// Success: discard drained records from the store and zero the
		// draining-side size estimate.
		commit()
		b.sizeDrain = 0
	}
	// On failure, no commit; sizeDrain stays as set above so EstimatedSize
	// continues to reflect the still-buffered records.
	b.mu.Unlock()

	// Records belong to the caller after Drain — release them unconditionally.
	// memStore keeps its own refs internally for failed-flush re-drains;
	// diskStore reads fresh records on every Drain.
	for _, rec := range records {
		rec.Release()
	}

	if opErr != nil {
		return rows, opErr
	}

	if parquetBytes > 0 {
		b.calibrate(parquetBytes, rows)
	}
	return rows, nil
}

// BufferMetrics is a snapshot of per-buffer telemetry counters.
type BufferMetrics struct {
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
func (b *SignalBuffer) Metrics() BufferMetrics {
	b.mu.Lock()
	defer b.mu.Unlock()
	sm := b.store.Metrics()
	return BufferMetrics{
		Rows:                    b.store.Rows(),
		PendingFiles:            sm.PendingFiles,
		PendingBytes:            sm.PendingBytes,
		OldestPendingAgeSeconds: sm.OldestPendingAgeSeconds,
	}
}

// EstimatedSize returns the estimated buffer size in bytes (active + drained-but-not-committed).
func (b *SignalBuffer) EstimatedSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sizeActive + b.sizeDrain
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
