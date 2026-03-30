// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
)

// SignalBuffer accumulates Arrow records for a single table and tracks
// estimated size. It calibrates bytes-per-row after the first Parquet write.
type SignalBuffer struct {
	mu      sync.Mutex
	table   string
	records []arrow.RecordBatch
	rows    int64
	size    int64 // estimated bytes

	// bytesPerRow is calibrated after the first flush by comparing actual
	// Parquet bytes to the number of rows written. Zero means uncalibrated.
	bytesPerRow float64
}

// NewSignalBuffer creates a buffer for the given table name.
func NewSignalBuffer(table string) *SignalBuffer {
	return &SignalBuffer{table: table}
}

// Table returns the table name for this buffer.
func (b *SignalBuffer) Table() string {
	return b.table
}

// Add appends a record to the buffer. The record is retained (ref count incremented).
func (b *SignalBuffer) Add(rec arrow.RecordBatch) {
	rec.Retain()
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, rec)
	numRows := rec.NumRows()
	b.rows += numRows

	if b.bytesPerRow > 0 {
		b.size += int64(float64(numRows) * b.bytesPerRow)
	} else {
		// Before calibration, use a rough estimate of 256 bytes per row
		b.size += numRows * 256
	}
}

// Drain removes all records from the buffer and returns them along with total
// row count. Caller is responsible for releasing the returned records.
func (b *SignalBuffer) Drain() ([]arrow.RecordBatch, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	records := b.records
	rows := b.rows
	b.records = nil
	b.rows = 0
	b.size = 0
	return records, rows
}

// Reappend puts records back into the buffer (e.g. after a failed flush).
// Records are retained again.
func (b *SignalBuffer) Reappend(records []arrow.RecordBatch) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, rec := range records {
		rec.Retain()
		b.records = append(b.records, rec)
		b.rows += rec.NumRows()
		if b.bytesPerRow > 0 {
			b.size += int64(float64(rec.NumRows()) * b.bytesPerRow)
		} else {
			b.size += rec.NumRows() * 256
		}
	}
}

// EstimatedSize returns the estimated buffer size in bytes.
func (b *SignalBuffer) EstimatedSize() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.size
}

// Rows returns the total number of buffered rows.
func (b *SignalBuffer) Rows() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rows
}

// IsEmpty returns true if the buffer holds no records.
func (b *SignalBuffer) IsEmpty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.records) == 0
}

// Calibrate updates the bytes-per-row estimate from actual Parquet output size.
func (b *SignalBuffer) Calibrate(parquetBytes int64, rows int64) {
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
