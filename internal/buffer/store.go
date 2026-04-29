// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import "github.com/apache/arrow-go/v18/arrow"

// recordStore is the storage backend for a per-table buffer. Implementations
// may keep records in memory (memStore) or persist them on disk (diskStore).
//
// The store is not internally synchronised; callers (SignalBuffer) provide
// outer locking. Drain/commit cycles are serialised by SignalBuffer's flushMu
// — implementations may rely on at most one drain being in-flight at a time.
type recordStore interface {
	// Append adds a record. The store retains the record (refcount + 1) for
	// in-memory backends; on-disk backends serialise the record and may
	// release their reference immediately.
	Append(rec arrow.RecordBatch) error

	// Drain returns all currently-stored records along with the total row
	// count and a commit function. Records are held in a "drained" state until
	// commit is called; if commit is never called (e.g. the consumer fails),
	// the records remain part of the store and a subsequent Drain will return
	// them again.
	//
	// Caller protocol:
	//   - On success consuming records (e.g. remote flush succeeded): call commit().
	//   - On failure: do NOT call commit. Records remain drainable for retry.
	//
	// Caller is responsible for releasing the returned records.
	Drain() (records []arrow.RecordBatch, rows int64, commit func(), err error)

	// Rows returns the current total row count (active + drained-but-not-committed).
	Rows() int64

	// IsEmpty reports whether the store holds zero records in either set.
	IsEmpty() bool

	// Close releases any resources held by the store. After Close, further
	// calls are undefined.
	Close()
}
