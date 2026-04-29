// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import "github.com/apache/arrow-go/v18/arrow"

// memStore is the default in-memory recordStore. It tracks two sets of
// records: "active" (newly Append'd, not yet drained) and "draining" (returned
// by a Drain call, awaiting commit). On Drain, active records are moved into
// draining and the entire draining set is returned. The commit function
// discards the draining set; failure to call commit leaves records drainable
// for the next attempt.
//
// Concurrency is provided by the enclosing SignalBuffer: SignalBuffer's mu
// guards individual operations and flushMu serialises drain/commit cycles, so
// at most one drain is in flight against this store at a time.
type memStore struct {
	active       []arrow.RecordBatch
	activeRows   int64
	draining     []arrow.RecordBatch
	drainingRows int64
}

func newMemStore() *memStore {
	return &memStore{}
}

func (m *memStore) Append(rec arrow.RecordBatch) error {
	rec.Retain()
	m.active = append(m.active, rec)
	m.activeRows += rec.NumRows()
	return nil
}

func (m *memStore) Drain() ([]arrow.RecordBatch, int64, func(), error) {
	// Move active into draining. If a previous drain failed, its records are
	// still in draining and get re-included alongside the new active records.
	m.draining = append(m.draining, m.active...)
	m.drainingRows += m.activeRows
	m.active = nil
	m.activeRows = 0

	// Snapshot the slice header for the commit closure to discard exactly
	// what was drained at this point. (Records are released by the caller.)
	snapshot := m.draining
	rows := m.drainingRows

	commit := func() {
		// flushMu serialisation guarantees no overlapping drain happened
		// between Drain returning and commit being called, so the draining
		// set still contains exactly snapshot.
		_ = snapshot // kept in closure for clarity; caller releases the records
		m.draining = nil
		m.drainingRows = 0
	}

	return snapshot, rows, commit, nil
}

func (m *memStore) Rows() int64 {
	return m.activeRows + m.drainingRows
}

func (m *memStore) IsEmpty() bool {
	return len(m.active) == 0 && len(m.draining) == 0
}

func (m *memStore) Close() {
	for _, rec := range m.active {
		rec.Release()
	}
	for _, rec := range m.draining {
		rec.Release()
	}
	m.active = nil
	m.draining = nil
	m.activeRows = 0
	m.drainingRows = 0
}
