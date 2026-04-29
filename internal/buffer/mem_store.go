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

	// Hand the caller their own refs (caller releases unconditionally —
	// regardless of commit). memStore keeps its own refs in m.draining so
	// failed flushes can re-drain.
	snapshot := make([]arrow.RecordBatch, len(m.draining))
	copy(snapshot, m.draining)
	for _, rec := range snapshot {
		rec.Retain()
	}
	rows := m.drainingRows

	// Capture for the commit closure. flushMu serialisation guarantees no
	// overlapping drain happens between Drain returning and commit, so this
	// slice is stable.
	drainedRefs := m.draining

	commit := func() {
		// On successful commit: release memStore's refs and clear draining.
		for _, rec := range drainedRefs {
			rec.Release()
		}
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

// Metrics on memStore always reports zeros — there are no pending files for
// the in-memory backend.
func (m *memStore) Metrics() StoreMetrics {
	return StoreMetrics{}
}
