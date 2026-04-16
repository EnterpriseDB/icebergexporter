// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"go.uber.org/zap"
)

// FlushFunc is called when a buffer needs flushing. It receives the table name
// and the accumulated records. Returns the number of Parquet bytes written
// (for calibration) or an error.
type FlushFunc func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (parquetBytes int64, err error)

// Manager implements a size+time hybrid buffer manager with per-table buffers.
type Manager struct {
	mu            sync.RWMutex
	buffers       map[string]*SignalBuffer
	maxSizeBytes  int64
	flushInterval time.Duration
	flushFn       FlushFunc
	logger        *zap.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// NewManager creates a buffer manager.
func NewManager(maxSizeBytes int, flushInterval time.Duration, flushFn FlushFunc, logger *zap.Logger) *Manager {
	return &Manager{
		buffers:       make(map[string]*SignalBuffer),
		maxSizeBytes:  int64(maxSizeBytes),
		flushInterval: flushInterval,
		flushFn:       flushFn,
		logger:        logger,
	}
}

// Start begins the background time-based flush goroutine.
func (m *Manager) Start() {
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})
	go m.flushLoop(ctx)
}

// Stop cancels the background flush goroutine, then drains all buffers.
func (m *Manager) Stop(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
	return m.flushAll(ctx)
}

// Add adds a record to the named table's buffer. If the buffer exceeds the
// size threshold, a synchronous flush is triggered and any error is returned
// to the caller (enabling OTel retry).
func (m *Manager) Add(ctx context.Context, table string, rec arrow.RecordBatch) error {
	buf := m.getOrCreateBuffer(table)
	buf.Add(rec)

	if m.maxSizeBytes > 0 && buf.EstimatedSize() >= m.maxSizeBytes {
		return m.flushBuffer(ctx, buf)
	}
	return nil
}

func (m *Manager) getOrCreateBuffer(table string) *SignalBuffer {
	m.mu.RLock()
	buf, ok := m.buffers[table]
	m.mu.RUnlock()
	if ok {
		return buf
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Double-check after acquiring write lock
	if buf, ok = m.buffers[table]; ok {
		return buf
	}
	buf = NewSignalBuffer(table)
	m.buffers[table] = buf
	return buf
}

func (m *Manager) flushBuffer(ctx context.Context, buf *SignalBuffer) error {
	records, rows := buf.Drain()
	if len(records) == 0 {
		return nil
	}

	parquetBytes, err := m.flushFn(ctx, buf.Table(), records, rows)
	if err != nil {
		// Re-append on failure so data isn't lost (don't release records).
		buf.Reappend(records)
		return err
	}

	// Release records only after a successful flush.
	for _, rec := range records {
		rec.Release()
	}

	if parquetBytes > 0 {
		buf.Calibrate(parquetBytes, rows)
	}
	return nil
}

func (m *Manager) flushAll(ctx context.Context) error {
	m.mu.RLock()
	buffers := make([]*SignalBuffer, 0, len(m.buffers))
	for _, buf := range m.buffers {
		buffers = append(buffers, buf)
	}
	m.mu.RUnlock()

	var lastErr error
	for _, buf := range buffers {
		if buf.IsEmpty() {
			continue
		}
		if err := m.flushBuffer(ctx, buf); err != nil {
			m.logger.Error("flush failed during drain",
				zap.String("table", buf.Table()),
				zap.Error(err))
			lastErr = err
		}
	}
	return lastErr
}

func (m *Manager) flushLoop(ctx context.Context) {
	defer close(m.done)
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.flushAll(ctx); err != nil {
				m.logger.Warn("time-triggered flush had errors", zap.Error(err))
			}
		}
	}
}
