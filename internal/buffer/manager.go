// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"

	"github.com/enterprisedb/icebergexporter/internal/metadata"
)

// FlushFunc is called when a buffer needs flushing. It receives the table name
// and the accumulated records. Returns the number of Parquet bytes written
// (for calibration) or an error.
type FlushFunc func(ctx context.Context, table string, records []arrow.RecordBatch, totalRows int64) (parquetBytes int64, err error)

// StorageType selects the backing store for per-table buffers.
type StorageType string

const (
	StorageMemory StorageType = "memory"
	StorageDisk   StorageType = "disk"
)

// StorageOptions configures the per-table store implementation.
type StorageOptions struct {
	// Type is the backend: StorageMemory (default) or StorageDisk.
	Type StorageType

	// Path is the root directory for disk-backed buffers. Each table gets a
	// subdirectory underneath. Required when Type is StorageDisk.
	Path string
}

// ManagerOptions configures a Manager.
type ManagerOptions struct {
	// MaxSizeBytes triggers a synchronous flush when a single buffer's
	// estimated size hits this threshold. Zero disables size-triggered flush.
	MaxSizeBytes int64

	// FlushInterval is the period between time-triggered flushes.
	FlushInterval time.Duration

	// Storage selects the per-table store backend.
	Storage StorageOptions

	// Allocator is the Arrow memory allocator used by disk-backed stores
	// when reading records back from IPC streams. Optional; defaults to
	// memory.DefaultAllocator.
	Allocator memory.Allocator

	// Telemetry is the optional generated telemetry builder used to emit
	// component metrics. When nil, the Manager runs without metrics.
	Telemetry *metadata.TelemetryBuilder
}

// Manager implements a size+time hybrid buffer manager with per-table buffers.
type Manager struct {
	mu        sync.RWMutex
	buffers   map[string]*SignalBuffer
	opts      ManagerOptions
	flushFn   FlushFunc
	logger    *zap.Logger
	allocator memory.Allocator

	cancel context.CancelFunc
	done   chan struct{}
}

// NewManager creates a buffer manager. Returns an error if the options are
// invalid (e.g. disk storage selected without a Path).
func NewManager(opts ManagerOptions, flushFn FlushFunc, logger *zap.Logger) (*Manager, error) {
	if err := validateOptions(opts); err != nil {
		return nil, err
	}
	alloc := opts.Allocator
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return &Manager{
		buffers:   make(map[string]*SignalBuffer),
		opts:      opts,
		flushFn:   flushFn,
		logger:    logger,
		allocator: alloc,
	}, nil
}

func validateOptions(opts ManagerOptions) error {
	if opts.MaxSizeBytes < 0 {
		return fmt.Errorf("max_size_bytes must be non-negative, got %d", opts.MaxSizeBytes)
	}
	if opts.FlushInterval < 0 {
		return fmt.Errorf("flush_interval must be non-negative, got %s", opts.FlushInterval)
	}
	switch opts.Storage.Type {
	case "", StorageMemory:
		// valid
	case StorageDisk:
		if opts.Storage.Path == "" {
			return fmt.Errorf("storage.path is required for storage.type=%q", StorageDisk)
		}
	default:
		return fmt.Errorf("unknown storage type %q", opts.Storage.Type)
	}
	return nil
}

// Start begins the background time-based flush goroutine and registers
// telemetry callbacks.
func (m *Manager) Start() error {
	if err := m.registerTelemetryCallbacks(); err != nil {
		return fmt.Errorf("register telemetry callbacks: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.done = make(chan struct{})
	go m.flushLoop(ctx)
	return nil
}

// Stop cancels the background flush goroutine, then drains all buffers.
// Telemetry callbacks remain registered until the TelemetryBuilder is
// shut down by its owner (typically the exporter).
func (m *Manager) Stop(ctx context.Context) error {
	if m.cancel != nil {
		m.cancel()
		<-m.done
	}
	return m.flushAll(ctx)
}

// Add adds a record to the named table's buffer. If the buffer exceeds the
// size threshold, a synchronous flush is triggered and any error is returned
// to the caller (enabling OTel retry). Errors from the underlying store
// (e.g. disk write failures for disk-backed buffers) are also propagated.
func (m *Manager) Add(ctx context.Context, table string, rec arrow.RecordBatch) error {
	buf, err := m.getOrCreateBuffer(table)
	if err != nil {
		return err
	}
	if err := buf.Add(rec); err != nil {
		return err
	}

	if m.opts.MaxSizeBytes > 0 && buf.EstimatedSize() >= m.opts.MaxSizeBytes {
		return m.flushBuffer(ctx, buf)
	}
	return nil
}

func (m *Manager) getOrCreateBuffer(table string) (*SignalBuffer, error) {
	m.mu.RLock()
	buf, ok := m.buffers[table]
	m.mu.RUnlock()
	if ok {
		return buf, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if buf, ok = m.buffers[table]; ok {
		return buf, nil
	}

	store, err := m.newStoreFor(table)
	if err != nil {
		return nil, fmt.Errorf("create store for table %q: %w", table, err)
	}
	buf = newSignalBufferWithStore(table, store)
	m.buffers[table] = buf
	return buf, nil
}

func (m *Manager) newStoreFor(table string) (recordStore, error) {
	switch m.opts.Storage.Type {
	case StorageDisk:
		dir := filepath.Join(m.opts.Storage.Path, sanitiseTableName(table))
		return newDiskStore(dir, m.allocator)
	default: // StorageMemory or empty
		return newMemStore(), nil
	}
}

func (m *Manager) flushBuffer(ctx context.Context, buf *SignalBuffer) error {
	start := time.Now()
	rows, err := buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		return m.flushFn(ctx, buf.Table(), records, rows)
	})

	// Record telemetry only when there was actual work (rows>0) or a failure.
	// Empty drains are no-ops we don't want polluting the histograms.
	if m.opts.Telemetry != nil && (rows > 0 || err != nil) {
		outcome := "success"
		if err != nil {
			outcome = "failure"
		}
		attrs := metric.WithAttributeSet(attribute.NewSet(
			attribute.String("outcome", outcome),
			attribute.String("table", buf.Table()),
		))
		m.opts.Telemetry.ExporterIcebergBufferFlushes.Add(ctx, 1, attrs)
		m.opts.Telemetry.ExporterIcebergBufferFlushDurationSeconds.Record(ctx, time.Since(start).Seconds(), attrs)
	}

	return err
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
	ticker := time.NewTicker(m.opts.FlushInterval)
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

// registerTelemetryCallbacks wires the four async gauge metrics. The
// callbacks iterate buffers under m.mu and acquire each SignalBuffer's mu
// briefly to read its current snapshot. Order matters: under no path is a
// SignalBuffer.mu held while m.mu is taken, so no deadlock.
func (m *Manager) registerTelemetryCallbacks() error {
	tb := m.opts.Telemetry
	if tb == nil {
		return nil
	}
	if err := tb.RegisterExporterIcebergBufferRowsCallback(func(_ context.Context, o metric.Int64Observer) error {
		for _, snap := range m.snapshotBufferMetrics() {
			o.Observe(snap.metrics.Rows, metric.WithAttributes(attribute.String("table", snap.table)))
		}
		return nil
	}); err != nil {
		return err
	}
	if err := tb.RegisterExporterIcebergBufferPendingFilesCallback(func(_ context.Context, o metric.Int64Observer) error {
		for _, snap := range m.snapshotBufferMetrics() {
			o.Observe(int64(snap.metrics.PendingFiles), metric.WithAttributes(attribute.String("table", snap.table)))
		}
		return nil
	}); err != nil {
		return err
	}
	if err := tb.RegisterExporterIcebergBufferPendingBytesCallback(func(_ context.Context, o metric.Int64Observer) error {
		for _, snap := range m.snapshotBufferMetrics() {
			o.Observe(snap.metrics.PendingBytes, metric.WithAttributes(attribute.String("table", snap.table)))
		}
		return nil
	}); err != nil {
		return err
	}
	if err := tb.RegisterExporterIcebergBufferPendingOldestAgeSecondsCallback(func(_ context.Context, o metric.Float64Observer) error {
		for _, snap := range m.snapshotBufferMetrics() {
			o.Observe(snap.metrics.OldestPendingAgeSeconds, metric.WithAttributes(attribute.String("table", snap.table)))
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// bufferMetricSnapshot pairs a table name with its current metrics snapshot.
type bufferMetricSnapshot struct {
	table   string
	metrics BufferMetrics
}

// snapshotBufferMetrics returns a per-buffer snapshot of telemetry counters.
// m.mu is released before per-buffer locks are acquired so the only locking
// order is m.mu → buf.mu, never the reverse.
func (m *Manager) snapshotBufferMetrics() []bufferMetricSnapshot {
	m.mu.RLock()
	pairs := make([]struct {
		table string
		buf   *SignalBuffer
	}, 0, len(m.buffers))
	for table, buf := range m.buffers {
		pairs = append(pairs, struct {
			table string
			buf   *SignalBuffer
		}{table, buf})
	}
	m.mu.RUnlock()

	snaps := make([]bufferMetricSnapshot, 0, len(pairs))
	for _, p := range pairs {
		snaps = append(snaps, bufferMetricSnapshot{table: p.table, metrics: p.buf.Metrics()})
	}
	return snaps
}
