// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"errors"
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
	// MaxSizeBytes is the hard cap on a single buffer's estimated size. Add
	// triggers a synchronous flush before crossing this threshold; if the
	// flush fails the Add is rejected so the buffer cannot exceed the cap.
	// Zero disables the cap (buffer grows freely; only the time-triggered
	// flush bounds it).
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
	creating  sync.Map // map[string]*sync.Mutex — per-table store-construction lock
	opts      ManagerOptions
	flushFn   FlushFunc
	logger    *zap.Logger
	allocator memory.Allocator

	startOnce sync.Once
	startErr  error
	stopOnce  sync.Once
	stopErr   error
	cancel    context.CancelFunc
	done      chan struct{}
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
		return fmt.Errorf("max_size must be non-negative, got %d", opts.MaxSizeBytes)
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

// Start registers telemetry callbacks and begins the background time-based
// flush goroutine. Idempotent — subsequent calls return the first call's
// error (or nil) without re-registering or re-spawning.
func (m *Manager) Start() error {
	m.startOnce.Do(func() {
		if err := m.registerTelemetryCallbacks(); err != nil {
			// Best-effort cleanup of partial registrations: Telemetry's
			// Shutdown unregisters whatever did register. Caller is expected
			// to invoke that on the TelemetryBuilder when the exporter
			// itself shuts down, so we don't double-shutdown here.
			m.startErr = fmt.Errorf("register telemetry callbacks: %w", err)
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		m.cancel = cancel
		m.done = make(chan struct{})
		go func() {
			// defer cancel() so the context is always released when the
			// flush loop exits, even if Stop was never called.
			defer cancel()
			m.flushLoop(ctx)
		}()
	})
	return m.startErr
}

// Stop cancels the background flush goroutine, then drains all buffers.
// Idempotent — subsequent calls return the first call's error without
// re-cancelling or re-draining. Telemetry callbacks remain registered until
// the TelemetryBuilder is shut down by its owner (typically the exporter).
func (m *Manager) Stop(ctx context.Context) error {
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
			<-m.done
		}
		m.stopErr = m.flushAll(ctx)
	})
	return m.stopErr
}

// Add adds a record to the named table's buffer. If the post-Add size would
// exceed MaxSizeBytes, a synchronous flush is triggered first; if that flush
// fails, the record is rejected (not buffered) and the error is returned so
// OTel can retry the whole batch without duplicating already-buffered data.
//
// Errors from the underlying store (e.g. disk write failures for disk-backed
// buffers) are also propagated.
func (m *Manager) Add(ctx context.Context, table string, rec arrow.RecordBatch) error {
	buf, err := m.getOrCreateBuffer(table)
	if err != nil {
		return err
	}

	if m.opts.MaxSizeBytes > 0 && buf.EstimatedSize() >= m.opts.MaxSizeBytes {
		// At cap — flush synchronously to make room. If flush fails the
		// caller's record is NOT buffered, so OTel retry won't duplicate.
		if err := m.flushBuffer(ctx, buf); err != nil {
			return fmt.Errorf("buffer at capacity and flush failed: %w", err)
		}
		// Post-flush, the buffer should be near-empty (commit ran). If a
		// failure left records in draining and they are still over-cap, the
		// flush would have returned an error above; reaching here means
		// space was reclaimed.
	}

	return buf.Add(rec)
}

func (m *Manager) getOrCreateBuffer(table string) (*SignalBuffer, error) {
	// Fast path: already exists.
	m.mu.RLock()
	buf, ok := m.buffers[table]
	m.mu.RUnlock()
	if ok {
		return buf, nil
	}

	// Slow path: serialise construction per-table. The disk-backed store
	// takes a flock on its directory at newDiskStore time, so two concurrent
	// first-Add goroutines for the same table must not both attempt
	// construction — the loser would see "locked by another process" and
	// fail its Add. Per-table mutex keeps construction parallel across
	// different tables while serialising same-table contenders.
	cmuI, _ := m.creating.LoadOrStore(table, &sync.Mutex{})
	cmu := cmuI.(*sync.Mutex)
	cmu.Lock()
	defer cmu.Unlock()

	// Re-check under the per-table mutex: a peer goroutine may have
	// constructed the buffer while we were waiting.
	m.mu.RLock()
	buf, ok = m.buffers[table]
	m.mu.RUnlock()
	if ok {
		return buf, nil
	}

	// Construct the store. Disk recovery can do significant I/O — we hold
	// only the per-table mutex here, not m.mu, so unrelated tables can
	// still claim/look-up entries in parallel.
	store, err := m.newStoreFor(table)
	if err != nil {
		return nil, fmt.Errorf("create store for table %q: %w", table, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Belt-and-braces: under the per-table mutex no peer can have inserted,
	// but check anyway in case construction-races assumptions ever change.
	if existing, ok := m.buffers[table]; ok {
		store.Close()
		return existing, nil
	}
	buf = newSignalBufferWithStore(table, store)
	m.buffers[table] = buf
	return buf, nil
}

func (m *Manager) newStoreFor(table string) (recordStore, error) {
	switch m.opts.Storage.Type {
	case StorageDisk:
		dir := filepath.Join(m.opts.Storage.Path, sanitiseTableName(table))
		return newDiskStore(dir, m.allocator, m.logger.With(zap.String("table", table)))
	default: // StorageMemory or empty
		return newMemStore(), nil
	}
}

func (m *Manager) flushBuffer(ctx context.Context, buf *SignalBuffer) error {
	start := time.Now()
	rows, err := buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		return m.flushFn(ctx, buf.Table(), records, rows)
	})
	m.recordFlushTelemetry(ctx, buf.Table(), rows, err, time.Since(start))
	return err
}

// recordFlushTelemetry emits the per-flush counter and duration histogram.
// No-op when Telemetry is nil (the documented "metrics disabled" path) or
// when there was nothing to do (empty drain with no error) — empty drains
// are not "flush attempts" we want polluting the histograms.
func (m *Manager) recordFlushTelemetry(ctx context.Context, table string, rows int64, flushErr error, dur time.Duration) {
	if m.opts.Telemetry == nil {
		return
	}
	if rows == 0 && flushErr == nil {
		return
	}
	outcome := "success"
	if flushErr != nil {
		outcome = "failure"
	}
	attrs := metric.WithAttributeSet(attribute.NewSet(
		attribute.String("outcome", outcome),
		attribute.String("table", table),
	))
	m.opts.Telemetry.ExporterIcebergBufferFlushes.Add(ctx, 1, attrs)
	m.opts.Telemetry.ExporterIcebergBufferFlushDurationSeconds.Record(ctx, dur.Seconds(), attrs)
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

// registerTelemetryCallbacks wires the four async gauge metrics. All four
// registrations are attempted; partial successes are returned as a joined
// error so the caller can decide whether to abort. The TelemetryBuilder's
// own Shutdown unregisters whatever did register.
//
// The callbacks iterate buffers via snapshotBufferMetrics, which captures
// the buffer slice under m.mu and then acquires each SignalBuffer.mu only
// after releasing m.mu — keeping the lock order m.mu → buf.mu unidirectional.
func (m *Manager) registerTelemetryCallbacks() error {
	tb := m.opts.Telemetry
	if tb == nil {
		return nil
	}

	int64Reg := func(register func(metric.Int64Callback) error, get func(Metrics) int64) error {
		return register(func(_ context.Context, o metric.Int64Observer) error {
			for _, snap := range m.snapshotBufferMetrics() {
				o.Observe(get(snap.metrics), metric.WithAttributes(attribute.String("table", snap.table)))
			}
			return nil
		})
	}
	float64Reg := func(register func(metric.Float64Callback) error, get func(Metrics) float64) error {
		return register(func(_ context.Context, o metric.Float64Observer) error {
			for _, snap := range m.snapshotBufferMetrics() {
				o.Observe(get(snap.metrics), metric.WithAttributes(attribute.String("table", snap.table)))
			}
			return nil
		})
	}

	var errs []error
	errs = append(errs,
		int64Reg(tb.RegisterExporterIcebergBufferRowsCallback, func(m Metrics) int64 { return m.Rows }),
		int64Reg(tb.RegisterExporterIcebergBufferPendingFilesCallback, func(m Metrics) int64 { return int64(m.PendingFiles) }),
		int64Reg(tb.RegisterExporterIcebergBufferPendingBytesCallback, func(m Metrics) int64 { return m.PendingBytes }),
		float64Reg(tb.RegisterExporterIcebergBufferPendingOldestAgeSecondsCallback, func(m Metrics) float64 { return m.OldestPendingAgeSeconds }),
	)
	return errors.Join(errs...)
}

// bufferMetricSnapshot pairs a table name with its current metrics snapshot.
type bufferMetricSnapshot struct {
	table   string
	metrics Metrics
}

// snapshotBufferMetrics returns a per-buffer snapshot of telemetry counters.
// m.mu is released before per-buffer locks are acquired so the only locking
// order is m.mu → buf.mu, never the reverse.
func (m *Manager) snapshotBufferMetrics() []bufferMetricSnapshot {
	m.mu.RLock()
	bufs := make([]*SignalBuffer, 0, len(m.buffers))
	for _, buf := range m.buffers {
		bufs = append(bufs, buf)
	}
	m.mu.RUnlock()

	snaps := make([]bufferMetricSnapshot, 0, len(bufs))
	for _, buf := range bufs {
		snaps = append(snaps, bufferMetricSnapshot{table: buf.Table(), metrics: buf.Metrics()})
	}
	return snaps
}
