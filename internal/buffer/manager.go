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
	"go.uber.org/zap"
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
}

// Manager implements a size+time hybrid buffer manager with per-table buffers.
type Manager struct {
	mu       sync.RWMutex
	buffers  map[string]*SignalBuffer
	opts     ManagerOptions
	flushFn  FlushFunc
	logger   *zap.Logger
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
	return buf.FlushVia(func(records []arrow.RecordBatch, rows int64) (int64, error) {
		return m.flushFn(ctx, buf.Table(), records, rows)
	})
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
