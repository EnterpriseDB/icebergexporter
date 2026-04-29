// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/gofrs/flock"
	"go.uber.org/zap"
)

// sanitiseTableName makes a table name safe to use as a filesystem path
// component. Characters outside [A-Za-z0-9_-] are replaced with '_'. OTel
// table names (otel_traces, otel_metrics_gauge, etc.) are already safe;
// this is defensive against future naming conventions.
func sanitiseTableName(t string) string {
	if t == "" {
		return "_"
	}
	var b strings.Builder
	b.Grow(len(t))
	for _, r := range t {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}

const (
	activeFilename     = "active.ipc"
	pendingFilenameFmt = "pending-%06d.ipc"
	dirLockFilename    = ".lock"
)

var pendingFilenameRE = regexp.MustCompile(`^pending-(\d+)\.ipc$`)

// pendingFile is a sealed record file awaiting flush.
type pendingFile struct {
	path      string
	bytes     int64
	rotatedAt time.Time
}

// diskStore is an Arrow-IPC-stream-backed recordStore. Records are appended
// to a single active.ipc file. On Drain, the active file is rotated to a new
// pending-NNNNNN.ipc and all pending files are read back as the drained set;
// they are deleted only when the caller invokes commit (i.e. flush succeeded).
//
// Layout per buffer:
//
//	<dir>/active.ipc           — currently being written
//	<dir>/pending-NNNNNN.ipc   — drained-but-not-committed records
//	<dir>/.lock                — exclusive directory lock (flock)
//
// Crash recovery on construction: any orphaned active.ipc is renamed to a
// fresh pending file, and existing pending files roll into the drained set so
// the next Drain picks them up.
//
// Concurrency is provided by the enclosing SignalBuffer's mu and flushMu;
// diskStore itself is not internally synchronised. Cross-process exclusion
// is enforced by an exclusive flock on <dir>/.lock taken at construction
// and held until Close.
type diskStore struct {
	dir    string
	lock   *flock.Flock
	logger *zap.Logger

	activeFile   *os.File
	activeWriter *ipc.Writer
	activeRows   int64
	activeSchema *arrow.Schema

	drainingFiles []pendingFile // sorted ascending by sequence (== rotatedAt)
	drainingRows  int64
	drainingBytes int64

	nextSeq uint64
	alloc   memory.Allocator
}

// newDiskStore creates a disk-backed record store rooted at dir. The directory
// is created if missing and an exclusive flock is acquired on <dir>/.lock to
// prevent two processes from sharing the same spill directory. Any
// pre-existing active/pending files (e.g. from a previous crashed run) are
// recovered into the drained set.
//
// logger may be nil; if nil, no operational warnings are emitted.
func newDiskStore(dir string, alloc memory.Allocator, logger *zap.Logger) (*diskStore, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create spill dir %s: %w", dir, err)
	}

	lockPath := filepath.Join(dir, dirLockFilename)
	lk := flock.New(lockPath)
	locked, err := lk.TryLock()
	if err != nil {
		return nil, fmt.Errorf("acquire spill dir lock %s: %w", lockPath, err)
	}
	if !locked {
		return nil, fmt.Errorf("spill dir %s is locked by another process", dir)
	}

	s := &diskStore{
		dir:    dir,
		lock:   lk,
		alloc:  alloc,
		logger: logger,
	}
	if err := s.recover(); err != nil {
		_ = lk.Unlock()
		return nil, fmt.Errorf("recover spill dir %s: %w", dir, err)
	}
	return s, nil
}

func (s *diskStore) recover() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	var maxSeq uint64
	activePath := filepath.Join(s.dir, activeFilename)
	activeExists := false
	var pendingPaths []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == activeFilename {
			activeExists = true
			continue
		}
		if name == dirLockFilename {
			continue
		}
		m := pendingFilenameRE.FindStringSubmatch(name)
		if m == nil {
			continue
		}
		seq, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			continue
		}
		if seq > maxSeq {
			maxSeq = seq
		}
		pendingPaths = append(pendingPaths, filepath.Join(s.dir, name))
	}

	s.nextSeq = maxSeq + 1

	// Promote orphaned active.ipc to a pending file.
	if activeExists {
		target := filepath.Join(s.dir, fmt.Sprintf(pendingFilenameFmt, s.nextSeq))
		if err := os.Rename(activePath, target); err != nil {
			return fmt.Errorf("promote orphaned active.ipc: %w", err)
		}
		pendingPaths = append(pendingPaths, target)
		s.nextSeq++
	}

	sort.Strings(pendingPaths)

	for _, p := range pendingPaths {
		info, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("stat pending file %s: %w", p, err)
		}
		rows, err := countRowsInIPC(p)
		if err != nil {
			return fmt.Errorf("count rows in %s: %w", p, err)
		}
		s.drainingFiles = append(s.drainingFiles, pendingFile{
			path:      p,
			bytes:     info.Size(),
			rotatedAt: info.ModTime(),
		})
		s.drainingRows += rows
		s.drainingBytes += info.Size()
	}
	return nil
}

func (s *diskStore) Append(rec arrow.RecordBatch) error {
	if s.activeWriter == nil {
		if err := s.openActive(rec.Schema()); err != nil {
			return err
		}
	} else if !s.activeSchema.Equal(rec.Schema()) {
		return fmt.Errorf("buffer schema mismatch: active stream uses %s, record has %s",
			s.activeSchema, rec.Schema())
	}

	if err := s.activeWriter.Write(rec); err != nil {
		// The writer is now in an undefined state. Try to preserve any
		// records that were successfully written before this failure by
		// rotating the active file to a pending file (truncated tails are
		// tolerated by the reader). If rotation also fails, discard the
		// active file entirely so the next Append starts clean.
		s.handleWriteFailure()
		return fmt.Errorf("write to active stream: %w", err)
	}
	s.activeRows += rec.NumRows()
	return nil
}

// handleWriteFailure attempts to salvage the active stream after a Write
// error. State is reset to "no active stream" on either path so subsequent
// Append calls open a fresh file.
func (s *diskStore) handleWriteFailure() {
	if s.activeRows == 0 {
		// Nothing successfully written; just drop the file.
		s.closeActiveDiscarding()
		return
	}
	if err := s.rotateActive(); err != nil {
		s.logger.Warn("rotating poisoned active stream failed; discarding pending records",
			zap.String("dir", s.dir),
			zap.Error(err))
		s.closeActiveDiscarding()
	}
}

func (s *diskStore) openActive(schema *arrow.Schema) error {
	path := filepath.Join(s.dir, activeFilename)
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create active file: %w", err)
	}
	w := ipc.NewWriter(f, ipc.WithSchema(schema), ipc.WithAllocator(s.alloc))
	s.activeFile = f
	s.activeWriter = w
	s.activeSchema = schema
	s.activeRows = 0
	return nil
}

func (s *diskStore) Drain() ([]arrow.RecordBatch, int64, func(), error) {
	// Rotate active stream to a pending file if it has any records.
	if s.activeWriter != nil && s.activeRows > 0 {
		if err := s.rotateActive(); err != nil {
			return nil, 0, nil, err
		}
	} else if s.activeWriter != nil {
		// Active is open but empty — discard the empty file rather than rotating.
		s.closeActiveDiscarding()
	}

	if len(s.drainingFiles) == 0 {
		return nil, 0, func() {}, nil
	}

	var allRecords []arrow.RecordBatch
	for _, pf := range s.drainingFiles {
		recs, err := readIPCFile(pf.path, s.alloc)
		if err != nil {
			for _, r := range allRecords {
				r.Release()
			}
			return nil, 0, nil, fmt.Errorf("read %s: %w", pf.path, err)
		}
		allRecords = append(allRecords, recs...)
	}

	rows := s.drainingRows
	snapshot := make([]pendingFile, len(s.drainingFiles))
	copy(snapshot, s.drainingFiles)

	commit := func() {
		// Clear in-memory state immediately so the store reflects the committed
		// view; then delete files. Failures to remove a file are logged — the
		// orphan would be picked up by recovery on next startup and could cause
		// a duplicate flush, so this is operationally significant.
		s.drainingFiles = nil
		s.drainingRows = 0
		s.drainingBytes = 0

		for _, pf := range snapshot {
			if err := os.Remove(pf.path); err != nil && !os.IsNotExist(err) {
				s.logger.Error("failed to remove committed pending file; recovery may re-flush",
					zap.String("path", pf.path),
					zap.Error(err))
			}
		}
	}

	return allRecords, rows, commit, nil
}

func (s *diskStore) rotateActive() error {
	if err := s.activeWriter.Close(); err != nil {
		return fmt.Errorf("close active writer: %w", err)
	}
	if err := s.activeFile.Close(); err != nil {
		return fmt.Errorf("close active file: %w", err)
	}

	activePath := filepath.Join(s.dir, activeFilename)
	target := filepath.Join(s.dir, fmt.Sprintf(pendingFilenameFmt, s.nextSeq))
	if err := os.Rename(activePath, target); err != nil {
		return fmt.Errorf("rotate active to %s: %w", target, err)
	}
	info, err := os.Stat(target)
	if err != nil {
		return fmt.Errorf("stat rotated file %s: %w", target, err)
	}

	s.drainingFiles = append(s.drainingFiles, pendingFile{
		path:      target,
		bytes:     info.Size(),
		rotatedAt: time.Now(),
	})
	s.drainingRows += s.activeRows
	s.drainingBytes += info.Size()
	s.nextSeq++

	s.activeWriter = nil
	s.activeFile = nil
	s.activeSchema = nil
	s.activeRows = 0
	return nil
}

func (s *diskStore) closeActiveDiscarding() {
	_ = s.activeWriter.Close()
	_ = s.activeFile.Close()
	_ = os.Remove(filepath.Join(s.dir, activeFilename))
	s.activeWriter = nil
	s.activeFile = nil
	s.activeSchema = nil
	s.activeRows = 0
}

func (s *diskStore) Rows() int64 {
	return s.activeRows + s.drainingRows
}

func (s *diskStore) IsEmpty() bool {
	return s.activeRows == 0 && len(s.drainingFiles) == 0
}

func (s *diskStore) Close() {
	if s.activeWriter != nil {
		_ = s.activeWriter.Close()
		_ = s.activeFile.Close()
		s.activeWriter = nil
		s.activeFile = nil
		// active.ipc remains on disk for recovery on next startup.
	}
	// drainingFiles remain on disk; recovery handles them next time.
	if s.lock != nil {
		_ = s.lock.Unlock()
		s.lock = nil
	}
}

func (s *diskStore) Metrics() StoreMetrics {
	var oldestAge float64
	if len(s.drainingFiles) > 0 {
		oldestAge = time.Since(s.drainingFiles[0].rotatedAt).Seconds()
	}
	return StoreMetrics{
		PendingFiles:            len(s.drainingFiles),
		PendingBytes:            s.drainingBytes,
		OldestPendingAgeSeconds: oldestAge,
	}
}

// readIPCFile reads all record batches from an Arrow IPC stream file.
// Truncated files (e.g. from a crash mid-write) are tolerated: the reader
// returns whatever complete messages it can parse and the tail is dropped.
func readIPCFile(path string, alloc memory.Allocator) ([]arrow.RecordBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, fmt.Errorf("open ipc reader: %w", err)
	}
	defer reader.Release()

	var records []arrow.RecordBatch
	for reader.Next() {
		rec := reader.RecordBatch()
		rec.Retain() // outlive reader.Release
		records = append(records, rec)
	}
	// Truncated tail (e.g. crash mid-write) is tolerated; accept the prefix.
	_ = reader.Err()
	return records, nil
}

func countRowsInIPC(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader, err := ipc.NewReader(f)
	if err != nil {
		return 0, fmt.Errorf("open ipc reader: %w", err)
	}
	defer reader.Release()

	var rows int64
	for reader.Next() {
		rows += reader.RecordBatch().NumRows()
	}
	// Truncated tail is tolerated; return what we counted.
	_ = reader.Err()
	return rows, nil
}
