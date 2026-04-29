// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	activeFilename     = "active.ipc"
	pendingFilenameFmt = "pending-%06d.ipc"
)

var pendingFilenameRE = regexp.MustCompile(`^pending-(\d+)\.ipc$`)

// diskStore is an Arrow-IPC-stream-backed recordStore. Records are appended
// to a single active.ipc file. On Drain, the active file is rotated to a new
// pending-NNNNNN.ipc and all pending files are read back as the drained set;
// they are deleted only when the caller invokes commit (i.e. flush succeeded).
//
// Layout per buffer:
//
//	<dir>/active.ipc           — currently being written
//	<dir>/pending-NNNNNN.ipc   — drained-but-not-committed records
//
// Crash recovery on construction: any orphaned active.ipc is renamed to a
// fresh pending file, and existing pending files roll into the drained set so
// the next Drain picks them up.
//
// Concurrency is provided by the enclosing SignalBuffer's mu and flushMu;
// diskStore itself is not internally synchronised.
type diskStore struct {
	dir string

	activeFile   *os.File
	activeWriter *ipc.Writer
	activeRows   int64
	activeSchema *arrow.Schema

	drainingFiles []string // absolute paths, sorted ascending by sequence
	drainingRows  int64

	nextSeq uint64
	alloc   memory.Allocator
}

// newDiskStore creates a disk-backed record store rooted at dir. The directory
// is created if missing. Any pre-existing active/pending files (e.g. from a
// previous crashed run) are recovered into the drained set.
func newDiskStore(dir string, alloc memory.Allocator) (*diskStore, error) {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create spill dir %s: %w", dir, err)
	}
	s := &diskStore{
		dir:   dir,
		alloc: alloc,
	}
	if err := s.recover(); err != nil {
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
	var pending []string

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if name == activeFilename {
			activeExists = true
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
		pending = append(pending, filepath.Join(s.dir, name))
	}

	s.nextSeq = maxSeq + 1

	// Promote orphaned active.ipc to a pending file.
	if activeExists {
		target := filepath.Join(s.dir, fmt.Sprintf(pendingFilenameFmt, s.nextSeq))
		if err := os.Rename(activePath, target); err != nil {
			return fmt.Errorf("promote orphaned active.ipc: %w", err)
		}
		pending = append(pending, target)
		s.nextSeq++
	}

	sort.Strings(pending)
	s.drainingFiles = pending

	for _, p := range pending {
		rows, err := countRowsInIPC(p)
		if err != nil {
			return fmt.Errorf("count rows in %s: %w", p, err)
		}
		s.drainingRows += rows
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
		return fmt.Errorf("write to active stream: %w", err)
	}
	s.activeRows += rec.NumRows()
	return nil
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
	for _, path := range s.drainingFiles {
		recs, err := readIPCFile(path, s.alloc)
		if err != nil {
			for _, r := range allRecords {
				r.Release()
			}
			return nil, 0, nil, fmt.Errorf("read %s: %w", path, err)
		}
		allRecords = append(allRecords, recs...)
	}

	rows := s.drainingRows
	snapshot := make([]string, len(s.drainingFiles))
	copy(snapshot, s.drainingFiles)

	commit := func() {
		for _, p := range snapshot {
			_ = os.Remove(p) // best-effort; orphans recovered on next restart
		}
		s.drainingFiles = nil
		s.drainingRows = 0
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

	s.drainingFiles = append(s.drainingFiles, target)
	s.drainingRows += s.activeRows
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
	if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
		// Truncated tail — accept the prefix we successfully read.
	}
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
	if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
		// Truncated — return what we got.
	}
	return rows, nil
}
