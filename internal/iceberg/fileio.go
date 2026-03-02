package iceberg

import (
	"context"
	"io"
)

// FileIO abstracts object storage operations for writing Parquet data files.
type FileIO interface {
	// Write uploads data to the given path.
	Write(ctx context.Context, path string, data []byte) error

	// Read retrieves data from the given path.
	Read(ctx context.Context, path string) ([]byte, error)

	// List returns all object keys matching the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the object at the given path.
	Delete(ctx context.Context, path string) error

	// URI returns the full URI for an object path (e.g. s3://bucket/path).
	URI(path string) string
}

// ReadCloserFileIO extends FileIO with streaming read support.
type ReadCloserFileIO interface {
	FileIO
	// ReadStream opens a streaming reader for the given path.
	ReadStream(ctx context.Context, path string) (io.ReadCloser, error)
}
