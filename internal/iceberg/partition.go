package iceberg

import (
	"fmt"

	"github.com/google/uuid"
)

// DataFilePath generates a Hive-style partition path for a data file.
// Format: {table}/data/{hivePath}/{uuid}.parquet
func DataFilePath(table, hivePath string) string {
	return fmt.Sprintf("%s/data/%s/%s.parquet", table, hivePath, uuid.New().String())
}

// MetadataPath generates a path for table metadata.
func MetadataPath(table string) string {
	return fmt.Sprintf("%s/metadata", table)
}
