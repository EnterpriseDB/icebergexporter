// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"fmt"
	"time"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Granularity controls partition time resolution.
type Granularity string

const (
	GranularityHour  Granularity = "hour"
	GranularityDay   Granularity = "day"
	GranularityMonth Granularity = "month"
)

// PartitionKey represents a time-based partition boundary.
type PartitionKey struct {
	Year  int
	Month int
	Day   int
	Hour  int

	// granularity determines which fields are significant for grouping and
	// path generation. Zero value defaults to hour for backwards compatibility.
	granularity Granularity
}

// HivePath returns the Hive-style partition path for this key.
func (pk PartitionKey) HivePath() string {
	switch pk.granularity {
	case GranularityMonth:
		return fmt.Sprintf("year=%04d/month=%02d", pk.Year, pk.Month)
	case GranularityDay:
		return fmt.Sprintf("year=%04d/month=%02d/day=%02d", pk.Year, pk.Month, pk.Day)
	default: // hour
		return fmt.Sprintf("year=%04d/month=%02d/day=%02d/hour=%02d", pk.Year, pk.Month, pk.Day, pk.Hour)
	}
}

// PartitionValues returns the Hive-style key=value pairs for this partition.
func (pk PartitionKey) PartitionValues() map[string]string {
	switch pk.granularity {
	case GranularityMonth:
		return map[string]string{
			"year":  fmt.Sprintf("%04d", pk.Year),
			"month": fmt.Sprintf("%02d", pk.Month),
		}
	case GranularityDay:
		return map[string]string{
			"year":  fmt.Sprintf("%04d", pk.Year),
			"month": fmt.Sprintf("%02d", pk.Month),
			"day":   fmt.Sprintf("%02d", pk.Day),
		}
	default: // hour
		return map[string]string{
			"year":  fmt.Sprintf("%04d", pk.Year),
			"month": fmt.Sprintf("%02d", pk.Month),
			"day":   fmt.Sprintf("%02d", pk.Day),
			"hour":  fmt.Sprintf("%02d", pk.Hour),
		}
	}
}

// groupKey returns a comparable value for partition grouping. Fields below
// the configured granularity are zeroed so rows in the same partition bucket
// compare equal.
func (pk PartitionKey) groupKey() PartitionKey {
	switch pk.granularity {
	case GranularityMonth:
		return PartitionKey{Year: pk.Year, Month: pk.Month, granularity: pk.granularity}
	case GranularityDay:
		return PartitionKey{Year: pk.Year, Month: pk.Month, Day: pk.Day, granularity: pk.granularity}
	default:
		return pk
	}
}

// partitionKeyFromMicro derives a partition key from a Unix microsecond timestamp.
func partitionKeyFromMicro(micros int64, g Granularity) PartitionKey {
	t := time.UnixMicro(micros).UTC()
	return PartitionKey{
		Year:        t.Year(),
		Month:       int(t.Month()),
		Day:         t.Day(),
		Hour:        t.Hour(),
		granularity: g,
	}
}

// PartitionKeyFromMicro derives an hour-granularity partition key from a Unix
// microsecond timestamp.
func PartitionKeyFromMicro(micros int64) PartitionKey {
	return partitionKeyFromMicro(micros, GranularityHour)
}

// PartitionKeyFromNano derives an hour-granularity partition key from a Unix
// nanosecond timestamp.
func PartitionKeyFromNano(nanos int64) PartitionKey {
	t := time.Unix(0, nanos).UTC()
	return PartitionKey{
		Year:        t.Year(),
		Month:       int(t.Month()),
		Day:         t.Day(),
		Hour:        t.Hour(),
		granularity: GranularityHour,
	}
}

// PartitionedRecord holds a record and its partition key.
type PartitionedRecord struct {
	Key    PartitionKey
	Record arrowlib.RecordBatch
}

// SplitByPartition splits an Arrow record by time partitions at the given
// granularity. The timestampCol parameter specifies which column contains the
// microsecond Arrow Timestamp used for partitioning.
// Caller is responsible for releasing the returned records.
func SplitByPartition(alloc memory.Allocator, rec arrowlib.RecordBatch, timestampCol string, granularity Granularity) ([]PartitionedRecord, error) {
	// Find the timestamp column
	colIdx := -1
	for i := 0; i < int(rec.NumCols()); i++ {
		if rec.Schema().Field(i).Name == timestampCol {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, fmt.Errorf("timestamp column %q not found", timestampCol)
	}

	tsCol, ok := rec.Column(colIdx).(*array.Timestamp)
	if !ok {
		return nil, fmt.Errorf("timestamp column %q is not Timestamp", timestampCol)
	}

	if rec.NumRows() == 0 {
		return nil, nil
	}

	if granularity == "" {
		granularity = GranularityHour
	}

	// Group row indices by partition key
	type partGroup struct {
		key     PartitionKey
		indices []int
	}
	groups := make(map[PartitionKey]*partGroup)
	var order []PartitionKey // preserve insertion order

	for i := 0; i < tsCol.Len(); i++ {
		micros := int64(tsCol.Value(i))
		pk := partitionKeyFromMicro(micros, granularity).groupKey()
		g, ok := groups[pk]
		if !ok {
			g = &partGroup{key: pk}
			groups[pk] = g
			order = append(order, pk)
		}
		g.indices = append(g.indices, i)
	}

	// If everything is in one partition, just retain and return
	if len(groups) == 1 {
		rec.Retain()
		return []PartitionedRecord{{Key: order[0], Record: rec}}, nil
	}

	// Build per-partition records by selecting rows
	result := make([]PartitionedRecord, 0, len(groups))
	for _, pk := range order {
		g := groups[pk]
		partRec, err := selectRows(alloc, rec, g.indices)
		if err != nil {
			// Release already-created records on error
			for _, pr := range result {
				pr.Record.Release()
			}
			return nil, fmt.Errorf("selecting rows for partition %v: %w", pk.HivePath(), err)
		}
		result = append(result, PartitionedRecord{Key: pk, Record: partRec})
	}

	return result, nil
}

// selectRows creates a new record containing only the specified row indices.
func selectRows(alloc memory.Allocator, rec arrowlib.RecordBatch, indices []int) (arrowlib.RecordBatch, error) {
	schema := rec.Schema()
	builder := array.NewRecordBuilder(alloc, schema)
	defer builder.Release()

	for _, idx := range indices {
		for col := 0; col < int(rec.NumCols()); col++ {
			appendValue(builder.Field(col), rec.Column(col), idx)
		}
	}

	return builder.NewRecordBatch(), nil
}

// appendValue copies a single value from src at the given index to dst.
func appendValue(dst array.Builder, src arrowlib.Array, idx int) {
	if src.IsNull(idx) {
		dst.AppendNull()
		return
	}

	switch s := src.(type) {
	case *array.Int32:
		dst.(*array.Int32Builder).Append(s.Value(idx))
	case *array.Int64:
		dst.(*array.Int64Builder).Append(s.Value(idx))
	case *array.Float64:
		dst.(*array.Float64Builder).Append(s.Value(idx))
	case *array.String:
		dst.(*array.StringBuilder).Append(s.Value(idx))
	case *array.Boolean:
		dst.(*array.BooleanBuilder).Append(s.Value(idx))
	case *array.Timestamp:
		dst.(*array.TimestampBuilder).Append(s.Value(idx))
	case *array.FixedSizeBinary:
		dst.(*array.FixedSizeBinaryBuilder).Append(s.Value(idx))
	// Note: trace_id/span_id are now stored as hex strings (arrow.String),
	// handled by the array.String case above.
	default:
		// Fallback: serialize as string
		dst.(*array.StringBuilder).Append(s.ValueStr(idx))
	}
}
