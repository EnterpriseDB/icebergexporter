package arrow

import (
	"testing"
	"time"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestPartitionKeyFromNano(t *testing.T) {
	ts := time.Date(2025, 3, 15, 14, 30, 0, 0, time.UTC)
	pk := PartitionKeyFromNano(ts.UnixNano())

	if pk.Year != 2025 || pk.Month != 3 || pk.Day != 15 || pk.Hour != 14 {
		t.Errorf("unexpected partition key: %+v", pk)
	}
	if pk.HivePath() != "year=2025/month=03/day=15/hour=14" {
		t.Errorf("unexpected hive path: %s", pk.HivePath())
	}
}

func TestPartitionKeyFromMicro(t *testing.T) {
	ts := time.Date(2025, 3, 15, 14, 30, 0, 0, time.UTC)
	pk := PartitionKeyFromMicro(ts.UnixMicro())

	if pk.Year != 2025 || pk.Month != 3 || pk.Day != 15 || pk.Hour != 14 {
		t.Errorf("unexpected partition key: %+v", pk)
	}
}

func TestPartitionKeyGranularity(t *testing.T) {
	ts := time.Date(2025, 3, 15, 14, 30, 0, 0, time.UTC)
	micros := ts.UnixMicro()

	tests := []struct {
		name        string
		granularity Granularity
		wantPath    string
		wantValues  map[string]string
	}{
		{
			name:        "hour",
			granularity: GranularityHour,
			wantPath:    "year=2025/month=03/day=15/hour=14",
			wantValues:  map[string]string{"year": "2025", "month": "03", "day": "15", "hour": "14"},
		},
		{
			name:        "day",
			granularity: GranularityDay,
			wantPath:    "year=2025/month=03/day=15",
			wantValues:  map[string]string{"year": "2025", "month": "03", "day": "15"},
		},
		{
			name:        "month",
			granularity: GranularityMonth,
			wantPath:    "year=2025/month=03",
			wantValues:  map[string]string{"year": "2025", "month": "03"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pk := partitionKeyFromMicro(micros, tt.granularity).groupKey()
			if pk.HivePath() != tt.wantPath {
				t.Errorf("HivePath() = %s, want %s", pk.HivePath(), tt.wantPath)
			}
			vals := pk.PartitionValues()
			if len(vals) != len(tt.wantValues) {
				t.Errorf("PartitionValues() has %d entries, want %d", len(vals), len(tt.wantValues))
			}
			for k, want := range tt.wantValues {
				if got := vals[k]; got != want {
					t.Errorf("PartitionValues()[%s] = %s, want %s", k, got, want)
				}
			}
		})
	}
}

func TestSplitByPartitionSinglePartition(t *testing.T) {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "ts", Type: TimestampType},
		{Name: "val", Type: arrowlib.BinaryTypes.String},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	// All in same hour
	base := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC).UnixMicro()
	for i := 0; i < 5; i++ {
		b.Field(0).(*array.TimestampBuilder).Append(arrowlib.Timestamp(base + int64(i*1e6)))
		b.Field(1).(*array.StringBuilder).Append("val")
	}
	rec := b.NewRecord()
	defer rec.Release()

	parts, err := SplitByPartition(memory.DefaultAllocator, rec, "ts", GranularityHour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, p := range parts {
			p.Record.Release()
		}
	}()

	if len(parts) != 1 {
		t.Fatalf("expected 1 partition, got %d", len(parts))
	}
	if parts[0].Record.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", parts[0].Record.NumRows())
	}
}

func TestSplitByPartitionMultiplePartitions(t *testing.T) {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "ts", Type: TimestampType},
		{Name: "val", Type: arrowlib.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	// Three different hours
	hours := []time.Time{
		time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 14, 30, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 15, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 16, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 15, 15, 0, 0, time.UTC),
	}
	for i, ts := range hours {
		b.Field(0).(*array.TimestampBuilder).Append(arrowlib.Timestamp(ts.UnixMicro()))
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	parts, err := SplitByPartition(memory.DefaultAllocator, rec, "ts", GranularityHour)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, p := range parts {
			p.Record.Release()
		}
	}()

	if len(parts) != 3 {
		t.Fatalf("expected 3 partitions, got %d", len(parts))
	}

	// First partition (hour 14): 2 rows
	if parts[0].Record.NumRows() != 2 {
		t.Errorf("partition 0: expected 2 rows, got %d", parts[0].Record.NumRows())
	}
	// Second partition (hour 15): 2 rows
	if parts[1].Record.NumRows() != 2 {
		t.Errorf("partition 1: expected 2 rows, got %d", parts[1].Record.NumRows())
	}
	// Third partition (hour 16): 1 row
	if parts[2].Record.NumRows() != 1 {
		t.Errorf("partition 2: expected 1 row, got %d", parts[2].Record.NumRows())
	}
}

func TestSplitByPartitionDayGranularity(t *testing.T) {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "ts", Type: TimestampType},
		{Name: "val", Type: arrowlib.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	// Two different days, three different hours — day granularity should merge
	times := []time.Time{
		time.Date(2025, 3, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 16, 8, 0, 0, 0, time.UTC),
	}
	for i, ts := range times {
		b.Field(0).(*array.TimestampBuilder).Append(arrowlib.Timestamp(ts.UnixMicro()))
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	parts, err := SplitByPartition(memory.DefaultAllocator, rec, "ts", GranularityDay)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, p := range parts {
			p.Record.Release()
		}
	}()

	if len(parts) != 2 {
		t.Fatalf("expected 2 day partitions, got %d", len(parts))
	}
	if parts[0].Record.NumRows() != 2 {
		t.Errorf("day 15: expected 2 rows, got %d", parts[0].Record.NumRows())
	}
	if parts[1].Record.NumRows() != 1 {
		t.Errorf("day 16: expected 1 row, got %d", parts[1].Record.NumRows())
	}
	if parts[0].Key.HivePath() != "year=2025/month=03/day=15" {
		t.Errorf("unexpected hive path: %s", parts[0].Key.HivePath())
	}
}

func TestSplitByPartitionMonthGranularity(t *testing.T) {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "ts", Type: TimestampType},
		{Name: "val", Type: arrowlib.PrimitiveTypes.Int64},
	}, nil)

	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()

	times := []time.Time{
		time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC),
		time.Date(2025, 4, 1, 0, 0, 0, 0, time.UTC),
	}
	for i, ts := range times {
		b.Field(0).(*array.TimestampBuilder).Append(arrowlib.Timestamp(ts.UnixMicro()))
		b.Field(1).(*array.Int64Builder).Append(int64(i))
	}
	rec := b.NewRecord()
	defer rec.Release()

	parts, err := SplitByPartition(memory.DefaultAllocator, rec, "ts", GranularityMonth)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, p := range parts {
			p.Record.Release()
		}
	}()

	if len(parts) != 2 {
		t.Fatalf("expected 2 month partitions, got %d", len(parts))
	}
	if parts[0].Record.NumRows() != 2 {
		t.Errorf("march: expected 2 rows, got %d", parts[0].Record.NumRows())
	}
	if parts[0].Key.HivePath() != "year=2025/month=03" {
		t.Errorf("unexpected hive path: %s", parts[0].Key.HivePath())
	}
}

func TestSplitByPartitionMissingColumn(t *testing.T) {
	schema := arrowlib.NewSchema([]arrowlib.Field{
		{Name: "val", Type: arrowlib.PrimitiveTypes.Int64},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).Append(1)
	rec := b.NewRecord()
	defer rec.Release()

	_, err := SplitByPartition(memory.DefaultAllocator, rec, "ts", GranularityHour)
	if err == nil {
		t.Error("expected error for missing timestamp column")
	}
}
