// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"errors"
	"testing"
	"time"

	iceberggo "github.com/apache/iceberg-go"

	iarrow "github.com/enterprisedb/icebergexporter/internal/arrow"
)

func TestIcebergTransform(t *testing.T) {
	tests := []struct {
		g    iarrow.Granularity
		want string
	}{
		{iarrow.GranularityHour, "hour"},
		{iarrow.GranularityDay, "day"},
		{iarrow.GranularityMonth, "month"},
		{"", "hour"}, // default
	}
	for _, tt := range tests {
		transform := icebergTransform(tt.g)
		if transform.String() != tt.want {
			t.Errorf("icebergTransform(%q).String() = %s, want %s", tt.g, transform.String(), tt.want)
		}
	}
}

func TestPartitionSpecForTable(t *testing.T) {
	// Build a minimal Iceberg schema with a timestamp field
	icebergSchema := iceberggo.NewSchema(0,
		iceberggo.NestedField{
			ID:       1,
			Name:     "time_unix_nano",
			Type:     iceberggo.PrimitiveTypes.TimestampTz,
			Required: true,
		},
		iceberggo.NestedField{
			ID:   2,
			Name: "value",
			Type: iceberggo.PrimitiveTypes.Float64,
		},
	)

	tests := []struct {
		name          string
		granularity   iarrow.Granularity
		wantTransform string
		wantName      string
	}{
		{"hour", iarrow.GranularityHour, "hour", "time_unix_nano_hour"},
		{"day", iarrow.GranularityDay, "day", "time_unix_nano_day"},
		{"month", iarrow.GranularityMonth, "month", "time_unix_nano_month"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := partitionSpecForTable(icebergSchema, tt.granularity)
			if spec == nil {
				t.Fatal("expected non-nil partition spec")
			}

			var found bool
			for f := range spec.Fields() {
				if f.Name == tt.wantName {
					found = true
					if f.Transform.String() != tt.wantTransform {
						t.Errorf("transform = %s, want %s", f.Transform.String(), tt.wantTransform)
					}
				}
			}
			if !found {
				t.Errorf("partition field %s not found in spec", tt.wantName)
			}
		})
	}
}

func TestPartitionSpecForTableNoTimestamp(t *testing.T) {
	schema := iceberggo.NewSchema(0,
		iceberggo.NestedField{
			ID:   1,
			Name: "value",
			Type: iceberggo.PrimitiveTypes.Float64,
		},
	)

	spec := partitionSpecForTable(schema, iarrow.GranularityHour)
	if spec != nil {
		t.Error("expected nil spec for schema without timestamp column")
	}
}

func TestPartitionSpecForTableStartTime(t *testing.T) {
	schema := iceberggo.NewSchema(0,
		iceberggo.NestedField{
			ID:       1,
			Name:     "start_time_unix_nano",
			Type:     iceberggo.PrimitiveTypes.TimestampTz,
			Required: true,
		},
	)

	spec := partitionSpecForTable(schema, iarrow.GranularityHour)
	if spec == nil {
		t.Fatal("expected non-nil spec for start_time_unix_nano")
	}

	for f := range spec.Fields() {
		if f.Name != "start_time_unix_nano_hour" {
			t.Errorf("unexpected field name: %s", f.Name)
		}
	}
}

func TestPartitionDataFromValuesHour(t *testing.T) {
	spec := iceberggo.NewPartitionSpec(
		iceberggo.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "ts_hour",
			Transform: iceberggo.HourTransform{},
		},
	)

	// 2025-03-15 14:00 UTC
	ts := time.Date(2025, 3, 15, 14, 0, 0, 0, time.UTC)
	expectedHours := int32(ts.UnixMicro() / (3600 * 1_000_000))

	vals := map[string]string{
		"year": "2025", "month": "03", "day": "15", "hour": "14",
	}
	result, err := partitionDataFromValues(spec, vals, iarrow.GranularityHour)
	if err != nil {
		t.Fatalf("partitionDataFromValues: %v", err)
	}
	if v, ok := result[1000]; !ok {
		t.Error("expected field 1000 in result")
	} else if v != expectedHours {
		t.Errorf("hour value = %v, want %v", v, expectedHours)
	}
}

func TestPartitionDataFromValuesDay(t *testing.T) {
	spec := iceberggo.NewPartitionSpec(
		iceberggo.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "ts_day",
			Transform: iceberggo.DayTransform{},
		},
	)

	// 2025-03-15 → days since epoch
	ts := time.Date(2025, 3, 15, 0, 0, 0, 0, time.UTC)
	expectedDays := int32(ts.Unix() / 86400)

	vals := map[string]string{
		"year": "2025", "month": "03", "day": "15",
	}
	result, err := partitionDataFromValues(spec, vals, iarrow.GranularityDay)
	if err != nil {
		t.Fatalf("partitionDataFromValues: %v", err)
	}
	if v := result[1000]; v != expectedDays {
		t.Errorf("day value = %v, want %v", v, expectedDays)
	}
}

func TestPartitionDataFromValuesMonth(t *testing.T) {
	spec := iceberggo.NewPartitionSpec(
		iceberggo.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "ts_month",
			Transform: iceberggo.MonthTransform{},
		},
	)

	// 2025-03 → (2025-1970)*12 + (3-1) = 55*12 + 2 = 662
	vals := map[string]string{
		"year": "2025", "month": "03",
	}
	result, err := partitionDataFromValues(spec, vals, iarrow.GranularityMonth)
	if err != nil {
		t.Fatalf("partitionDataFromValues: %v", err)
	}
	if v := result[1000]; v != int32(662) {
		t.Errorf("month value = %v, want 662", v)
	}
}

func TestPartitionDataFromValuesEmpty(t *testing.T) {
	spec := iceberggo.NewPartitionSpec()
	result, err := partitionDataFromValues(spec, nil, iarrow.GranularityHour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for empty values, got %v", result)
	}
}

func TestPartitionDataFromValuesInvalid(t *testing.T) {
	spec := iceberggo.NewPartitionSpec(
		iceberggo.PartitionField{
			SourceID:  1,
			FieldID:   1000,
			Name:      "ts_hour",
			Transform: iceberggo.HourTransform{},
		},
	)

	tests := []struct {
		name string
		vals map[string]string
	}{
		{"bad year", map[string]string{"year": "abc", "month": "03", "day": "15", "hour": "14"}},
		{"bad month", map[string]string{"year": "2025", "month": "abc", "day": "15", "hour": "14"}},
		{"bad day", map[string]string{"year": "2025", "month": "03", "day": "abc", "hour": "14"}},
		{"bad hour", map[string]string{"year": "2025", "month": "03", "day": "15", "hour": "abc"}},
		{"missing day", map[string]string{"year": "2025", "month": "03", "hour": "14"}},
		{"missing hour", map[string]string{"year": "2025", "month": "03", "day": "15"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := partitionDataFromValues(spec, tt.vals, iarrow.GranularityHour)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}

func TestIsAlreadyExists(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic", errors.New("something failed"), false},
		{"already exists lowercase", errors.New("table already exists"), true},
		{"AlreadyExists camelcase", errors.New("AlreadyExists: namespace otel"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAlreadyExists(tt.err); got != tt.want {
				t.Errorf("isAlreadyExists(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestRESTCatalogTableKey(t *testing.T) {
	c := &RESTCatalog{}
	key := c.tableKey("otel", "otel_traces")
	if key != "otel.otel_traces" {
		t.Errorf("tableKey = %s, want otel.otel_traces", key)
	}
}
