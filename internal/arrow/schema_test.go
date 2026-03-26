// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
)

func TestSanitiseFieldName(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"service.name", "service_name"},
		{"http.status_code", "http_status_code"},
		{"simple", "simple"},
		{"a.b.c", "a_b_c"},
	}
	for _, tt := range tests {
		got := sanitiseFieldName(tt.input)
		if got != tt.want {
			t.Errorf("sanitiseFieldName(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestTracesSchemaFieldCount(t *testing.T) {
	promoted := DefaultTracesPromoted
	schema := TracesSchema(promoted)
	// 22 base + 8 promoted + 3 remainder = 33
	if schema.NumFields() != 33 {
		t.Errorf("TracesSchema has %d fields, want 33", schema.NumFields())
	}
	// Verify a promoted field exists
	if !hasField(schema, "attr_service_name") {
		t.Error("expected attr_service_name field in traces schema")
	}
	// Verify remainder fields exist
	for _, name := range []string{"resource_attributes", "scope_attributes", "attributes_remaining"} {
		if !hasField(schema, name) {
			t.Errorf("expected %s field in traces schema", name)
		}
	}
}

func TestLogsSchemaFieldCount(t *testing.T) {
	promoted := DefaultLogsPromoted
	schema := LogsSchema(promoted)
	// 14 base + 4 promoted + 3 remainder = 21
	if schema.NumFields() != 21 {
		t.Errorf("LogsSchema has %d fields, want 21", schema.NumFields())
	}
}

func TestGaugeSchemaFieldCount(t *testing.T) {
	promoted := DefaultMetricsPromoted
	schema := GaugeSchema(promoted)
	// 8 base + 6 gauge-specific + 2 promoted + 3 remainder = 19
	if schema.NumFields() != 19 {
		t.Errorf("GaugeSchema has %d fields, want 19", schema.NumFields())
	}
}

func TestSumSchemaFieldCount(t *testing.T) {
	promoted := DefaultMetricsPromoted
	schema := SumSchema(promoted)
	// 8 base + 8 sum-specific + 2 promoted + 3 remainder = 21
	if schema.NumFields() != 21 {
		t.Errorf("SumSchema has %d fields, want 21", schema.NumFields())
	}
}

func TestHistogramSchemaFieldCount(t *testing.T) {
	promoted := DefaultMetricsPromoted
	schema := HistogramSchema(promoted)
	// 8 base + 11 histogram-specific + 2 promoted + 3 remainder = 24
	if schema.NumFields() != 24 {
		t.Errorf("HistogramSchema has %d fields, want 24", schema.NumFields())
	}
}

func TestExpHistogramSchemaFieldCount(t *testing.T) {
	promoted := DefaultMetricsPromoted
	schema := ExpHistogramSchema(promoted)
	// 8 base + 16 exp-specific + 2 promoted + 3 remainder = 29
	if schema.NumFields() != 29 {
		t.Errorf("ExpHistogramSchema has %d fields, want 29", schema.NumFields())
	}
}

func TestSummarySchemaFieldCount(t *testing.T) {
	promoted := DefaultMetricsPromoted
	schema := SummarySchema(promoted)
	// 8 base + 6 summary-specific + 2 promoted + 3 remainder = 19
	if schema.NumFields() != 19 {
		t.Errorf("SummarySchema has %d fields, want 19", schema.NumFields())
	}
}

func TestCustomPromotedAttributes(t *testing.T) {
	custom := []string{"custom.attr"}
	schema := TracesSchema(custom)
	if !hasField(schema, "attr_custom_attr") {
		t.Error("expected custom promoted attribute in schema")
	}
	if hasField(schema, "attr_service_name") {
		t.Error("default promoted attribute should not be present with custom override")
	}
}

func hasField(schema *arrowlib.Schema, name string) bool {
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == name {
			return true
		}
	}
	return false
}

func TestMergePromoted(t *testing.T) {
	defaults := []string{"a", "b"}
	if got := MergePromoted(defaults, nil); len(got) != 2 || got[0] != "a" {
		t.Errorf("MergePromoted with nil overrides should return defaults, got %v", got)
	}
	overrides := []string{"c"}
	if got := MergePromoted(defaults, overrides); len(got) != 1 || got[0] != "c" {
		t.Errorf("MergePromoted with overrides should return overrides, got %v", got)
	}
}
