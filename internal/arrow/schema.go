// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// TimestampType is the Arrow type used for OTel timestamp columns.
// Microsecond UTC timestamps map to Iceberg's timestamptz, which supports
// time-based partition transforms (e.g. HourTransform).
var TimestampType arrow.DataType = arrow.FixedWidthTypes.Timestamp_us

// NanoToMicro converts a nanosecond Unix timestamp to microseconds.
func NanoToMicro(nanos int64) int64 {
	return nanos / 1000
}

// Table names for each signal/metric type.
const (
	TableTraces       = "otel_traces"
	TableLogs         = "otel_logs"
	TableGauge        = "otel_metrics_gauge"
	TableSum          = "otel_metrics_sum"
	TableHistogram    = "otel_metrics_histogram"
	TableExpHistogram = "otel_metrics_exp_histogram"
	TableSummary      = "otel_metrics_summary"
)

// TracesSchema returns the Arrow schema for the traces table.
// Unsigned OTLP ints are mapped to signed for Iceberg compatibility.
func TracesSchema(promoted []string) *arrow.Schema {
	fields := []arrow.Field{
		// Resource
		{Name: "resource_schema_url", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "resource_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Scope
		{Name: "scope_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_version", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Span
		{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "trace_state", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "parent_span_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "kind", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		{Name: "end_time_unix_nano", Type: TimestampType, Nullable: false},
		{Name: "duration_nano", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "status_code", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "status_message", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "dropped_events_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "dropped_links_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Events and links as JSON arrays
		{Name: "events", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "links", Type: arrow.BinaryTypes.String, Nullable: true},
	}

	// Promoted attributes as nullable string columns
	for _, attr := range promoted {
		fields = append(fields, arrow.Field{
			Name: "attr_" + sanitizeFieldName(attr), Type: arrow.BinaryTypes.String, Nullable: true,
		})
	}

	// Remaining attributes as JSON
	fields = append(fields, arrow.Field{
		Name: "resource_attributes", Type: arrow.BinaryTypes.String, Nullable: true,
	})
	fields = append(fields, arrow.Field{
		Name: "scope_attributes", Type: arrow.BinaryTypes.String, Nullable: true,
	})
	fields = append(fields, arrow.Field{
		Name: "attributes_remaining", Type: arrow.BinaryTypes.String, Nullable: true,
	})

	return arrow.NewSchema(fields, nil)
}

// LogsSchema returns the Arrow schema for the logs table.
func LogsSchema(promoted []string) *arrow.Schema {
	fields := []arrow.Field{
		// Resource
		{Name: "resource_schema_url", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "resource_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Scope
		{Name: "scope_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_version", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Log record
		{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		{Name: "observed_time_unix_nano", Type: TimestampType, Nullable: false},
		{Name: "trace_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "span_id", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "severity_number", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "severity_text", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "body", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}

	for _, attr := range promoted {
		fields = append(fields, arrow.Field{
			Name: "attr_" + sanitizeFieldName(attr), Type: arrow.BinaryTypes.String, Nullable: true,
		})
	}

	fields = append(fields, arrow.Field{
		Name: "resource_attributes", Type: arrow.BinaryTypes.String, Nullable: true,
	})
	fields = append(fields, arrow.Field{
		Name: "scope_attributes", Type: arrow.BinaryTypes.String, Nullable: true,
	})
	fields = append(fields, arrow.Field{
		Name: "attributes_remaining", Type: arrow.BinaryTypes.String, Nullable: true,
	})

	return arrow.NewSchema(fields, nil)
}

// metricBaseFields returns fields common to all metric table schemas.
func metricBaseFields() []arrow.Field {
	return []arrow.Field{
		// Resource
		{Name: "resource_schema_url", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "resource_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Scope
		{Name: "scope_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_version", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "scope_dropped_attributes_count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},

		// Metric identity
		{Name: "metric_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "metric_description", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "metric_unit", Type: arrow.BinaryTypes.String, Nullable: true},
	}
}

// GaugeSchema returns the Arrow schema for gauge metrics.
func GaugeSchema(promoted []string) *arrow.Schema {
	fields := metricBaseFields()
	fields = append(fields,
		arrow.Field{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "value_double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "value_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "exemplars", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	fields = appendPromotedAndRemainder(fields, promoted)
	return arrow.NewSchema(fields, nil)
}

// SumSchema returns the Arrow schema for sum (counter/cumulative) metrics.
func SumSchema(promoted []string) *arrow.Schema {
	fields := metricBaseFields()
	fields = append(fields,
		arrow.Field{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "value_double", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "value_int", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		arrow.Field{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "is_monotonic", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		arrow.Field{Name: "aggregation_temporality", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "exemplars", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	fields = appendPromotedAndRemainder(fields, promoted)
	return arrow.NewSchema(fields, nil)
}

// HistogramSchema returns the Arrow schema for histogram metrics.
func HistogramSchema(promoted []string) *arrow.Schema {
	fields := metricBaseFields()
	fields = append(fields,
		arrow.Field{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "min", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "max", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "bucket_counts", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "explicit_bounds", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "aggregation_temporality", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "exemplars", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	fields = appendPromotedAndRemainder(fields, promoted)
	return arrow.NewSchema(fields, nil)
}

// ExpHistogramSchema returns the Arrow schema for exponential histogram metrics.
func ExpHistogramSchema(promoted []string) *arrow.Schema {
	fields := metricBaseFields()
	fields = append(fields,
		arrow.Field{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "min", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "max", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		arrow.Field{Name: "scale", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "zero_count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "zero_threshold", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "positive_offset", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "positive_bucket_counts", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "negative_offset", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "negative_bucket_counts", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "aggregation_temporality", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		arrow.Field{Name: "exemplars", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	fields = appendPromotedAndRemainder(fields, promoted)
	return arrow.NewSchema(fields, nil)
}

// SummarySchema returns the Arrow schema for summary metrics.
func SummarySchema(promoted []string) *arrow.Schema {
	fields := metricBaseFields()
	fields = append(fields,
		arrow.Field{Name: "time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "start_time_unix_nano", Type: TimestampType, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		arrow.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "quantile_values", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "flags", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	)
	fields = appendPromotedAndRemainder(fields, promoted)
	return arrow.NewSchema(fields, nil)
}

func appendPromotedAndRemainder(fields []arrow.Field, promoted []string) []arrow.Field {
	for _, attr := range promoted {
		fields = append(fields, arrow.Field{
			Name: "attr_" + sanitizeFieldName(attr), Type: arrow.BinaryTypes.String, Nullable: true,
		})
	}
	fields = append(fields,
		arrow.Field{Name: "resource_attributes", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "scope_attributes", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "attributes_remaining", Type: arrow.BinaryTypes.String, Nullable: true},
	)
	return fields
}

// sanitizeFieldName replaces dots with underscores for Arrow/Parquet column names.
func sanitizeFieldName(name string) string {
	result := make([]byte, len(name))
	for i := range name {
		if name[i] == '.' {
			result[i] = '_'
		} else {
			result[i] = name[i]
		}
	}
	return string(result)
}
