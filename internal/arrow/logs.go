// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/plog"
)

// LogsConverter converts plog.Logs to Arrow records.
type LogsConverter struct {
	schema   *arrow.Schema
	promoted []string
	alloc    memory.Allocator
}

// NewLogsConverter creates a converter with the given promoted attributes.
func NewLogsConverter(promoted []string) *LogsConverter {
	return &LogsConverter{
		schema:   LogsSchema(promoted),
		promoted: promoted,
		alloc:    memory.DefaultAllocator,
	}
}

// Schema returns the Arrow schema used by this converter.
func (c *LogsConverter) Schema() *arrow.Schema {
	return c.schema
}

// Convert transforms plog.Logs into an Arrow record.
func (c *LogsConverter) Convert(ld plog.Logs) (arrow.RecordBatch, error) {
	builder := array.NewRecordBuilder(c.alloc, c.schema)
	defer builder.Release()

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		resourceAttrs := MapToJSON(resource.Attributes())

		sls := rl.ScopeLogs()
		for j := 0; j < sls.Len(); j++ {
			sl := sls.At(j)
			scope := sl.Scope()
			scopeAttrs := MapToJSON(scope.Attributes())

			records := sl.LogRecords()
			for k := 0; k < records.Len(); k++ {
				lr := records.At(k)
				col := 0

				// Resource fields
				appendOptionalString(builder.Field(col), rl.SchemaUrl())
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(resource.DroppedAttributesCount()))
				col++

				// Scope fields
				appendOptionalString(builder.Field(col), scope.Name())
				col++
				appendOptionalString(builder.Field(col), scope.Version())
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(scope.DroppedAttributesCount()))
				col++

				// Log record fields
				builder.Field(col).(*array.TimestampBuilder).Append(arrow.Timestamp(NanoToMicro(int64(lr.Timestamp()))))
				col++
				builder.Field(col).(*array.TimestampBuilder).Append(arrow.Timestamp(NanoToMicro(int64(lr.ObservedTimestamp()))))
				col++

				tid := lr.TraceID()
				if tid.IsEmpty() {
					builder.Field(col).(*array.StringBuilder).AppendNull()
				} else {
					builder.Field(col).(*array.StringBuilder).Append(hex(tid[:]))
				}
				col++

				sid := lr.SpanID()
				if sid.IsEmpty() {
					builder.Field(col).(*array.StringBuilder).AppendNull()
				} else {
					builder.Field(col).(*array.StringBuilder).Append(hex(sid[:]))
				}
				col++

				builder.Field(col).(*array.Int32Builder).Append(int32(lr.Flags()))
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(lr.SeverityNumber()))
				col++
				appendOptionalString(builder.Field(col), lr.SeverityText())
				col++
				appendOptionalString(builder.Field(col), lr.Body().AsString())
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(lr.DroppedAttributesCount()))
				col++

				// Promoted attributes
				merged := mergeAttributes(resource.Attributes(), lr.Attributes())
				promoted := ExtractPromotedAttributes(merged, c.promoted)
				for _, attr := range c.promoted {
					val := promoted.Values[attr]
					if val == "" {
						builder.Field(col).(*array.StringBuilder).AppendNull()
					} else {
						builder.Field(col).(*array.StringBuilder).Append(val)
					}
					col++
				}

				// Resource, scope, remaining attributes
				appendOptionalString(builder.Field(col), resourceAttrs)
				col++
				appendOptionalString(builder.Field(col), scopeAttrs)
				col++
				appendOptionalString(builder.Field(col), promoted.Remainder)
			}
		}
	}

	return builder.NewRecordBatch(), nil
}
