package arrow

import (
	"encoding/json"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// TracesConverter converts ptrace.Traces to Arrow records.
type TracesConverter struct {
	schema   *arrow.Schema
	promoted []string
	alloc    memory.Allocator
}

// NewTracesConverter creates a converter with the given promoted attributes.
func NewTracesConverter(promoted []string) *TracesConverter {
	return &TracesConverter{
		schema:   TracesSchema(promoted),
		promoted: promoted,
		alloc:    memory.DefaultAllocator,
	}
}

// Schema returns the Arrow schema used by this converter.
func (c *TracesConverter) Schema() *arrow.Schema {
	return c.schema
}

// Convert transforms ptrace.Traces into an Arrow record.
func (c *TracesConverter) Convert(td ptrace.Traces) (arrow.Record, error) {
	builder := array.NewRecordBuilder(c.alloc, c.schema)
	defer builder.Release()

	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		rs := rss.At(i)
		resource := rs.Resource()
		resourceAttrs := MapToJSON(resource.Attributes())

		sss := rs.ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			ss := sss.At(j)
			scope := ss.Scope()
			scopeAttrs := MapToJSON(scope.Attributes())

			spans := ss.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				col := 0

				// Resource fields
				appendOptionalString(builder.Field(col), rs.SchemaUrl())
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

				// Span fields
				tid := span.TraceID()
				builder.Field(col).(*array.FixedSizeBinaryBuilder).Append(tid[:])
				col++
				sid := span.SpanID()
				builder.Field(col).(*array.FixedSizeBinaryBuilder).Append(sid[:])
				col++
				appendOptionalString(builder.Field(col), span.TraceState().AsRaw())
				col++

				psid := span.ParentSpanID()
				if psid.IsEmpty() {
					builder.Field(col).(*array.FixedSizeBinaryBuilder).AppendNull()
				} else {
					builder.Field(col).(*array.FixedSizeBinaryBuilder).Append(psid[:])
				}
				col++

				builder.Field(col).(*array.Int32Builder).Append(int32(span.Flags()))
				col++
				builder.Field(col).(*array.StringBuilder).Append(span.Name())
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(span.Kind()))
				col++
				builder.Field(col).(*array.TimestampBuilder).Append(arrow.Timestamp(NanoToMicro(int64(span.StartTimestamp()))))
				col++
				builder.Field(col).(*array.TimestampBuilder).Append(arrow.Timestamp(NanoToMicro(int64(span.EndTimestamp()))))
				col++
				// duration
				builder.Field(col).(*array.Int64Builder).Append(int64(span.EndTimestamp()) - int64(span.StartTimestamp()))
				col++

				builder.Field(col).(*array.Int32Builder).Append(int32(span.Status().Code()))
				col++
				appendOptionalString(builder.Field(col), span.Status().Message())
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(span.DroppedAttributesCount()))
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(span.DroppedEventsCount()))
				col++
				builder.Field(col).(*array.Int32Builder).Append(int32(span.DroppedLinksCount()))
				col++

				// Events as JSON
				appendOptionalString(builder.Field(col), eventsToJSON(span.Events()))
				col++
				// Links as JSON
				appendOptionalString(builder.Field(col), linksToJSON(span.Links()))
				col++

				// Promoted attributes — merge resource + span attrs for extraction
				merged := mergeAttributes(resource.Attributes(), span.Attributes())
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

				// Resource attributes, scope attributes, remaining span attributes
				appendOptionalString(builder.Field(col), resourceAttrs)
				col++
				appendOptionalString(builder.Field(col), scopeAttrs)
				col++
				appendOptionalString(builder.Field(col), promoted.Remainder)
				col++
			}
		}
	}

	return builder.NewRecord(), nil
}

func eventsToJSON(events ptrace.SpanEventSlice) string {
	if events.Len() == 0 {
		return ""
	}
	out := make([]map[string]any, events.Len())
	for i := 0; i < events.Len(); i++ {
		e := events.At(i)
		m := map[string]any{
			"time_unix_nano":           int64(e.Timestamp()),
			"name":                     e.Name(),
			"dropped_attributes_count": e.DroppedAttributesCount(),
		}
		if e.Attributes().Len() > 0 {
			attrs := make(map[string]any)
			e.Attributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = valueToAny(v)
				return true
			})
			m["attributes"] = attrs
		}
		out[i] = m
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func linksToJSON(links ptrace.SpanLinkSlice) string {
	if links.Len() == 0 {
		return ""
	}
	out := make([]map[string]any, links.Len())
	for i := 0; i < links.Len(); i++ {
		l := links.At(i)
		tid := l.TraceID()
		sid := l.SpanID()
		m := map[string]any{
			"trace_id":                 hex(tid[:]),
			"span_id":                  hex(sid[:]),
			"trace_state":              l.TraceState().AsRaw(),
			"dropped_attributes_count": l.DroppedAttributesCount(),
		}
		if l.Attributes().Len() > 0 {
			attrs := make(map[string]any)
			l.Attributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = valueToAny(v)
				return true
			})
			m["attributes"] = attrs
		}
		out[i] = m
	}
	b, _ := json.Marshal(out)
	return string(b)
}

// mergeAttributes creates a new map with resource attrs overlaid by span/log attrs.
// Span/log attrs take precedence on key collision.
func mergeAttributes(resource, signal pcommon.Map) pcommon.Map {
	merged := pcommon.NewMap()
	resource.CopyTo(merged)
	signal.Range(func(k string, v pcommon.Value) bool {
		v.CopyTo(merged.PutEmpty(k))
		return true
	})
	return merged
}

func appendOptionalString(b array.Builder, val string) {
	sb := b.(*array.StringBuilder)
	if val == "" {
		sb.AppendNull()
	} else {
		sb.Append(val)
	}
}

func hex(b []byte) string {
	const hextable = "0123456789abcdef"
	dst := make([]byte, len(b)*2)
	for i, v := range b {
		dst[i*2] = hextable[v>>4]
		dst[i*2+1] = hextable[v&0x0f]
	}
	return string(dst)
}
