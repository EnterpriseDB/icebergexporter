// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// MetricRecords holds optional Arrow records for each metric type.
// Only non-nil records should be written.
type MetricRecords struct {
	Gauge        arrowlib.Record
	Sum          arrowlib.Record
	Histogram    arrowlib.Record
	ExpHistogram arrowlib.Record
	Summary      arrowlib.Record
}

// Release releases all non-nil records.
func (mr *MetricRecords) Release() {
	if mr.Gauge != nil {
		mr.Gauge.Release()
	}
	if mr.Sum != nil {
		mr.Sum.Release()
	}
	if mr.Histogram != nil {
		mr.Histogram.Release()
	}
	if mr.ExpHistogram != nil {
		mr.ExpHistogram.Release()
	}
	if mr.Summary != nil {
		mr.Summary.Release()
	}
}

// MetricsConverter converts pmetric.Metrics to Arrow records.
type MetricsConverter struct {
	promoted []string
	alloc    memory.Allocator

	gaugeSchema   *arrowlib.Schema
	sumSchema     *arrowlib.Schema
	histSchema    *arrowlib.Schema
	expHistSchema *arrowlib.Schema
	summarySchema *arrowlib.Schema
}

// NewMetricsConverter creates a converter with the given promoted attributes.
func NewMetricsConverter(promoted []string) *MetricsConverter {
	return &MetricsConverter{
		promoted:      promoted,
		alloc:         memory.DefaultAllocator,
		gaugeSchema:   GaugeSchema(promoted),
		sumSchema:     SumSchema(promoted),
		histSchema:    HistogramSchema(promoted),
		expHistSchema: ExpHistogramSchema(promoted),
		summarySchema: SummarySchema(promoted),
	}
}

// Convert transforms pmetric.Metrics into per-type Arrow records.
func (c *MetricsConverter) Convert(md pmetric.Metrics) (*MetricRecords, error) {
	gaugeB := array.NewRecordBuilder(c.alloc, c.gaugeSchema)
	defer gaugeB.Release()
	sumB := array.NewRecordBuilder(c.alloc, c.sumSchema)
	defer sumB.Release()
	histB := array.NewRecordBuilder(c.alloc, c.histSchema)
	defer histB.Release()
	expHistB := array.NewRecordBuilder(c.alloc, c.expHistSchema)
	defer expHistB.Release()
	summaryB := array.NewRecordBuilder(c.alloc, c.summarySchema)
	defer summaryB.Release()

	var gaugeRows, sumRows, histRows, expHistRows, summaryRows int

	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		rm := rms.At(i)
		resource := rm.Resource()
		resourceAttrs := MapToJSON(resource.Attributes())

		sms := rm.ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			sm := sms.At(j)
			scope := sm.Scope()
			scopeAttrs := MapToJSON(scope.Attributes())

			metrics := sm.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				m := metrics.At(k)

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					dps := m.Gauge().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						// Gauge schema: base + time + start + val_dbl + val_int + flags + exemplars + promoted + remainder
						col := c.appendNumberDataPointCore(gaugeB, rm, resource, resourceAttrs, scope, scopeAttrs, m, dp)
						appendOptionalString(gaugeB.Field(col), exemplarsToJSON(dp.Exemplars()))
						col++
						c.appendPromotedAndRemainder(gaugeB, col, resource.Attributes(), dp.Attributes(), resourceAttrs, scopeAttrs)
						gaugeRows++
					}
				case pmetric.MetricTypeSum:
					sum := m.Sum()
					dps := sum.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						// Sum schema: base + time + start + val_dbl + val_int + flags + is_monotonic + agg_temp + exemplars + promoted + remainder
						col := c.appendNumberDataPointCore(sumB, rm, resource, resourceAttrs, scope, scopeAttrs, m, dp)
						sumB.Field(col).(*array.BooleanBuilder).Append(sum.IsMonotonic())
						col++
						sumB.Field(col).(*array.Int32Builder).Append(int32(sum.AggregationTemporality()))
						col++
						appendOptionalString(sumB.Field(col), exemplarsToJSON(dp.Exemplars()))
						col++
						c.appendPromotedAndRemainder(sumB, col, resource.Attributes(), dp.Attributes(), resourceAttrs, scopeAttrs)
						sumRows++
					}
				case pmetric.MetricTypeHistogram:
					hist := m.Histogram()
					dps := hist.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						col := c.appendMetricBase(histB, rm, resource, resourceAttrs, scope, scopeAttrs, m)
						histB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.Timestamp()))))
						col++
						histB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.StartTimestamp()))))
						col++
						histB.Field(col).(*array.Int64Builder).Append(int64(dp.Count()))
						col++
						appendOptionalFloat64(histB.Field(col), dp.HasSum(), dp.Sum())
						col++
						appendOptionalFloat64(histB.Field(col), dp.HasMin(), dp.Min())
						col++
						appendOptionalFloat64(histB.Field(col), dp.HasMax(), dp.Max())
						col++
						appendOptionalString(histB.Field(col), uint64SliceToJSON(dp.BucketCounts().AsRaw()))
						col++
						appendOptionalString(histB.Field(col), float64SliceToJSON(dp.ExplicitBounds().AsRaw()))
						col++
						histB.Field(col).(*array.Int32Builder).Append(int32(dp.Flags()))
						col++
						histB.Field(col).(*array.Int32Builder).Append(int32(hist.AggregationTemporality()))
						col++
						appendOptionalString(histB.Field(col), exemplarsToJSON(dp.Exemplars()))
						col++
						c.appendPromotedAndRemainder(histB, col, resource.Attributes(), dp.Attributes(), resourceAttrs, scopeAttrs)
						histRows++
					}
				case pmetric.MetricTypeExponentialHistogram:
					expHist := m.ExponentialHistogram()
					dps := expHist.DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						col := c.appendMetricBase(expHistB, rm, resource, resourceAttrs, scope, scopeAttrs, m)
						expHistB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.Timestamp()))))
						col++
						expHistB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.StartTimestamp()))))
						col++
						expHistB.Field(col).(*array.Int64Builder).Append(int64(dp.Count()))
						col++
						appendOptionalFloat64(expHistB.Field(col), dp.HasSum(), dp.Sum())
						col++
						appendOptionalFloat64(expHistB.Field(col), dp.HasMin(), dp.Min())
						col++
						appendOptionalFloat64(expHistB.Field(col), dp.HasMax(), dp.Max())
						col++
						expHistB.Field(col).(*array.Int32Builder).Append(dp.Scale())
						col++
						expHistB.Field(col).(*array.Int64Builder).Append(int64(dp.ZeroCount()))
						col++
						expHistB.Field(col).(*array.Float64Builder).Append(dp.ZeroThreshold())
						col++
						expHistB.Field(col).(*array.Int32Builder).Append(dp.Positive().Offset())
						col++
						appendOptionalString(expHistB.Field(col), uint64SliceToJSON(dp.Positive().BucketCounts().AsRaw()))
						col++
						expHistB.Field(col).(*array.Int32Builder).Append(dp.Negative().Offset())
						col++
						appendOptionalString(expHistB.Field(col), uint64SliceToJSON(dp.Negative().BucketCounts().AsRaw()))
						col++
						expHistB.Field(col).(*array.Int32Builder).Append(int32(dp.Flags()))
						col++
						expHistB.Field(col).(*array.Int32Builder).Append(int32(expHist.AggregationTemporality()))
						col++
						appendOptionalString(expHistB.Field(col), exemplarsToJSON(dp.Exemplars()))
						col++
						c.appendPromotedAndRemainder(expHistB, col, resource.Attributes(), dp.Attributes(), resourceAttrs, scopeAttrs)
						expHistRows++
					}
				case pmetric.MetricTypeSummary:
					dps := m.Summary().DataPoints()
					for l := 0; l < dps.Len(); l++ {
						dp := dps.At(l)
						col := c.appendMetricBase(summaryB, rm, resource, resourceAttrs, scope, scopeAttrs, m)
						summaryB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.Timestamp()))))
						col++
						summaryB.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.StartTimestamp()))))
						col++
						summaryB.Field(col).(*array.Int64Builder).Append(int64(dp.Count()))
						col++
						summaryB.Field(col).(*array.Float64Builder).Append(dp.Sum())
						col++
						appendOptionalString(summaryB.Field(col), quantileValuesToJSON(dp.QuantileValues()))
						col++
						summaryB.Field(col).(*array.Int32Builder).Append(int32(dp.Flags()))
						col++
						c.appendPromotedAndRemainder(summaryB, col, resource.Attributes(), dp.Attributes(), resourceAttrs, scopeAttrs)
						summaryRows++
					}
				}
			}
		}
	}

	result := &MetricRecords{}
	if gaugeRows > 0 {
		result.Gauge = gaugeB.NewRecord()
	}
	if sumRows > 0 {
		result.Sum = sumB.NewRecord()
	}
	if histRows > 0 {
		result.Histogram = histB.NewRecord()
	}
	if expHistRows > 0 {
		result.ExpHistogram = expHistB.NewRecord()
	}
	if summaryRows > 0 {
		result.Summary = summaryB.NewRecord()
	}
	return result, nil
}

// appendMetricBase writes the common metric fields and returns the next column index.
func (c *MetricsConverter) appendMetricBase(
	b *array.RecordBuilder,
	rm pmetric.ResourceMetrics,
	resource pcommon.Resource,
	resourceAttrs string,
	scope pcommon.InstrumentationScope,
	scopeAttrs string,
	m pmetric.Metric,
) int {
	col := 0
	appendOptionalString(b.Field(col), rm.SchemaUrl())
	col++
	b.Field(col).(*array.Int32Builder).Append(int32(resource.DroppedAttributesCount()))
	col++
	appendOptionalString(b.Field(col), scope.Name())
	col++
	appendOptionalString(b.Field(col), scope.Version())
	col++
	b.Field(col).(*array.Int32Builder).Append(int32(scope.DroppedAttributesCount()))
	col++
	b.Field(col).(*array.StringBuilder).Append(m.Name())
	col++
	appendOptionalString(b.Field(col), m.Description())
	col++
	appendOptionalString(b.Field(col), m.Unit())
	col++
	return col
}

// appendNumberDataPointCore appends base + time + start_time + value_double + value_int + flags
// and returns the next column index. Does NOT append exemplars (position differs between gauge and sum).
func (c *MetricsConverter) appendNumberDataPointCore(
	b *array.RecordBuilder,
	rm pmetric.ResourceMetrics,
	resource pcommon.Resource,
	resourceAttrs string,
	scope pcommon.InstrumentationScope,
	scopeAttrs string,
	m pmetric.Metric,
	dp pmetric.NumberDataPoint,
) int {
	col := c.appendMetricBase(b, rm, resource, resourceAttrs, scope, scopeAttrs, m)

	b.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.Timestamp()))))
	col++
	b.Field(col).(*array.TimestampBuilder).Append(arrowlib.Timestamp(NanoToMicro(int64(dp.StartTimestamp()))))
	col++

	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		b.Field(col).(*array.Float64Builder).Append(dp.DoubleValue())
		col++
		b.Field(col).(*array.Int64Builder).AppendNull()
		col++
	case pmetric.NumberDataPointValueTypeInt:
		b.Field(col).(*array.Float64Builder).AppendNull()
		col++
		b.Field(col).(*array.Int64Builder).Append(dp.IntValue())
		col++
	default:
		b.Field(col).(*array.Float64Builder).AppendNull()
		col++
		b.Field(col).(*array.Int64Builder).AppendNull()
		col++
	}

	b.Field(col).(*array.Int32Builder).Append(int32(dp.Flags()))
	col++

	return col
}

func (c *MetricsConverter) appendPromotedAndRemainder(
	b *array.RecordBuilder,
	col int,
	resourceAttrsMap, dpAttrs pcommon.Map,
	resourceAttrsJSON, scopeAttrsJSON string,
) {
	merged := mergeAttributes(resourceAttrsMap, dpAttrs)
	promoted := ExtractPromotedAttributes(merged, c.promoted)
	for _, attr := range c.promoted {
		val := promoted.Values[attr]
		if val == "" {
			b.Field(col).(*array.StringBuilder).AppendNull()
		} else {
			b.Field(col).(*array.StringBuilder).Append(val)
		}
		col++
	}
	appendOptionalString(b.Field(col), resourceAttrsJSON)
	col++
	appendOptionalString(b.Field(col), scopeAttrsJSON)
	col++
	appendOptionalString(b.Field(col), promoted.Remainder)
}

func exemplarsToJSON(exemplars pmetric.ExemplarSlice) string {
	if exemplars.Len() == 0 {
		return ""
	}
	out := make([]map[string]any, exemplars.Len())
	for i := 0; i < exemplars.Len(); i++ {
		e := exemplars.At(i)
		tid := e.TraceID()
		sid := e.SpanID()
		m := map[string]any{
			"time_unix_nano": int64(e.Timestamp()),
			"trace_id":       hex(tid[:]),
			"span_id":        hex(sid[:]),
		}
		switch e.ValueType() {
		case pmetric.ExemplarValueTypeDouble:
			m["value_double"] = e.DoubleValue()
		case pmetric.ExemplarValueTypeInt:
			m["value_int"] = e.IntValue()
		}
		if e.FilteredAttributes().Len() > 0 {
			attrs := make(map[string]any)
			e.FilteredAttributes().Range(func(k string, v pcommon.Value) bool {
				attrs[k] = valueToAny(v)
				return true
			})
			m["filtered_attributes"] = attrs
		}
		out[i] = m
	}
	return jsonString(out)
}

func quantileValuesToJSON(qvs pmetric.SummaryDataPointValueAtQuantileSlice) string {
	if qvs.Len() == 0 {
		return ""
	}
	out := make([]map[string]any, qvs.Len())
	for i := 0; i < qvs.Len(); i++ {
		qv := qvs.At(i)
		out[i] = map[string]any{
			"quantile": qv.Quantile(),
			"value":    qv.Value(),
		}
	}
	return jsonString(out)
}

func uint64SliceToJSON(s []uint64) string {
	if len(s) == 0 {
		return ""
	}
	// Convert to int64 for JSON (avoids uint64 precision issues)
	out := make([]int64, len(s))
	for i, v := range s {
		out[i] = int64(v)
	}
	return jsonString(out)
}

func float64SliceToJSON(s []float64) string {
	if len(s) == 0 {
		return ""
	}
	return jsonString(s)
}

func appendOptionalFloat64(b array.Builder, has bool, val float64) {
	fb := b.(*array.Float64Builder)
	if has {
		fb.Append(val)
	} else {
		fb.AppendNull()
	}
}
