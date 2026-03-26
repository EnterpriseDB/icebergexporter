// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func TestMetricsConverterEmpty(t *testing.T) {
	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(pmetric.NewMetrics())
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Gauge != nil {
		t.Error("expected nil gauge record for empty metrics")
	}
	if result.Sum != nil {
		t.Error("expected nil sum record for empty metrics")
	}
}

func TestMetricsConverterGauge(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "gauge-svc")
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("cpu.usage")
	m.SetUnit("percent")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(1000000000)
	dp.SetDoubleValue(42.5)

	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(md)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Gauge == nil {
		t.Fatal("expected non-nil gauge record")
	}
	if result.Gauge.NumRows() != 1 {
		t.Errorf("expected 1 gauge row, got %d", result.Gauge.NumRows())
	}
	if result.Sum != nil {
		t.Error("expected nil sum record")
	}
}

func TestMetricsConverterSum(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.requests")
	m.SetEmptySum()
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.SetTimestamp(1000000000)
	dp.SetIntValue(100)

	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(md)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Sum == nil {
		t.Fatal("expected non-nil sum record")
	}
	if result.Sum.NumRows() != 1 {
		t.Errorf("expected 1 sum row, got %d", result.Sum.NumRows())
	}
}

func TestMetricsConverterHistogram(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("http.duration")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.SetTimestamp(1000000000)
	dp.SetCount(10)
	dp.SetSum(150.0)
	dp.ExplicitBounds().Append(10, 50, 100)
	dp.BucketCounts().Append(2, 5, 2, 1)

	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(md)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Histogram == nil {
		t.Fatal("expected non-nil histogram record")
	}
	if result.Histogram.NumRows() != 1 {
		t.Errorf("expected 1 histogram row, got %d", result.Histogram.NumRows())
	}
}

func TestMetricsConverterSummary(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName("rpc.duration")
	m.SetEmptySummary()
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.SetTimestamp(1000000000)
	dp.SetCount(100)
	dp.SetSum(500.0)
	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.99)
	qv.SetValue(45.0)

	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(md)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Summary == nil {
		t.Fatal("expected non-nil summary record")
	}
	if result.Summary.NumRows() != 1 {
		t.Errorf("expected 1 summary row, got %d", result.Summary.NumRows())
	}
}

func TestMetricsConverterMixedTypes(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	// Gauge
	g := sm.Metrics().AppendEmpty()
	g.SetName("gauge")
	g.SetEmptyGauge()
	g.Gauge().DataPoints().AppendEmpty().SetDoubleValue(1.0)

	// Sum
	s := sm.Metrics().AppendEmpty()
	s.SetName("sum")
	s.SetEmptySum()
	s.Sum().DataPoints().AppendEmpty().SetIntValue(1)

	c := NewMetricsConverter(DefaultMetricsPromoted)
	result, err := c.Convert(md)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Release()

	if result.Gauge == nil || result.Sum == nil {
		t.Error("expected both gauge and sum records")
	}
	if result.Histogram != nil || result.ExpHistogram != nil || result.Summary != nil {
		t.Error("expected nil records for unused metric types")
	}
}
