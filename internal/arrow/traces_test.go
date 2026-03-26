// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestTracesConverterEmpty(t *testing.T) {
	c := NewTracesConverter(DefaultTracesPromoted)
	rec, err := c.Convert(ptrace.NewTraces())
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Errorf("expected 0 rows, got %d", rec.NumRows())
	}
	if rec.NumCols() != int64(c.Schema().NumFields()) {
		t.Errorf("expected %d cols, got %d", c.Schema().NumFields(), rec.NumCols())
	}
}

func TestTracesConverterSingleSpan(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "test-svc")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName("my-scope")

	span := ss.Spans().AppendEmpty()
	span.SetName("test-span")
	span.SetTraceID(pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pcommon.SpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))
	span.SetKind(ptrace.SpanKindServer)
	span.SetStartTimestamp(1000000000)
	span.SetEndTimestamp(2000000000)
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.Attributes().PutStr("http.method", "GET")
	span.Attributes().PutInt("http.status_code", 200)

	c := NewTracesConverter(DefaultTracesPromoted)
	rec, err := c.Convert(td)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}

	// Verify span name
	nameIdx := fieldIndex(c.Schema(), "name")
	if nameIdx < 0 {
		t.Fatal("name field not found")
	}

	// Verify duration
	durIdx := fieldIndex(c.Schema(), "duration_nano")
	if durIdx < 0 {
		t.Fatal("duration_nano field not found")
	}
}

func TestTracesConverterMultipleSpans(t *testing.T) {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty()

	for i := 0; i < 5; i++ {
		span := ss.Spans().AppendEmpty()
		span.SetName("span")
		span.SetTraceID(pcommon.TraceID([16]byte{byte(i)}))
		span.SetSpanID(pcommon.SpanID([8]byte{byte(i)}))
	}

	c := NewTracesConverter(DefaultTracesPromoted)
	rec, err := c.Convert(td)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	if rec.NumRows() != 5 {
		t.Errorf("expected 5 rows, got %d", rec.NumRows())
	}
}

func fieldIndex(schema *arrowlib.Schema, name string) int {
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == name {
			return i
		}
	}
	return -1
}
