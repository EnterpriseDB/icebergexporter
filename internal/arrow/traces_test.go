// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"testing"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func assertStringCol(t *testing.T, rec arrowlib.RecordBatch, schema *arrowlib.Schema, col string, row int, want string) {
	t.Helper()
	idx := fieldIndex(schema, col)
	if idx < 0 {
		t.Fatalf("field %q not found", col)
	}
	got := rec.Column(idx).(*array.String).Value(row)
	if got != want {
		t.Errorf("%s[%d] = %q, want %q", col, row, got, want)
	}
}

func assertInt64Col(t *testing.T, rec arrowlib.RecordBatch, schema *arrowlib.Schema, col string, row int, want int64) {
	t.Helper()
	idx := fieldIndex(schema, col)
	if idx < 0 {
		t.Fatalf("field %q not found", col)
	}
	got := rec.Column(idx).(*array.Int64).Value(row)
	if got != want {
		t.Errorf("%s[%d] = %d, want %d", col, row, got, want)
	}
}

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
	assertStringCol(t, rec, c.Schema(), "name", 0, "test-span")

	// Verify duration = end - start = 2000000000 - 1000000000
	assertInt64Col(t, rec, c.Schema(), "duration_nano", 0, 1000000000)

	// Verify promoted attributes
	assertStringCol(t, rec, c.Schema(), "attr_service_name", 0, "test-svc")
	assertStringCol(t, rec, c.Schema(), "attr_http_method", 0, "GET")

	// Verify trace_id is lowercase hex
	assertStringCol(t, rec, c.Schema(), "trace_id", 0, "0102030405060708090a0b0c0d0e0f10")
	assertStringCol(t, rec, c.Schema(), "span_id", 0, "0102030405060708")
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
