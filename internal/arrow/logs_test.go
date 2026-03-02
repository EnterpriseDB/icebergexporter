package arrow

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
)

func TestLogsConverterEmpty(t *testing.T) {
	c := NewLogsConverter(DefaultLogsPromoted)
	rec, err := c.Convert(plog.NewLogs())
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	if rec.NumRows() != 0 {
		t.Errorf("expected 0 rows, got %d", rec.NumRows())
	}
}

func TestLogsConverterSingleRecord(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "log-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName("my-logger")

	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(1000000000)
	lr.SetObservedTimestamp(1000000001)
	lr.SetSeverityNumber(plog.SeverityNumberError)
	lr.SetSeverityText("ERROR")
	lr.Body().SetStr("something went wrong")
	lr.Attributes().PutStr("exception.type", "RuntimeError")

	c := NewLogsConverter(DefaultLogsPromoted)
	rec, err := c.Convert(ld)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()

	if rec.NumRows() != 1 {
		t.Fatalf("expected 1 row, got %d", rec.NumRows())
	}
}
