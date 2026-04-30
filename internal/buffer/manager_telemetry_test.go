// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package buffer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	"go.uber.org/zap/zaptest"

	"github.com/enterprisedb/icebergexporter/internal/metadata"
	"github.com/enterprisedb/icebergexporter/internal/metadatatest"
)

// newTelemetryHarness returns a Manager wired with a fresh in-memory telemetry
// pipeline plus the underlying componenttest.Telemetry so tests can assert
// against emitted metrics.
func newTelemetryHarness(t *testing.T, opts ManagerOptions, flushFn FlushFunc) (*Manager, *componenttest.Telemetry, *metadata.TelemetryBuilder) {
	t.Helper()
	tt := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, tt.Shutdown(context.Background())) })

	tb, err := metadata.NewTelemetryBuilder(tt.NewTelemetrySettings())
	require.NoError(t, err)
	t.Cleanup(tb.Shutdown)

	opts.Telemetry = tb
	if opts.FlushInterval == 0 {
		opts.FlushInterval = time.Hour
	}
	mgr, err := NewManager(opts, flushFn, zaptest.NewLogger(t))
	require.NoError(t, err)
	require.NoError(t, mgr.Start())
	// Stop may surface a flush error if the test left the buffer dirty (e.g.
	// failing-flush tests). The metric assertions are the test's contract;
	// Stop errors are expected noise in those cases.
	t.Cleanup(func() { _ = mgr.Stop(context.Background()) })

	return mgr, tt, tb
}

func TestBufferTelemetryFlushSuccess(t *testing.T) {
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, totalRows int64) (int64, error) {
		return totalRows * 50, nil
	}
	mgr, tt, _ := newTelemetryHarness(t, ManagerOptions{}, flushFn)

	rec := makeTestRecord(10)
	defer rec.Release()
	require.NoError(t, mgr.Add(context.Background(), "otel_traces", rec))

	// Force a flush by stopping early via direct flushAll; the public path is
	// to trigger a flush manually. We can call Stop, which drains.
	require.NoError(t, mgr.flushAll(context.Background()))

	metadatatest.AssertEqualExporterIcebergBufferFlushes(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("outcome", "success"),
				attribute.String("table", "otel_traces"),
			),
			Value: 1,
		}},
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestBufferTelemetryFlushFailure(t *testing.T) {
	failErr := errors.New("simulated flush failure")
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, failErr
	}
	mgr, tt, _ := newTelemetryHarness(t, ManagerOptions{}, flushFn)

	rec := makeTestRecord(7)
	defer rec.Release()
	require.NoError(t, mgr.Add(context.Background(), "otel_logs", rec))

	// Drive a flush attempt — it will fail.
	_ = mgr.flushAll(context.Background())

	metadatatest.AssertEqualExporterIcebergBufferFlushes(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("outcome", "failure"),
				attribute.String("table", "otel_logs"),
			),
			Value: 1,
		}},
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestBufferTelemetryEmptyDrainEmitsNothing(t *testing.T) {
	flushCalls := 0
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		flushCalls++
		return 0, nil
	}
	mgr, tt, _ := newTelemetryHarness(t, ManagerOptions{}, flushFn)

	// Add then drain — first flush emits one success.
	rec := makeTestRecord(3)
	defer rec.Release()
	require.NoError(t, mgr.Add(context.Background(), "otel_traces", rec))
	require.NoError(t, mgr.flushAll(context.Background()))

	// Second flushAll has nothing to do — should not increment counter.
	require.NoError(t, mgr.flushAll(context.Background()))

	metadatatest.AssertEqualExporterIcebergBufferFlushes(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(
				attribute.String("outcome", "success"),
				attribute.String("table", "otel_traces"),
			),
			Value: 1, // not 2 — the empty drain didn't emit
		}},
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestBufferTelemetryRowsGauge(t *testing.T) {
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, nil
	}
	mgr, tt, _ := newTelemetryHarness(t, ManagerOptions{}, flushFn)

	rec := makeTestRecord(15)
	defer rec.Release()
	require.NoError(t, mgr.Add(context.Background(), "otel_metrics_gauge", rec))

	metadatatest.AssertEqualExporterIcebergBufferRows(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("table", "otel_metrics_gauge")),
			Value:      15,
		}},
		metricdatatest.IgnoreTimestamp(),
	)
}

func TestBufferTelemetryDiskPendingFiles(t *testing.T) {
	dir := t.TempDir()
	failErr := errors.New("disk telemetry failure")
	flushFn := func(_ context.Context, _ string, _ []arrow.RecordBatch, _ int64) (int64, error) {
		return 0, failErr
	}
	mgr, tt, _ := newTelemetryHarness(t, ManagerOptions{
		Storage: StorageOptions{Type: StorageDisk, Path: dir},
	}, flushFn)

	// Add records then fail flush — produces one pending file on disk.
	rec := makeTestRecord(4)
	defer rec.Release()
	require.NoError(t, mgr.Add(context.Background(), "otel_traces", rec))
	_ = mgr.flushAll(context.Background())

	metadatatest.AssertEqualExporterIcebergBufferPendingFiles(t, tt,
		[]metricdata.DataPoint[int64]{{
			Attributes: attribute.NewSet(attribute.String("table", "otel_traces")),
			Value:      1,
		}},
		metricdatatest.IgnoreTimestamp(),
	)
}
