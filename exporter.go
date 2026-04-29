// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package icebergexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"

	iarrow "github.com/enterprisedb/icebergexporter/internal/arrow"
	"github.com/enterprisedb/icebergexporter/internal/buffer"
	"github.com/enterprisedb/icebergexporter/internal/iceberg"
	"github.com/enterprisedb/icebergexporter/internal/metadata"
	"github.com/enterprisedb/icebergexporter/internal/writer"
)

type icebergExporter struct {
	cfg       *Config
	logger    *zap.Logger
	telemetry component.TelemetrySettings

	tracesConv  *iarrow.TracesConverter
	logsConv    *iarrow.LogsConverter
	metricsConv *iarrow.MetricsConverter

	bufferMgr *buffer.Manager
	writer    *writer.Writer
	catalog   iceberg.Catalog
	tb        *metadata.TelemetryBuilder
}

func newExporter(cfg *Config, telemetry component.TelemetrySettings) *icebergExporter {
	return &icebergExporter{
		cfg:       cfg,
		logger:    telemetry.Logger,
		telemetry: telemetry,
	}
}

func (e *icebergExporter) start(ctx context.Context, host component.Host) error {
	e.logger.Info("starting iceberg exporter")

	// Resolve promoted attributes
	tracesPromoted := iarrow.MergePromoted(iarrow.DefaultTracesPromoted, e.cfg.Promoted.Traces)
	logsPromoted := iarrow.MergePromoted(iarrow.DefaultLogsPromoted, e.cfg.Promoted.Logs)
	metricsPromoted := iarrow.MergePromoted(iarrow.DefaultMetricsPromoted, e.cfg.Promoted.Metrics)

	// Create converters
	e.tracesConv = iarrow.NewTracesConverter(tracesPromoted)
	e.logsConv = iarrow.NewLogsConverter(logsPromoted)
	e.metricsConv = iarrow.NewMetricsConverter(metricsPromoted)

	// Create S3 FileIO
	fileIO, err := iceberg.NewFileIO(ctx, iceberg.S3Config{
		Endpoint:  e.cfg.Storage.Endpoint,
		Region:    e.cfg.Storage.Region,
		Bucket:    e.cfg.Storage.Bucket,
		Prefix:    e.cfg.Storage.Prefix,
		AccessKey: e.cfg.Storage.AccessKey,
		SecretKey: e.cfg.Storage.SecretKey,
		PathStyle: e.cfg.Storage.PathStyle,
	})
	if err != nil {
		return fmt.Errorf("creating file IO: %w", err)
	}

	granularity := iarrow.Granularity(e.cfg.Partition.Granularity)

	// Create catalog (forward S3 props for iceberg-go IO layer)
	cat, err := iceberg.NewCatalog(ctx, e.cfg.Catalog.Type, iceberg.RESTCatalogConfig{
		URI:         e.cfg.Catalog.URI,
		Warehouse:   e.cfg.Catalog.Warehouse,
		Granularity: granularity,
		S3Endpoint:  e.cfg.Storage.Endpoint,
		S3Region:    e.cfg.Storage.Region,
		S3AccessKey: e.cfg.Storage.AccessKey,
		S3SecretKey: e.cfg.Storage.SecretKey,
		S3PathStyle: e.cfg.Storage.PathStyle,
	})
	if err != nil {
		return fmt.Errorf("creating catalog: %w", err)
	}
	e.catalog = cat

	// Create writer
	e.writer = writer.New(writer.Config{
		FileIO:      fileIO,
		Catalog:     cat,
		Namespace:   e.cfg.Catalog.Namespace,
		Granularity: granularity,
		Logger:      e.logger,
	})

	// Register schemas
	e.writer.RegisterSchema(iarrow.TableTraces, e.tracesConv.Schema())
	e.writer.RegisterSchema(iarrow.TableLogs, e.logsConv.Schema())
	e.writer.RegisterSchema(iarrow.TableGauge, iarrow.GaugeSchema(metricsPromoted))
	e.writer.RegisterSchema(iarrow.TableSum, iarrow.SumSchema(metricsPromoted))
	e.writer.RegisterSchema(iarrow.TableHistogram, iarrow.HistogramSchema(metricsPromoted))
	e.writer.RegisterSchema(iarrow.TableExpHistogram, iarrow.ExpHistogramSchema(metricsPromoted))
	e.writer.RegisterSchema(iarrow.TableSummary, iarrow.SummarySchema(metricsPromoted))

	// Create the telemetry builder — gives us typed instruments backed by the
	// collector's MeterProvider.
	tb, err := metadata.NewTelemetryBuilder(e.telemetry)
	if err != nil {
		return fmt.Errorf("create telemetry builder: %w", err)
	}
	e.tb = tb

	// Create buffer manager
	bufMgr, err := buffer.NewManager(buffer.ManagerOptions{
		MaxSizeBytes:  int64(e.cfg.Buffer.MaxSize),
		FlushInterval: e.cfg.Buffer.FlushInterval,
		Storage: buffer.StorageOptions{
			Type: buffer.StorageType(e.cfg.Buffer.Storage.Type),
			Path: e.cfg.Buffer.Storage.Path,
		},
		Telemetry: tb,
	}, e.writer.Flush, e.logger)
	if err != nil {
		return fmt.Errorf("create buffer manager: %w", err)
	}
	e.bufferMgr = bufMgr
	if err := e.bufferMgr.Start(); err != nil {
		return fmt.Errorf("start buffer manager: %w", err)
	}

	e.logger.Info("iceberg exporter started",
		zap.String("endpoint", e.cfg.Storage.Endpoint),
		zap.String("bucket", e.cfg.Storage.Bucket),
		zap.String("catalog_type", e.cfg.Catalog.Type),
	)
	return nil
}

func (e *icebergExporter) shutdown(ctx context.Context) error {
	e.logger.Info("shutting down iceberg exporter")

	if e.bufferMgr != nil {
		if err := e.bufferMgr.Stop(ctx); err != nil {
			e.logger.Error("error draining buffers", zap.Error(err))
		}
	}
	if e.tb != nil {
		e.tb.Shutdown()
	}
	if e.catalog != nil {
		if err := e.catalog.Close(); err != nil {
			e.logger.Error("error closing catalog", zap.Error(err))
		}
	}
	return nil
}

func (e *icebergExporter) consumeTraces(ctx context.Context, td ptrace.Traces) error {
	if td.SpanCount() == 0 {
		return nil
	}

	rec, err := e.tracesConv.Convert(td)
	if err != nil {
		return fmt.Errorf("converting traces to arrow: %w", err)
	}
	defer rec.Release()

	return e.bufferMgr.Add(ctx, iarrow.TableTraces, rec)
}

func (e *icebergExporter) consumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	if md.DataPointCount() == 0 {
		return nil
	}

	result, err := e.metricsConv.Convert(md)
	if err != nil {
		return fmt.Errorf("converting metrics to arrow: %w", err)
	}
	defer result.Release()

	// Add each metric type to its own table buffer
	if result.Gauge != nil {
		if err := e.bufferMgr.Add(ctx, iarrow.TableGauge, result.Gauge); err != nil {
			return err
		}
	}
	if result.Sum != nil {
		if err := e.bufferMgr.Add(ctx, iarrow.TableSum, result.Sum); err != nil {
			return err
		}
	}
	if result.Histogram != nil {
		if err := e.bufferMgr.Add(ctx, iarrow.TableHistogram, result.Histogram); err != nil {
			return err
		}
	}
	if result.ExpHistogram != nil {
		if err := e.bufferMgr.Add(ctx, iarrow.TableExpHistogram, result.ExpHistogram); err != nil {
			return err
		}
	}
	if result.Summary != nil {
		if err := e.bufferMgr.Add(ctx, iarrow.TableSummary, result.Summary); err != nil {
			return err
		}
	}
	return nil
}

func (e *icebergExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	if ld.LogRecordCount() == 0 {
		return nil
	}

	rec, err := e.logsConv.Convert(ld)
	if err != nil {
		return fmt.Errorf("converting logs to arrow: %w", err)
	}
	defer rec.Release()

	return e.bufferMgr.Add(ctx, iarrow.TableLogs, rec)
}
