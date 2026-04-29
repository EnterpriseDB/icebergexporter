// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

//go:generate mdatagen metadata.yaml
//go:generate go run golang.org/x/tools/cmd/goimports@latest -w -local github.com/enterprisedb/icebergexporter .

package icebergexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/enterprisedb/icebergexporter/internal/metadata"
)

// NewFactory creates a new Iceberg exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return defaultConfig()
}

func createTracesExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Traces, error) {
	oCfg := cfg.(*Config)
	exp := newExporter(oCfg, set.TelemetrySettings)
	return exporterhelper.NewTraces(
		ctx, set, cfg,
		exp.consumeTraces,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

func createMetricsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Metrics, error) {
	oCfg := cfg.(*Config)
	exp := newExporter(oCfg, set.TelemetrySettings)
	return exporterhelper.NewMetrics(
		ctx, set, cfg,
		exp.consumeMetrics,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	oCfg := cfg.(*Config)
	exp := newExporter(oCfg, set.TelemetrySettings)
	return exporterhelper.NewLogs(
		ctx, set, cfg,
		exp.consumeLogs,
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
	)
}
