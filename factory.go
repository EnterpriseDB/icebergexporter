// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package icebergexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	typeStr   = "iceberg"
	stability = component.StabilityLevelAlpha
)

// NewFactory creates a new Iceberg exporter factory.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		component.MustNewType(typeStr),
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, stability),
		exporter.WithMetrics(createMetricsExporter, stability),
		exporter.WithLogs(createLogsExporter, stability),
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
