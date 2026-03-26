// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package icebergexporter

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestExporterStartShutdown(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Endpoint = "http://localhost:9000"
	cfg.Storage.Bucket = "test"
	cfg.Catalog.Type = "noop"

	exp := newExporter(cfg, exportertest.NewNopSettings(component.MustNewType("iceberg")).TelemetrySettings)
	if err := exp.start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start() error: %v", err)
	}
	if err := exp.shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown() error: %v", err)
	}
}

func TestConsumeTracesEmpty(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Endpoint = "http://localhost:9000"
	cfg.Storage.Bucket = "test"
	cfg.Catalog.Type = "noop"

	exp := newExporter(cfg, exportertest.NewNopSettings(component.MustNewType("iceberg")).TelemetrySettings)
	if err := exp.consumeTraces(context.Background(), ptrace.NewTraces()); err != nil {
		t.Fatalf("consumeTraces() error: %v", err)
	}
}

func TestConsumeMetricsEmpty(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Endpoint = "http://localhost:9000"
	cfg.Storage.Bucket = "test"
	cfg.Catalog.Type = "noop"

	exp := newExporter(cfg, exportertest.NewNopSettings(component.MustNewType("iceberg")).TelemetrySettings)
	if err := exp.consumeMetrics(context.Background(), pmetric.NewMetrics()); err != nil {
		t.Fatalf("consumeMetrics() error: %v", err)
	}
}

func TestConsumeLogsEmpty(t *testing.T) {
	cfg := defaultConfig()
	cfg.Storage.Endpoint = "http://localhost:9000"
	cfg.Storage.Bucket = "test"
	cfg.Catalog.Type = "noop"

	exp := newExporter(cfg, exportertest.NewNopSettings(component.MustNewType("iceberg")).TelemetrySettings)
	if err := exp.consumeLogs(context.Background(), plog.NewLogs()); err != nil {
		t.Fatalf("consumeLogs() error: %v", err)
	}
}
