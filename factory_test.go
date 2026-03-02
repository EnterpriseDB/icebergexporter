package icebergexporter

import (
	"testing"

	"go.opentelemetry.io/collector/component/componenttest"
)

func TestNewFactory(t *testing.T) {
	f := NewFactory()
	if f == nil {
		t.Fatal("NewFactory() returned nil")
	}
	if f.Type().String() != "iceberg" {
		t.Errorf("expected type iceberg, got %s", f.Type())
	}

	cfg := f.CreateDefaultConfig()
	if cfg == nil {
		t.Fatal("CreateDefaultConfig() returned nil")
	}
	if err := componenttest.CheckConfigStruct(cfg); err != nil {
		t.Errorf("config struct check failed: %v", err)
	}
}
