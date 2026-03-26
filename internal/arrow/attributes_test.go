// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"encoding/json"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestExtractPromotedAttributes(t *testing.T) {
	attrs := pcommon.NewMap()
	attrs.PutStr("service.name", "my-svc")
	attrs.PutStr("http.method", "GET")
	attrs.PutInt("http.status_code", 200)
	attrs.PutStr("extra.key", "extra-val")
	attrs.PutBool("flag", true)

	promoted := []string{"service.name", "http.method", "http.status_code", "missing.key"}
	result := ExtractPromotedAttributes(attrs, promoted)

	if result.Values["service.name"] != "my-svc" {
		t.Errorf("service.name = %q, want %q", result.Values["service.name"], "my-svc")
	}
	if result.Values["http.method"] != "GET" {
		t.Errorf("http.method = %q, want %q", result.Values["http.method"], "GET")
	}
	if result.Values["http.status_code"] != "200" {
		t.Errorf("http.status_code = %q, want %q", result.Values["http.status_code"], "200")
	}
	if result.Values["missing.key"] != "" {
		t.Errorf("missing.key = %q, want empty", result.Values["missing.key"])
	}

	// Remainder should contain extra.key and flag, but not promoted keys
	var remainder map[string]any
	if err := json.Unmarshal([]byte(result.Remainder), &remainder); err != nil {
		t.Fatalf("failed to parse remainder JSON: %v", err)
	}
	if _, ok := remainder["extra.key"]; !ok {
		t.Error("remainder should contain extra.key")
	}
	if _, ok := remainder["flag"]; !ok {
		t.Error("remainder should contain flag")
	}
	if _, ok := remainder["service.name"]; ok {
		t.Error("remainder should not contain promoted key service.name")
	}
}

func TestExtractPromotedAttributesEmptyMap(t *testing.T) {
	attrs := pcommon.NewMap()
	promoted := []string{"service.name"}
	result := ExtractPromotedAttributes(attrs, promoted)

	if result.Values["service.name"] != "" {
		t.Errorf("expected empty value for missing promoted attr")
	}
	if result.Remainder != "" {
		t.Errorf("expected empty remainder, got %q", result.Remainder)
	}
}

func TestMapToJSON(t *testing.T) {
	m := pcommon.NewMap()
	if got := MapToJSON(m); got != "" {
		t.Errorf("MapToJSON(empty) = %q, want empty", got)
	}

	m.PutStr("key", "value")
	got := MapToJSON(m)
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("failed to parse MapToJSON output: %v", err)
	}
	if parsed["key"] != "value" {
		t.Errorf("expected key=value, got %v", parsed["key"])
	}
}
