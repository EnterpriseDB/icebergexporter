// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

import (
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// jsonString marshals v to a JSON string. It panics on failure, which is
// intentional: the callers only marshal maps and slices of primitives, so
// a marshal error would indicate a programming bug, not a runtime condition.
func jsonString(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("json.Marshal(%T): %v", v, err))
	}
	return string(b)
}

// PromotedResult holds the extracted promoted attribute values and the JSON
// remainder string for attributes that were not promoted.
type PromotedResult struct {
	// Values maps promoted attribute names to their string values.
	// Missing attributes have empty string values.
	Values map[string]string
	// Remainder is the JSON-encoded map of non-promoted attributes.
	// Empty string if no remaining attributes.
	Remainder string
}

// ExtractPromotedAttributes splits a pcommon.Map into promoted values and a
// JSON remainder string. Promoted attributes are removed from the remainder.
func ExtractPromotedAttributes(attrs pcommon.Map, promoted []string) PromotedResult {
	result := PromotedResult{
		Values: make(map[string]string, len(promoted)),
	}

	// Build a set of promoted keys for fast lookup
	promotedSet := make(map[string]struct{}, len(promoted))
	for _, key := range promoted {
		promotedSet[key] = struct{}{}
		result.Values[key] = "" // default empty
	}

	// Extract promoted values and collect remainder
	remainder := make(map[string]any)
	attrs.Range(func(k string, v pcommon.Value) bool {
		if _, ok := promotedSet[k]; ok {
			result.Values[k] = v.AsString()
		} else {
			remainder[k] = valueToAny(v)
		}
		return true
	})

	if len(remainder) > 0 {
		result.Remainder = jsonString(remainder)
	}

	return result
}

// MapToJSON serialises a pcommon.Map to a JSON string.
// Returns empty string for empty maps.
func MapToJSON(m pcommon.Map) string {
	if m.Len() == 0 {
		return ""
	}
	out := make(map[string]any, m.Len())
	m.Range(func(k string, v pcommon.Value) bool {
		out[k] = valueToAny(v)
		return true
	})
	return jsonString(out)
}

func valueToAny(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeSlice:
		slice := v.Slice()
		out := make([]any, slice.Len())
		for i := 0; i < slice.Len(); i++ {
			out[i] = valueToAny(slice.At(i))
		}
		return out
	case pcommon.ValueTypeMap:
		m := v.Map()
		out := make(map[string]any, m.Len())
		m.Range(func(k string, val pcommon.Value) bool {
			out[k] = valueToAny(val)
			return true
		})
		return out
	default:
		return v.AsString()
	}
}
