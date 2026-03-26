// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package arrow

// PromotedAttributes defines which OTel attributes are promoted to top-level
// Parquet/Iceberg columns for each signal type. Promoted attributes are
// extracted from the attributes map and stored as dedicated columns for
// efficient filtering and predicate pushdown. Any remaining attributes are
// serialised as JSON into the attributes_remaining column.

// DefaultTracesPromoted is the default set of promoted attributes for traces.
var DefaultTracesPromoted = []string{
	"service.name",
	"http.method",
	"http.status_code",
	"http.url",
	"http.route",
	"db.system",
	"rpc.method",
	"rpc.service",
}

// DefaultLogsPromoted is the default set of promoted attributes for logs.
var DefaultLogsPromoted = []string{
	"service.name",
	"log.file.path",
	"exception.type",
	"exception.message",
}

// DefaultMetricsPromoted is the default set of promoted attributes for metrics.
var DefaultMetricsPromoted = []string{
	"service.name",
	"host.name",
}

// MergePromoted returns the defaults if overrides is empty, otherwise the overrides.
func MergePromoted(defaults, overrides []string) []string {
	if len(overrides) > 0 {
		return overrides
	}
	return defaults
}
