// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
)

// NoopCatalog is a no-op catalog for "just write Parquet files" mode.
// It satisfies the Catalog interface but performs no catalog operations.
type NoopCatalog struct{}

func NewNoopCatalog() *NoopCatalog {
	return &NoopCatalog{}
}

func (c *NoopCatalog) EnsureNamespace(_ context.Context, _ string) error {
	return nil
}

func (c *NoopCatalog) EnsureTable(_ context.Context, _, _ string, _ *arrowlib.Schema) error {
	return nil
}

func (c *NoopCatalog) AppendDataFiles(_ context.Context, _, _ string, _ []DataFile) error {
	return nil
}

func (c *NoopCatalog) Close() error {
	return nil
}
