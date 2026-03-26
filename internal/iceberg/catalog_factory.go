// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"fmt"
)

// CatalogType identifies the catalog backend.
type CatalogType string

const (
	CatalogTypeREST CatalogType = "rest"
	CatalogTypeNoop CatalogType = "noop"
)

// NewCatalog creates a Catalog of the specified type.
func NewCatalog(ctx context.Context, catalogType string, cfg RESTCatalogConfig) (Catalog, error) {
	switch CatalogType(catalogType) {
	case CatalogTypeREST:
		return NewRESTCatalog(ctx, cfg)
	case CatalogTypeNoop, "":
		return NewNoopCatalog(), nil
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", catalogType)
	}
}
