// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"fmt"
)

// NewFileIO creates a FileIO from the given S3Config.
func NewFileIO(ctx context.Context, cfg S3Config) (FileIO, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("storage endpoint is required")
	}
	return NewS3FileIO(ctx, cfg)
}
