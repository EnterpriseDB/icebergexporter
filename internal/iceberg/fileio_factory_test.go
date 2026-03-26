// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"testing"
)

func TestNewFileIORequiresEndpoint(t *testing.T) {
	_, err := NewFileIO(context.Background(), S3Config{})
	if err == nil {
		t.Error("expected error when endpoint is empty")
	}
}
