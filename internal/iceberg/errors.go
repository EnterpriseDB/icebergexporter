// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"errors"
	"net"
	"strings"
)

// IsPermanentError returns true if the error is permanent and should not be retried.
func IsPermanentError(err error) bool {
	if err == nil {
		return false
	}
	// Schema mismatches, invalid arguments, etc. are permanent
	msg := err.Error()
	for _, substr := range []string{
		"schema mismatch",
		"invalid argument",
		"table already exists",
		"namespace already exists",
		"not supported",
	} {
		if strings.Contains(strings.ToLower(msg), substr) {
			return true
		}
	}
	return false
}

// IsTransientError returns true if the error is transient and should be retried.
func IsTransientError(err error) bool {
	if err == nil {
		return false
	}
	// Network errors are transient
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := strings.ToLower(err.Error())
	for _, substr := range []string{
		"timeout",
		"connection refused",
		"connection reset",
		"temporary failure",
		"service unavailable",
		"too many requests",
		"retry",
	} {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}
