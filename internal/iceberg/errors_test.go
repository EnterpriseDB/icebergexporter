// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"errors"
	"net"
	"testing"
)

func TestIsPermanentError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic", errors.New("something broke"), false},
		{"schema mismatch", errors.New("schema mismatch in table"), true},
		{"invalid argument", errors.New("invalid argument: bad field"), true},
		{"table already exists", errors.New("table already exists"), true},
		{"namespace already exists", errors.New("namespace already exists"), true},
		{"not supported", errors.New("operation not supported"), true},
		{"case insensitive", errors.New("Schema Mismatch detected"), true},
		{"transient error", errors.New("connection refused"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanentError(tt.err); got != tt.want {
				t.Errorf("IsPermanentError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"generic", errors.New("something broke"), false},
		{"timeout", errors.New("request timeout"), true},
		{"connection refused", errors.New("connection refused"), true},
		{"connection reset", errors.New("connection reset by peer"), true},
		{"temporary failure", errors.New("temporary failure in name resolution"), true},
		{"service unavailable", errors.New("503 service unavailable"), true},
		{"too many requests", errors.New("429 too many requests"), true},
		{"retry", errors.New("please retry later"), true},
		{"permanent error", errors.New("schema mismatch"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransientError(tt.err); got != tt.want {
				t.Errorf("IsTransientError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// testNetError implements net.Error for testing.
type testNetError struct {
	timeout   bool
	temporary bool
}

func (e *testNetError) Error() string   { return "test net error" }
func (e *testNetError) Timeout() bool   { return e.timeout }
func (e *testNetError) Temporary() bool { return e.temporary }

var _ net.Error = (*testNetError)(nil)

func TestIsTransientErrorNetError(t *testing.T) {
	err := &testNetError{timeout: true}
	if !IsTransientError(err) {
		t.Error("expected net.Error to be transient")
	}
}
