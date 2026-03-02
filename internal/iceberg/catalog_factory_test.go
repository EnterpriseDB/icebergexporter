package iceberg

import (
	"context"
	"testing"
)

func TestNewCatalogNoop(t *testing.T) {
	ctx := context.Background()

	cat, err := NewCatalog(ctx, "noop", RESTCatalogConfig{})
	if err != nil {
		t.Fatalf("NewCatalog(noop): %v", err)
	}
	if _, ok := cat.(*NoopCatalog); !ok {
		t.Errorf("expected *NoopCatalog, got %T", cat)
	}
}

func TestNewCatalogEmpty(t *testing.T) {
	ctx := context.Background()

	cat, err := NewCatalog(ctx, "", RESTCatalogConfig{})
	if err != nil {
		t.Fatalf("NewCatalog(empty): %v", err)
	}
	if _, ok := cat.(*NoopCatalog); !ok {
		t.Errorf("expected *NoopCatalog for empty type, got %T", cat)
	}
}

func TestNewCatalogUnsupported(t *testing.T) {
	ctx := context.Background()

	_, err := NewCatalog(ctx, "glue", RESTCatalogConfig{})
	if err == nil {
		t.Error("expected error for unsupported catalog type")
	}
}
