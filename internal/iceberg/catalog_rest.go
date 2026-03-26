// Copyright 2026- EnterpriseDB
// SPDX-License-Identifier: Apache-2.0

package iceberg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	arrowlib "github.com/apache/arrow-go/v18/arrow"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	_ "github.com/apache/iceberg-go/io/gocloud" // registers S3/GCS/Azure IO schemes
	icebergtable "github.com/apache/iceberg-go/table"

	iarrow "github.com/enterprisedb/icebergexporter/internal/arrow"
)

// RESTCatalog wraps the iceberg-go REST catalog with table caching and
// idempotent namespace/table creation.
type RESTCatalog struct {
	inner       *rest.Catalog
	warehouse   string
	granularity iarrow.Granularity

	mu     sync.RWMutex
	tables map[string]*icebergtable.Table // "namespace.table" → cached table
}

// RESTCatalogConfig holds configuration for creating a REST catalog.
type RESTCatalogConfig struct {
	URI         string
	Warehouse   string
	Granularity iarrow.Granularity

	// S3 properties forwarded to iceberg-go's IO layer for data file access.
	S3Endpoint  string
	S3Region    string
	S3AccessKey string
	S3SecretKey string
	S3PathStyle bool
}

// NewRESTCatalog creates a new REST catalog client.
func NewRESTCatalog(ctx context.Context, cfg RESTCatalogConfig) (*RESTCatalog, error) {
	var opts []rest.Option
	if cfg.Warehouse != "" {
		opts = append(opts, rest.WithWarehouseLocation(cfg.Warehouse))
	}

	// Forward S3 properties so iceberg-go's IO layer can access data files
	// (needed for AddDataFiles duplicate detection and manifest writing).
	s3Props := make(iceberggo.Properties)
	if cfg.S3Region != "" {
		s3Props[icebergio.S3Region] = cfg.S3Region
	}
	if cfg.S3AccessKey != "" {
		s3Props[icebergio.S3AccessKeyID] = cfg.S3AccessKey
	}
	if cfg.S3SecretKey != "" {
		s3Props[icebergio.S3SecretAccessKey] = cfg.S3SecretKey
	}
	if cfg.S3Endpoint != "" {
		s3Props[icebergio.S3EndpointURL] = cfg.S3Endpoint
	}
	if cfg.S3PathStyle {
		// path-style = NOT force-virtual-addressing
		s3Props[icebergio.S3ForceVirtualAddressing] = "false"
	}
	if len(s3Props) > 0 {
		opts = append(opts, rest.WithAdditionalProps(s3Props))
	}

	cat, err := rest.NewCatalog(ctx, "iceberg-exporter", cfg.URI, opts...)
	if err != nil {
		return nil, fmt.Errorf("creating REST catalog: %w", err)
	}

	g := cfg.Granularity
	if g == "" {
		g = iarrow.GranularityHour
	}

	return &RESTCatalog{
		inner:       cat,
		warehouse:   cfg.Warehouse,
		granularity: g,
		tables:      make(map[string]*icebergtable.Table),
	}, nil
}

func (c *RESTCatalog) tableKey(namespace, table string) string {
	return namespace + "." + table
}

func (c *RESTCatalog) EnsureNamespace(ctx context.Context, namespace string) error {
	err := c.inner.CreateNamespace(ctx, icebergtable.Identifier{namespace}, nil)
	if err != nil {
		if isAlreadyExists(err) {
			return nil
		}
		return fmt.Errorf("creating namespace %s: %w", namespace, err)
	}
	return nil
}

func (c *RESTCatalog) EnsureTable(ctx context.Context, namespace, tableName string, schema *arrowlib.Schema) error {
	key := c.tableKey(namespace, tableName)

	c.mu.RLock()
	_, exists := c.tables[key]
	c.mu.RUnlock()
	if exists {
		return nil
	}

	// Convert Arrow schema to Iceberg schema
	icebergSchema, err := icebergtable.ArrowSchemaToIcebergWithFreshIDs(schema, false)
	if err != nil {
		return fmt.Errorf("converting Arrow schema to Iceberg: %w", err)
	}

	// Create partition spec for time-based partitioning on time columns
	partSpec := partitionSpecForTable(icebergSchema, c.granularity)

	var opts []catalog.CreateTableOpt
	if partSpec != nil {
		opts = append(opts, catalog.WithPartitionSpec(partSpec))
	}

	tbl, err := c.inner.CreateTable(ctx,
		icebergtable.Identifier{namespace, tableName},
		icebergSchema,
		opts...,
	)
	if err != nil {
		if isAlreadyExists(err) {
			tbl, err = c.inner.LoadTable(ctx, icebergtable.Identifier{namespace, tableName})
			if err != nil {
				return fmt.Errorf("loading existing table %s.%s: %w", namespace, tableName, err)
			}
		} else {
			return fmt.Errorf("creating table %s.%s: %w", namespace, tableName, err)
		}
	}

	c.mu.Lock()
	c.tables[key] = tbl
	c.mu.Unlock()
	return nil
}

func (c *RESTCatalog) AppendDataFiles(ctx context.Context, namespace, tableName string, files []DataFile) error {
	if len(files) == 0 {
		return nil
	}

	key := c.tableKey(namespace, tableName)
	c.mu.RLock()
	tbl, ok := c.tables[key]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("table %s.%s not found in cache; call EnsureTable first", namespace, tableName)
	}

	// Get the partition spec from table metadata
	spec := tbl.Metadata().PartitionSpec()

	// Build iceberg-go DataFile objects from our DataFile structs.
	icebergFiles := make([]iceberggo.DataFile, 0, len(files))
	for _, df := range files {
		// Compute partition data for the time transform.
		partData, err := partitionDataFromValues(spec, df.PartitionValues, c.granularity)
		if err != nil {
			return fmt.Errorf("computing partition data for %s: %w", df.Path, err)
		}

		builder, err := iceberggo.NewDataFileBuilder(
			spec,
			iceberggo.EntryContentData,
			df.Path,
			iceberggo.ParquetFile,
			partData,
			nil, // fieldIDToLogicalType
			nil, // fieldIDToFixedSize
			df.RecordCount,
			df.FileSizeBytes,
		)
		if err != nil {
			return fmt.Errorf("building data file for %s: %w", df.Path, err)
		}

		icebergFiles = append(icebergFiles, builder.Build())
	}

	// Commit via transaction
	txn := tbl.NewTransaction()
	if err := txn.AddDataFiles(ctx, icebergFiles, nil); err != nil {
		return fmt.Errorf("adding data files to %s.%s: %w", namespace, tableName, err)
	}

	newTbl, err := txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("committing transaction to %s.%s: %w", namespace, tableName, err)
	}

	// Update cached table reference with new metadata
	c.mu.Lock()
	c.tables[key] = newTbl
	c.mu.Unlock()

	return nil
}

func (c *RESTCatalog) Close() error {
	return nil
}

// icebergTransform returns the iceberg-go Transform for the given granularity.
func icebergTransform(g iarrow.Granularity) iceberggo.Transform {
	switch g {
	case iarrow.GranularityMonth:
		return iceberggo.MonthTransform{}
	case iarrow.GranularityDay:
		return iceberggo.DayTransform{}
	default:
		return iceberggo.HourTransform{}
	}
}

// partitionSpecForTable creates a partition spec with the appropriate time
// transform if the schema has a timestamp-like column.
func partitionSpecForTable(schema *iceberggo.Schema, g iarrow.Granularity) *iceberggo.PartitionSpec {
	for _, field := range schema.Fields() {
		switch field.Name {
		case "start_time_unix_nano", "time_unix_nano":
			spec := iceberggo.NewPartitionSpec(
				iceberggo.PartitionField{
					SourceID:  field.ID,
					FieldID:   1000,
					Name:      field.Name + "_" + string(g),
					Transform: icebergTransform(g),
				},
			)
			return &spec
		}
	}
	return nil
}

// partitionDataFromValues computes the iceberg-go partition data map from
// Hive-style partition values. The computed value depends on the granularity:
// - hour: int32 hours since epoch
// - day: int32 days since epoch
// - month: int32 months since epoch (year*12 + month - 1)
func partitionDataFromValues(spec iceberggo.PartitionSpec, vals map[string]string, g iarrow.Granularity) (map[int]any, error) {
	if len(vals) == 0 {
		return nil, nil
	}

	year, err := strconv.Atoi(vals["year"])
	if err != nil {
		return nil, fmt.Errorf("parsing year: %w", err)
	}
	month, err := strconv.Atoi(vals["month"])
	if err != nil {
		return nil, fmt.Errorf("parsing month: %w", err)
	}

	var partValue int32

	switch g {
	case iarrow.GranularityMonth:
		// Iceberg MonthTransform: months since 1970-01-01
		partValue = int32((year-1970)*12 + (month - 1))

	case iarrow.GranularityDay:
		dayStr, ok := vals["day"]
		if !ok {
			return nil, fmt.Errorf("day partition value missing")
		}
		day, err := strconv.Atoi(dayStr)
		if err != nil {
			return nil, fmt.Errorf("parsing day: %w", err)
		}
		t := time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC)
		partValue = int32(t.Unix() / 86400)

	default: // hour
		dayStr, ok := vals["day"]
		if !ok {
			return nil, fmt.Errorf("day partition value missing")
		}
		day, err := strconv.Atoi(dayStr)
		if err != nil {
			return nil, fmt.Errorf("parsing day: %w", err)
		}
		hourStr, ok := vals["hour"]
		if !ok {
			return nil, fmt.Errorf("hour partition value missing")
		}
		hour, err := strconv.Atoi(hourStr)
		if err != nil {
			return nil, fmt.Errorf("parsing hour: %w", err)
		}
		t := time.Date(year, time.Month(month), day, hour, 0, 0, 0, time.UTC)
		partValue = int32(t.UnixMicro() / (3600 * 1_000_000))
	}

	result := make(map[int]any)
	for f := range spec.Fields() {
		switch f.Transform.(type) {
		case iceberggo.HourTransform, iceberggo.DayTransform, iceberggo.MonthTransform:
			result[f.FieldID] = partValue
		}
	}
	return result, nil
}

func isAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "AlreadyExists")
}

// Compile-time interface checks
var _ Catalog = (*RESTCatalog)(nil)
var _ Catalog = (*NoopCatalog)(nil)
