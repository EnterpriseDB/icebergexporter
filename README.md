# icebergexporter

An OpenTelemetry Collector exporter that writes traces, logs, and metrics as
Parquet files to S3-compatible storage, optionally managed by an Iceberg REST
catalog.

**OTel Collector → Parquet on S3 → queryable via any Iceberg-compatible engine**

## Architecture

```
OTel Collector pipeline
  → consumeTraces / consumeLogs / consumeMetrics
    → Arrow converter (otel collector pipeline data → columnar arrow.Record)
      → Buffer manager (size + time hybrid flush)
        → Parquet writer (zstd compressed)
          → S3 upload (Hive-style partition paths)
            → Iceberg catalog commit (optional)
```

Signals are written to 7 tables:

| Signal   | Table(s) |
|----------|----------|
| Traces   | `otel_traces` |
| Logs     | `otel_logs` |
| Metrics  | `otel_metrics_gauge`, `otel_metrics_sum`, `otel_metrics_histogram`, `otel_metrics_exp_histogram`, `otel_metrics_summary` |
| Profiles | `in progress` |

Each table is partitioned by time on its timestamp column
(`start_time_unix_nano` for traces, `time_unix_nano` for logs and metrics) using
Hive-style paths. The partition granularity is configurable (`hour`, `day`, or
`month`; default `hour`):

- **Hour:** `{table}/data/year=2025/month=03/day=02/hour=14/{uuid}.parquet`
- **Day:** `{table}/data/year=2025/month=03/day=02/{uuid}.parquet`
- **Month:** `{table}/data/year=2025/month=03/{uuid}.parquet`

### Query flexibility

The exporter writes standard Parquet files with Hive-style partition paths, so
you can query the data at whatever level of sophistication suits your scale.
Point DuckDB or pyarrow at the files with a glob for quick exploration. Use the
Hive partition structure (`year=.../month=.../day=.../hour=...`) for predicate
pushdown when the file count grows. Or enable the Iceberg REST catalog for
full table metadata — partition pruning via column statistics, snapshot
isolation, compaction, and retention — when you're running a production
telemetry system.

### Promoted attributes

Frequently queried OTel attributes are extracted as top-level Parquet columns
(prefixed `attr_`) for predicate pushdown. Remaining attributes are serialized
as JSON in `attributes_remaining`. Defaults:

- **Traces:** `service.name`, `http.method`, `http.status_code`, `http.url`, `http.route`, `db.system`, `rpc.method`, `rpc.service`
- **Logs:** `service.name`, `log.file.path`, `exception.type`, `exception.message`
- **Metrics:** `service.name`, `host.name`

Override via `promoted.traces`, `promoted.logs`, `promoted.metrics` in config.

### Buffering

The buffer manager uses a hybrid flush strategy:

- **Size trigger:** synchronous flush when a table's buffer exceeds
  `max_size` (default 128 MiB). Errors propagate to the collector for
  retry via the OTel exporter helper.
- **Time trigger:** background flush of all non-empty buffers every
  `flush_interval` (default 60s). Errors are logged; on flush failure the
  records remain drainable for the next attempt.

Bytes-per-row is calibrated after the first Parquet write per table, then
updated via exponential moving average.

#### Storage backends

The buffer keeps records on one of two backends, selected via
`buffer.storage.type`:

- **`memory` (default):** records sit in RAM until flush. Lowest overhead;
  records in flight are lost if the process crashes.
- **`disk`:** records are serialised to Arrow IPC stream files on local disk
  and survive crashes. The active stream is rotated to a new pending file on
  every drain; pending files are deleted only when their flush succeeds. On
  startup, any orphaned active stream is recovered as a pending file.
  Requires `buffer.storage.path` for the root directory; each table gets a
  subdirectory underneath.

Disk-backed buffering is the right choice for hour-scale `flush_interval`
values or any deployment where in-flight data must survive restarts.

## Quickstart

Prerequisites: Docker and Docker Compose.

```sh
git clone https://github.com/enterprisedb/icebergexporter.git
cd icebergexporter
make up
```

This starts:

- **MinIO** — S3-compatible storage (console at `http://localhost:9001`,
  credentials `minioadmin`/`minioadmin`)
- **Lakekeeper** — Iceberg REST catalog (API at `http://localhost:8181`)
- **OTel Collector** — custom build with the iceberg exporter, configured with
  `catalog.type: rest` to commit Iceberg metadata via Lakekeeper
- **telemetrygen** — generates traces (10/s), metrics (20/s), and logs (20/s)

After ~30 seconds, Parquet files appear in MinIO under
`otel-data/iceberg/otel_traces/data/...`. Browse them at
`http://localhost:9001` → bucket `otel-data`.

To tear down (volumes are removed, clearing all stored data):

```sh
make down
```

### Querying with DuckDB

Data is hive-partitioned by `year/month/day/hour`. Querying with a
partition-scoped path avoids slow S3 listing operations if there is historic
data. This example sets today as a variable using the `current_date` for ease
of use.

```sql
-- Install and load extensions
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='localhost:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Build today's partition path prefix
SET VARIABLE today = 'year=' || year(current_date)
  || '/month=' || lpad(month(current_date)::VARCHAR, 2, '0')
  || '/day=' || lpad(day(current_date)::VARCHAR, 2, '0');

-- Query traces
SELECT name, attr_service_name, duration_nano / 1e6 AS duration_ms
FROM read_parquet(
  's3://otel-data/iceberg/otel_traces/data/'
  || getvariable('today') || '/*/*.parquet',
  hive_partitioning=true
)
LIMIT 10;

-- Query logs
SELECT severity_text, body, attr_service_name
FROM read_parquet(
  's3://otel-data/iceberg/otel_logs/data/'
  || getvariable('today') || '/*/*.parquet',
  hive_partitioning=true
)
LIMIT 10;

-- Query metrics
SELECT metric_name, value_double, value_int, attr_service_name
FROM read_parquet(
  's3://otel-data/iceberg/otel_metrics_gauge/data/'
  || getvariable('today') || '/*/*.parquet',
  hive_partitioning=true
)
LIMIT 10;
```

### Querying with pyarrow

```python
import pyarrow.parquet as pq
import s3fs

fs = s3fs.S3FileSystem(
    endpoint_url="http://localhost:9000",
    key="minioadmin", secret="minioadmin",
)

dataset = pq.ParquetDataset(
    "otel-data/iceberg/otel_traces/data/",
    filesystem=fs,
)
table = dataset.read()
print(table.schema)
print(table.to_pandas().head())
```

## Configuration reference

```yaml
exporters:
  iceberg:
    storage:
      endpoint: http://minio:9000      # Required. S3-compatible endpoint.
      bucket: otel-data                 # Required. Target bucket.
      prefix: iceberg                   # Key prefix for all objects. Default: "iceberg"
      region: us-east-1                 # AWS region. Default: "us-east-1"
      access_key: minioadmin            # S3 access key.
      secret_key: minioadmin            # S3 secret key.
      path_style: true                  # Use path-style URLs (required for MinIO). Default: true

    catalog:
      type: rest                        # "rest" or "noop". Default: "rest"
      uri: http://catalog:8181          # REST catalog URI (required when type=rest).
      namespace: otel                   # Iceberg namespace. Default: "otel"
      warehouse: otel                   # Warehouse name in the catalog (rest only).

    buffer:
      max_size: 128Mi                   # Flush when buffer exceeds this size. Accepts raw bytes,
                                        # SI (1G = 10^9), or IEC (1Gi = 2^30). Default: 128Mi
      flush_interval: 60s               # Background flush interval. Default: 60s
      storage:
        type: memory                    # "memory" (default) or "disk".
        path: /var/lib/icebergexporter/buffer  # Root dir for disk-backed buffer. Required if type=disk.

    partition:
      granularity: hour                 # Partition time resolution: "hour", "day", or "month". Default: "hour"

    promoted:                           # Override default promoted attributes.
      traces:
        - service.name
        - http.method
      logs:
        - service.name
      metrics:
        - service.name
```

### Catalog modes

| Mode   | Behaviour |
|--------|-----------|
| `noop` | Writes Parquet files to S3 only. No Iceberg metadata. Files are queryable directly via `read_parquet()` globs. |
| `rest` | Writes Parquet files to S3, then commits them to an Iceberg REST catalog (e.g., [Lakekeeper](https://lakekeeper.io), Apache Polaris). Creates namespaces and tables on first write. |

The dev stack uses Lakekeeper as the REST catalog. Any implementation that
conforms to the [Iceberg REST OpenAPI spec](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml) should work.

## Telemetry

The exporter emits its own metrics through the OTel Collector's internal
telemetry pipeline (no `/metrics` endpoint — point the collector's
`service.telemetry.metrics` at OTLP, Prometheus remote-write, or whatever you
already use for collector self-observability). Metrics include per-table
buffered row counts, flush attempts and durations, plus disk-backed buffer
counters (pending files, pending bytes, age of the oldest pending file) when
`buffer.storage.type: disk` is set.

Full reference is in [documentation.md](./documentation.md), regenerated by
`go generate ./...` from declarations in `metadata.yaml`.

## Building

### As a standalone collector

```sh
# Install the OTel Collector Builder
go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

# Build the collector binary
builder --config=builder-config.yaml

# Run it
./dist/otelcol-iceberg --config=example/otel-config.yaml
```

### Docker

```sh
docker build -t otelcol-iceberg .
```

## Development

```sh
make build          # Compile
make test           # Unit tests with race detector
make vet            # go vet
make lint           # golangci-lint (must be installed)
make fmt            # gofmt
make tidy           # go mod tidy
make up             # Start local stack (MinIO + Lakekeeper + Collector + telemetrygen)
make down           # Tear down local stack and remove volumes
```

### Dependency gotchas

**iceberg-go version pinning:** `iceberg-go` v0.4.0 is broken by a transitive
dependency on `substrait-go` v4.4.0. We're pinned to **v0.5.0-rc0**. When
v0.5.0 stable lands, upgrade. Key API differences from v0.4.0:

- `LoadTable` takes 2 args (ctx, identifier), not 3
- `NewPartitionSpec` returns a value, not a pointer
- `PartitionSpec.Fields()` returns `iter.Seq[PartitionField]` — use single
  variable range (`for f := range`) not two-variable

**iceberg-go S3 IO scheme registration:** Cloud storage IO schemes aren't
registered by default. The codebase includes a blank import to register them:

```go
import _ "github.com/apache/iceberg-go/io/gocloud"
```

Without this, any operation that resolves an `s3://` path fails with
`io scheme not registered`.

**OTel Collector v0.146.1:** `exportertest.NewNopSettings()` requires a
`component.Type` argument. The builder binary is called `builder`, not `ocb`.

**Arrow v18:** `schema.FieldsByName()` returns `[]arrow.Field`, not `[]int`.
Unsigned integer types must be mapped to signed equivalents for Iceberg
compatibility — Iceberg has no unsigned integer types. The dependency is
pinned to a pseudo-version (`v18.5.2-0.20260220...`) because no stable
release includes the fixes we need yet. Replace with a tagged release when
one lands.

## Known limitations

- **No integration tests.** The `//go:build integration` tag is set up but no
  integration tests exist yet. Unit test coverage is good but the S3/catalog
  path is only validated manually via the dev stack.
- **No schema evolution.** If the promoted attributes config changes after
  tables are created, existing tables keep the old schema.
- **Partition granularity is immutable.** Changing `partition.granularity` after
  tables have been created requires recreating the tables. The exporter does not
  alter existing partition specs.
- **Partial metrics e2e coverage.** Gauge metrics are verified end-to-end.
  Sum, histogram, exponential histogram, and summary have unit tests but no
  e2e validation.
- **No compaction.** The exporter writes many small files. Compaction is not
  the exporter's responsibility — it belongs to the platform layer that owns
  the Iceberg tables. Use an external process such as
  [pyiceberg](https://py.iceberg.apache.org/),
  Spark (`CALL rewrite_data_files`), or Trino for periodic compaction.
- **Telemetry signals not fully implemented**
  The exporter doesn't have a full set of telemetry signals.

---

Copyright 2026- EnterpriseDB

Licensed under the [Apache License, Version 2.0](LICENSE); you may not use this
project except in compliance with the License. You may obtain a copy of the
License at <http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.