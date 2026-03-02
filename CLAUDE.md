# Iceberg exporter

An OpenTelemetry Collector exporter that writes traces, logs, and metrics as
Parquet files to S3-compatible storage, optionally managed by an Iceberg REST
catalogue.

## Docs

The main docs are in README.md

## Rules

* All tests must pass. This includes fixing test that may have already been broken
* Don't pip install with --break-system-packages