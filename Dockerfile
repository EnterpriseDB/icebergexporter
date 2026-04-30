FROM golang:1.25-bookworm AS builder

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

WORKDIR /build
COPY . .

RUN CGO_ENABLED=0 builder --config=builder-config.yaml

# Pre-create the disk-buffer spill dir so a mounted volume inherits the
# nonroot ownership (distroless :nonroot runs as UID/GID 65532). Without
# this the volume comes up root-owned and disk-mode buffering fails with
# "permission denied" on the first mkdir.
RUN mkdir -p /spill && chown 65532:65532 /spill

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/dist/otelcol-iceberg /otelcol-iceberg
COPY --from=builder --chown=nonroot:nonroot /spill /var/lib/icebergexporter/buffer

ENTRYPOINT ["/otelcol-iceberg"]
