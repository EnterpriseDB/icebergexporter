FROM golang:1.25-bookworm AS builder

RUN go install go.opentelemetry.io/collector/cmd/builder@v0.146.1

WORKDIR /build
COPY . .

RUN CGO_ENABLED=0 builder --config=builder-config.yaml

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /build/dist/otelcol-iceberg /otelcol-iceberg

ENTRYPOINT ["/otelcol-iceberg"]
