# syntax=docker/dockerfile:1

# Build stage. The toolchain never reaches the final image, which is the
# difference between a ~1GB container and a ~20MB one. The image this replaces
# installed build-base and linux-headers into the runtime layer and shipped
# roughly 450MB to run a single Python script.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build

WORKDIR /src

# Dependencies are copied and downloaded first so this layer stays cached
# across source-only changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

# Version identifiers are stamped in at build time. This is genuinely useful in
# operation: it lets anyone establish exactly which commit a running container
# was built from, without guessing from image tags.
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

# TARGETOS/TARGETARCH are supplied by buildx for each platform in the matrix.
ARG TARGETOS
ARG TARGETARCH

# CGO is disabled so the result is a genuinely static binary with no libc
# dependency, which is what allows the distroless static base below.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build \
      -trimpath \
      -ldflags="-s -w \
        -X main.version=${VERSION} \
        -X main.commit=${COMMIT} \
        -X main.buildDate=${BUILD_DATE}" \
      -o /out/solarham-exporter \
      ./cmd/solarham-exporter

# Runtime stage.
#
# distroless/static carries no shell, no package manager and no libc, so the
# attack surface is the binary itself. :nonroot runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/solarham-exporter /usr/local/bin/solarham-exporter

# This exporter needs no device access and no writable filesystem: it makes
# outbound HTTPS requests and serves one HTTP port. There is no reason for it
# to run as root, and it can run with --read-only and a dropped capability set.
USER nonroot:nonroot

EXPOSE 9102

# The health endpoints are plain HTTP, but distroless has no shell or curl to
# call them with, so HEALTHCHECK is left to the orchestrator or an external
# monitor pointed at /readyz.

ENTRYPOINT ["/usr/local/bin/solarham-exporter"]

LABEL org.opencontainers.image.title="solarham" \
      org.opencontainers.image.description="Export space weather and HF propagation data from NOAA SWPC, hamqsl.com and prop.kc2g.com to Prometheus, InfluxDB, MQTT/Home Assistant and OTLP" \
      org.opencontainers.image.source="https://github.com/RealDougEubanks/solarham" \
      org.opencontainers.image.licenses="MIT"
