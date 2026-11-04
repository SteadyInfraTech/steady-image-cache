# The build stage always runs on the builder's own architecture and compiles
# for the target one, so a multi-architecture image needs no emulation.
FROM --platform=$BUILDPLATFORM golang:1.27 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS="${TARGETOS:-linux}" GOARCH="${TARGETARCH:-$(go env GOARCH)}" \
    go build -trimpath \
      -ldflags="-s -w -X github.com/SteadyInfraTech/steady-image-cache/internal/version.Version=${VERSION}" \
      -o /out/sic ./cmd/sic

FROM scratch
# Upstream fallback verifies registries against the system roots, and a scratch
# image has none. A private registry's CA is added by mounting a bundle and
# setting SSL_CERT_FILE (see "Upstream fallback" in the README).
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/sic /sic
USER 65532:65532
ENTRYPOINT ["/sic"]
