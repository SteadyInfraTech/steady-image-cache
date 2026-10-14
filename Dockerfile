FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /out/sic ./cmd/sic
FROM scratch
COPY --from=build /out/sic /sic
USER 65532:65532
ENTRYPOINT ["/sic"]
