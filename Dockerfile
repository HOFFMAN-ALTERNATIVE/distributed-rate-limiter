# syntax=docker/dockerfile:1
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/ratelimiterd ./cmd/ratelimiterd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ratelimiterd /ratelimiterd
EXPOSE 8080
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 CMD ["/ratelimiterd", "healthcheck"]
ENTRYPOINT ["/ratelimiterd"]
