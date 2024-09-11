FROM --platform=$BUILDPLATFORM golang:1.22 AS base

WORKDIR /app

COPY . .

RUN \
    rm -rf cmd/callback \
    && go mod tidy \
    && go mod download

ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -o /app/main cmd/worker/main.go

FROM gcr.io/distroless/static-debian11 AS final

USER nonroot:nonroot

WORKDIR /app

COPY --from=base /app/main /app

ENTRYPOINT ["/app/main"]
