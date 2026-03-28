FROM golang:1.25-alpine AS builder

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o skylr-shard ./cmd

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /build/skylr-shard /app/skylr-shard
COPY --from=builder /build/config/config.yaml /app/config/config.yaml

EXPOSE 5000 5001

ENTRYPOINT ["/app/skylr-shard"]
