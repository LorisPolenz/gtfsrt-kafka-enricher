FROM golang:1.25.1 AS builder

WORKDIR /app
COPY main.go .
#COPY types.go .
COPY go.mod .
COPY go.sum .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gtfs-kafka-enrichment .

FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=builder /app/gtfs-kafka-enrichment .

ENTRYPOINT ["/app/gtfs-kafka-enrichment"]

