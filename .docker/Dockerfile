FROM golang:1.25.1 AS builder

WORKDIR /app
COPY main.go .
#COPY types.go .
COPY go.mod .
COPY go.sum .

RUN go build -o gtfs-kafka-enrichment .

FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=builder /app/gtfs-kafka-enrichment .

ENTRYPOINT ["/app/gtfs-kafka-enrichment"]

