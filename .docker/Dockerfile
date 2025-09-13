FROM golang:1.25.1 AS builder

WORKDIR /app
COPY main.go .
COPY types.go .
COPY go.mod .
COPY go.sum .

RUN go build -o gtfsrt-kafka-enricher .

FROM gcr.io/distroless/static:nonroot
WORKDIR /app
COPY --from=builder /app/gtfsrt-kafka-enricher .

ENTRYPOINT ["/app/gtfsrt-kafka-enricher"]

