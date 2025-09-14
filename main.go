package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/redis/go-redis/v9"
)

var ctx = context.Background()
var rdb = getRedisClient()
var kc = getConsumer()
var kp = getProducer()

var stops = make(map[string]struct {
	ID         string
	DataString string
	validUntil time.Time
})

func getConsumer() *kafka.Consumer {
	var bootstrapServers string
	bootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	if bootstrapServers == "" {
		fmt.Println("KAFKA_BOOTSTRAP_SERVERS environment variable is not set.")
		os.Exit(1)
	}

	kc, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          "gtfsrt-kafka-enricher",
	})

	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
		os.Exit(1)
	}

	return kc
}

func getProducer() *kafka.Producer {
	var bootstrapServers string
	bootstrapServers = os.Getenv("KAFKA_BOOTSTRAP_SERVERS")

	if bootstrapServers == "" {
		fmt.Println("KAFKA_BOOTSTRAP_SERVERS environment variable is not set.")
		os.Exit(1)
	}

	kp, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
	})

	if err != nil {
		fmt.Printf("Failed to create producer: %s", err)
		os.Exit(1)
	}

	return kp
}

func getRedisClient() *redis.Client {
	var redisAddr string
	redisAddr = os.Getenv("REDIS_ADDR")

	if redisAddr == "" {
		fmt.Println("REDIS_ADDR environment variable is not set. Using default localhost:6379")
		redisAddr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return rdb
}

func resolveStopName(stopID string) (string, error) {
	// Check if the stop name is already cached and still valid
	if stopObject, found := stops[stopID]; found {
		if stopObject.validUntil.After(time.Now()) {
			return stopObject.DataString, nil
		}
	}

	// If not found or expired, fetch from Redis
	stopData, err := rdb.Get(ctx, stopID).Result()
	if err != nil {
		// fmt.Printf("Failed to get stop name from Redis: %s", stopID)
		// os.Exit(1)
		// return "", fmt.Errorf("Failed to get stop name from Redis: %s", err)
		//TODO : Technically we should return an error here, but for now we just return a dummy stop because of invalid GTFS data
		return "{\"stop_name\": \"Undefined\", \"location\": {\"lat\": 0, \"lon\": 0}, \"parent_station\": \"Undefined\" }", nil // Fallback to stop ID if not found in Redis
	}

	// Cache the stop name with an expiration time
	stops[stopID] = struct {
		ID         string
		DataString string
		validUntil time.Time
	}{
		ID:         stopID,
		DataString: stopData,
		validUntil: time.Now().Add(1 * time.Minute), // Cache for 1 minute
	}

	return stopData, nil
}

func deserializeMessage(msg []byte) (*SourceRecord, error) {
	var record SourceRecord
	err := json.Unmarshal(msg, &record)
	if err != nil {
		fmt.Println("Failed to deserialize message:", string(msg))
		return nil, fmt.Errorf("failed to deserialize message: %s", err)
	}
	return &record, nil
}

func serializeMessage(record *DestStopTimeUpdate) ([]byte, error) {
	msg, err := json.Marshal(record)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize message: %s", err)
	}
	return msg, nil
}

func deserializeStop(msg []byte) (*Stop, error) {
	var stop Stop
	err := json.Unmarshal(msg, &stop)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize stop: %s", err)
	}
	return &stop, nil
}

func processDate(dateStr string, timeStr string) (string, error) {

	timeElements := strings.Split(timeStr, ":")

	hour, err := strconv.Atoi(timeElements[0])

	if err != nil {
		return "", fmt.Errorf("failed to parse hour: %s", err)
	}

	hourActual := hour

	if hour >= 24 {
		hourActual = hour - 24
	}

	// Parse the date
	dateTimeStr := dateStr + "T" + fmt.Sprintf("%s:%s:%s", fmt.Sprintf("%02d", hourActual), timeElements[1], timeElements[2])
	parsedTime, err := time.Parse("20060102T15:04:05", dateTimeStr)

	if err != nil {
		panic(err)
	}

	if hour >= 24 {
		parsedTime = parsedTime.Add(24 * time.Hour)
	}

	location, _ := time.LoadLocation("Europe/Zurich")
	localTime := parsedTime.In(location)

	// Format the date as needed
	return localTime.Format(time.RFC3339), nil
}

func main() {
	var srcTopic string
	srcTopic = os.Getenv("KAFKA_SOURCE_TOPIC")

	if srcTopic == "" {
		fmt.Println("KAFKA_SOURCE_TOPIC environment variable is not set.")
		os.Exit(1)
	}

	var dstTopic string
	dstTopic = os.Getenv("KAFKA_DESTINATION_TOPIC")

	if dstTopic == "" {
		fmt.Println("KAFKA_DESTINATION_TOPIC environment variable is not set.")
		os.Exit(1)
	}


	dstTopic := "logstash.index.gtfs-delays"

	defer func() {
		if err := kc.Close(); err != nil {
			fmt.Printf("error closing Kafka consumer: %v\n", err)
			os.Exit(1)
		}
	}()

	defer kp.Close()

	err := kc.SubscribeTopics([]string{srcTopic}, nil)

	// Set up a channel for handling Ctrl-C, etc
	signalChain := make(chan os.Signal, 1)
	signal.Notify(signalChain, syscall.SIGINT, syscall.SIGTERM)

	if err != nil {
		fmt.Printf("Failed to subscribe to topic %s: %s", srcTopic, err)
		os.Exit(1)
	}

	fmt.Printf("Subscribed to topic %s\n", srcTopic)

	// Process messages
	run := true
	for run {
		select {
		case sig := <-signalChain:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev, err := kc.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Errors are informational and automatically handled by the consumer
				continue
			}
			record, err := deserializeMessage(ev.Value)
			if err != nil {
				fmt.Printf("Failed to deserialize message: %s\n", err)
				continue
			}

			for i, stopTimeUpdate := range record.TripUpdate.StopTimeUpdate {
				stopString, err := resolveStopName(stopTimeUpdate.StopID)
				if err != nil {
					fmt.Printf("Failed to resolve stop name for stop ID %s: %s\n", stopTimeUpdate.StopID, err)
					continue
				}

				stop, err := deserializeStop([]byte(stopString))

				if err != nil {
					fmt.Printf("Failed to deserialize stop: %s\n", err)
					continue
				}

				startDatetime, err := processDate(record.TripUpdate.Trip.StartDate, record.TripUpdate.Trip.StartTime)

				if err != nil {
					fmt.Printf("Failed to process start date/time: %s\n", err)
					continue
				}

				destStopTimeUpdate := &DestStopTimeUpdate{
					StopTimeUpdate: record.TripUpdate.StopTimeUpdate[i],
					Stop:           *stop,
					TripId:         record.TripUpdate.Trip.TripID,
					RouteId:        record.TripUpdate.Trip.RouteID,
					StartTime:      record.TripUpdate.Trip.StartTime,
					StartDate:      record.TripUpdate.Trip.StartDate,
					StartDatetime:  startDatetime,
				}

				msg, err := serializeMessage(destStopTimeUpdate)
				if err != nil {
					fmt.Printf("Failed to serialize message: %s\n", err)
					continue
				}

				// Produce the enriched message to the output topic
				err = kp.Produce(&kafka.Message{
					TopicPartition: kafka.TopicPartition{Topic: &dstTopic, Partition: kafka.PartitionAny},
					Value:          msg,
				}, nil)

				if err != nil {
					fmt.Printf("failed publish message with error: %s", err)
					continue
				}
			}
		}
	}
}
