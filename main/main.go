package main

import (
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/TerrexTech/go-authserver-query/kafka"
	"github.com/TerrexTech/go-commonutils/commonutil"
	esmodel "github.com/TerrexTech/go-eventstore-models/model"
	"github.com/TerrexTech/go-reports-query/report"
	"github.com/joho/godotenv"
	"github.com/pkg/errors"
)

func main() {
	// Load environment-file.
	// Env vars will be read directly from environment if this file fails loading
	err := godotenv.Load()
	if err != nil {
		err = errors.Wrap(err,
			".env file not found, env-vars will be read as set in environment",
		)
		log.Println(err)
	}

	missingVar, err := commonutil.ValidateEnv(
		"KAFKA_BROKERS",
		"KAFKA_CONSUMER_GROUP_REPORT",
		"KAFKA_CONSUMER_TOPIC_REPORT",
		"KAFKA_PRODUCER_TOPIC_REPORT",

		"MONGO_HOSTS",
		"MONGO_DATABASE",
		"MONGO_COLLECTION",
		"MONGO_TIMEOUT",
	)
	if err != nil {
		log.Fatalf(
			"Error: Environment variable %s is required but was not found", missingVar,
		)
	}

	hosts := os.Getenv("MONGO_HOSTS")
	username := os.Getenv("MONGO_USERNAME")
	password := os.Getenv("MONGO_PASSWORD")
	database := os.Getenv("MONGO_DATABASE")
	collection := os.Getenv("MONGO_COLLECTION")

	invCollection := os.Getenv("MONGO_INV_COLLECTION")

	timeoutMilliStr := os.Getenv("MONGO_TIMEOUT")
	parsedTimeoutMilli, err := strconv.Atoi(timeoutMilliStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting Timeout value to int32")
		log.Println(err)
		log.Println("MONGO_TIMEOUT value will be set to 3000 as default value")
		parsedTimeoutMilli = 3000
	}
	timeoutMilli := uint32(parsedTimeoutMilli)

	config := report.DBIConfig{
		Hosts:               *commonutil.ParseHosts(hosts),
		Username:            username,
		Password:            password,
		TimeoutMilliseconds: timeoutMilli,
		Database:            database,
		Collection:          collection,
	}

	// Init IO
	db, err := report.ReportDB(config)
	if err != nil {
		err = errors.Wrap(err, "Error connecting to Report-DB")
		log.Println(err)
		return
	}
	kio, err := initKafkaIOLogin()
	if err != nil {
		err = errors.Wrap(err, "Error creating KafkaIO")
		log.Println(err)
		return
	}

	// Listen on Error-Channels
	go func() {
		for err := range kio.ConsumerErrors() {
			err = errors.Wrap(err, "Consumer Error")
			log.Println(err)
		}
	}()
	go func() {
		for err := range kio.ProducerErrors() {
			parsedErr := errors.Wrap(err.Err, "Producer Error")
			log.Println(parsedErr)
			log.Println(err)
		}
	}()

	for msg := range kio.ConsumerMessages() {
		go handleRequest(db, kio, msg)
	}
}

// handleRequest handles the GraphQL request from HTTP server.
func handleRequest(db report.DBI, kio *kafka.IO, msg *sarama.ConsumerMessage) {
	// Unmarshal msg to KafkaResponse
	kr := &esmodel.KafkaResponse{}
	err := json.Unmarshal(msg.Value, kr)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling message into KafkaResponse")
		log.Println(err)
		kio.MarkOffset() <- msg
		return
	}

	// Unmarshal KafkaResponse.Input to User
	report := &report.Report{}
	err = json.Unmarshal([]byte(kr.Input), report)
	if err != nil {
		err = errors.Wrap(err, "Error unmarshalling KafkaResponse input into User")
		log.Println(err)
		kio.MarkOffset() <- msg
		return
	}

	mReport := make([]byte, 1)
	errStr := ""
	//Create report data in db
	reportData, err = db.CreateReportData()
	if err != nil {
		err = errors.Wrap(err, "Error creating data")
		log.Println(err)
		kio.MarkOffset() <- msg
		errStr = err.Error()
	} else {
		// Marshal Ethylene
		mReport, err = report.MarshalJSON()
		if err != nil {
			err = errors.Wrap(err, "Error marshalling login-user into JSON")
			log.Println(err)
			kio.MarkOffset() <- msg
			errStr = err.Error()
		}
	}

	log.Printf("%+v", kr)

	kr = &esmodel.KafkaResponse{
		AggregateID:   3,
		CorrelationID: kr.CorrelationID,
		Error:         errStr,
		Result:        mReport,
	}
	kio.ProducerInput() <- kr
	kio.MarkOffset() <- msg
}
