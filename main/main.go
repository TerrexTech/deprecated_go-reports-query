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

type DbCollections struct {
	ReportColl    *report.DB
	MetricColl    *report.DB
	InventoryColl *report.DB
}

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
		"MONGO_REP_COLLECTION",
		"MONGO_INV_COLLECTION",
		"MONGO_METRIC_COLLECTION",
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
	collectionReport := os.Getenv("MONGO_REP_COLLECTION")
	collectionInv := os.Getenv("MONGO_INV_COLLECTION")
	collectionMet := os.Getenv("MONGO_METRIC_COLLECTION")

	timeoutMilliStr := os.Getenv("MONGO_TIMEOUT")
	parsedTimeoutMilli, err := strconv.Atoi(timeoutMilliStr)
	if err != nil {
		err = errors.Wrap(err, "Error converting Timeout value to int32")
		log.Println(err)
		log.Println("MONGO_TIMEOUT value will be set to 3000 as default value")
		parsedTimeoutMilli = 3000
	}
	timeoutMilli := uint32(parsedTimeoutMilli)

	// collections := report.Collections{
	// 	Report:    collectionReport,
	// 	Metric:    collectiionMet,
	// 	Inventory: collectionInv,
	// }

	configReport := report.DBIConfig{
		Hosts:               *commonutil.ParseHosts(hosts),
		Username:            username,
		Password:            password,
		TimeoutMilliseconds: timeoutMilli,
		Database:            database,
	}

	configReport = report.DBIConfig{
		Collection: collectionReport,
	}
	// Init IO
	dbReport, err := report.GenerateDB(configReport, &report.ConfigSchema{
		Report: &report.Report{},
	})
	if err != nil {
		err = errors.Wrap(err, "Error connecting to Report-DB")
		log.Println(err)
		return
	}

	configReport = report.DBIConfig{
		Collection: collectionMet,
	}

	dbMetric, err := report.GenerateDB(configReport, &report.ConfigSchema{
		Metric: &report.Metric{},
	})

	configReport = report.DBIConfig{
		Collection: collectionInv,
	}

	dbInventory, err := report.GenerateDB(configReport, &report.ConfigSchema{
		Inventory: &report.Inventory{},
	})

	db := DbCollections{
		ReportColl:    dbReport,
		MetricColl:    dbMetric,
		InventoryColl: dbInventory,
	}

	kio, err := initKafkaIOReport()
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
func handleRequest(db DbCollections, kio *kafka.IO, msg *sarama.ConsumerMessage) {

	numValues := 10

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
	reportData, err := db.ReportColl.CreateReportData(numValues)
	if err != nil {
		err = errors.Wrap(err, "Error creating data")
		log.Println(err)
		kio.MarkOffset() <- msg
		errStr = err.Error()
	} else {
		// Marshal Ethylene
		for _, v := range reportData {
			mReport, err = v.MarshalJSON()
		}
		// mReport, err = report.MarshalJSON()
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
